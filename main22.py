import socket
import struct
import threading
import time
import csv
from datetime import datetime
import traceback
import sys
import datetime
import logging
import smbus2
import RPi.GPIO as GPIO
import subprocess
import os
from enum import Enum, Flag, auto
from typing import Dict, Any, Optional, Tuple, Union, List
from collections import deque


# ===== Configuration Constants =====
class Config:
    """System configuration constants"""
    # I2C Addresses and Commands
    I2C_ADDR_MOTOR = 0x1b
    CMD_UP = 0x0E
    CMD_DOWN = 0x0C
    CMD_PASSIV = 0x0D  # Motor passive mode command
    CMD_STATUS = 0x40  # Motor status request command
    HYT_ADDR = 0x28

    # Motor Status Masks
    MASK_BUSY = 0b10000000  # Motor is busy
    MASK_HOME = 0b01000000  # Motor is in home position
    MASK_ERR  = 0b00100000  # Motor error
    MASK_ACT  = 0b00010000  # Motor is active

    # Reference Voltages
    VREF_ADC = 2.048    # MCP3553 uses 2.5V reference
    VREF_DAC = 4.096
    DAC_GAIN = 1.2

    # GPIO Pin Assignments
    PIN_CS_ADC = 10     # CS (Chip Select)
    PIN_CLK_ADC = 11    # SCLK (Clock)
    PIN_SDO_ADC = 9     # SDO/RDY (Data Output and Ready)
    PIN_DAT_DAC = 23
    PIN_CLK_DAC = 24
    PIN_CS_DAC = 25
    MAGNET1_GPIO = 13
    MAGNET2_GPIO = 27
    START_LED_GPIO = 26
    START_TASTE_GPIO = 22
    HVAC_GPIO = 12

    # Network Configuration
    PRIMARY_PORT = 9760
    COMPAT_PORT = 5760
    BUFFER_SIZE = 1024

    # Timing Constants
    BUTTON_POLL_INTERVAL = 0.1
    BUTTON_DEBOUNCE_TIME = 0.05
    LED_BLINK_SLOW = 0.5
    LED_BLINK_FAST = 0.1
    MEASUREMENT_TOTAL_TIME = 60  # Total measurement time in seconds
    PREPARATION_TIME = 7         # Initial preparation time in seconds
    MOTOR_MOVEMENT_TIME = 3      # Time to wait for motor movement
    STATUS_UPDATE_INTERVAL = 30  # Status monitor update interval
    MOTOR_STATUS_POLL_INTERVAL = 1.0  # How often to poll motor status
    
    # ADC Timing Constants for MCP3553SN
    ADC_SAMPLE_RATE = 60.0    # MCP3553 delivers ~60 samples/second
    ADC_SAMPLE_INTERVAL = 1.0/ADC_SAMPLE_RATE  # ~16.67ms between samples
    
    # Simulation settings
    SIMULATED_SAMPLE_RATE = 100.0  # Simulate 100 samples/s for Delphi
    SIMULATED_SAMPLE_INTERVAL = 1.0/SIMULATED_SAMPLE_RATE  # 10ms between simulated samples
    
    HYT_SAMPLE_INTERVAL = 1.0    # How often to measure temperature and humidity
    TEMP_HUMID_SAMPLE_INTERVAL = 1.0  # Interval for temperature and humidity measurement

    # File Paths
    LOG_FILE = "qumat628.log"

    # Device Information
    SERIAL_NUMBER = 6280004
    FIRMWARE_VERSION_MAJOR = 6
    FIRMWARE_VERSION_MINOR = 2

    # ADC/DAC Configuration
    ADC_BUFFER_SIZE = 10000      # Size of ring buffer for ADC samples (increased to handle long measurements)
    ADC_CLK_DELAY = 0.000002     # Clock delay for bit-banged SPI (2μs for ~250kHz)
    DAC_CLK_DELAY = 0.00001      # Clock delay for bit-banged SPI
    
    ADC_SAMPLE_RATE = 60.0    # MCP3553 delivers ~60 samples/second (from adc_gui.py)
    ADC_SAMPLE_INTERVAL = 1.0/ADC_SAMPLE_RATE  # ~16.67ms between samples
    ADC_CLK_DELAY = 0.000005  # 5μs (matches adc_gui.py)
    DAC_CLK_DELAY = 0.00001   # 10μs (matches dac_gui.py)
    FIXED_INITIALIZATION_TIME = 5 # Fixed initialization time for ADC/DAC





# ===== Enumerations for State Management =====
class MotorPosition(Enum):
    """Motor position states"""
    UP = "hoch"
    DOWN = "runter"
    UNKNOWN = "unbekannt"

    def __str__(self) -> str:
        return self.value


class ValveStatus(Enum):
    """Valve status states"""
    OPEN = "auf"
    CLOSED = "zu"
    UNKNOWN = "unbekannt"

    def __str__(self) -> str:
        return self.value


class CommandType(Enum):
    """TCP command types"""
    START = 'S'        # Enable measurement (requires button press)
    TRIGGER = 'T'      # Simulate button press
    GET_STATUS = 'G'   # Poll status
    MOTOR_UP = 'U'     # Move motor up
    MOTOR_DOWN = 'D'   # Move motor down
    RESET = 'R'        # Reset system state
    UNKNOWN = '?'      # Unknown command


class MotorStatus(Flag):
    """Motor status flags"""
    NONE = 0
    ACTIVE = auto()    # Motor is active
    ERROR = auto()     # Motor has error
    HOME = auto()      # Motor is in home position
    BUSY = auto()      # Motor is busy

    @classmethod
    def from_byte(cls, status_byte: int) -> 'MotorStatus':
        """Create MotorStatus from status byte"""
        result = cls.NONE
        if status_byte & Config.MASK_ACT:
            result |= cls.ACTIVE
        if status_byte & Config.MASK_ERR:
            result |= cls.ERROR
        if status_byte & Config.MASK_HOME:
            result |= cls.HOME
        if status_byte & Config.MASK_BUSY:
            result |= cls.BUSY
        return result

    def to_dict(self) -> Dict[str, bool]:
        """Convert to dictionary for easier access"""
        return {
            "active": bool(self & self.ACTIVE),
            "error": bool(self & self.ERROR),
            "home": bool(self & self.HOME),
            "busy": bool(self & self.BUSY)
        }

    def __str__(self) -> str:
        """Human-readable status representation"""
        parts = []
        if self & self.BUSY:
            parts.append("🟡 Busy")
        else:
            parts.append("✅ Ready")
        if self & self.HOME:
            parts.append("🏠 Home")
        if self & self.ERROR:
            parts.append("❌ Error")
        if self & self.ACTIVE:
            parts.append("⚙️ Active")
        return " | ".join(parts) if parts else "Unknown"


class Q628State(Enum):
    """Q628 state machine states"""
    POWER_UP = auto()      # Initial power-up state
    STARTUP = auto()       # System startup
    PRE_IDLE = auto()      # Preparing for idle
    IDLE = auto()          # System idle, waiting for commands
    WAIT_START_SW = auto() # Waiting for start switch
    WAIT_HV = auto()       # Waiting for HV stabilization
    START = auto()         # Starting measurement
    PRE_ACTIV = auto()     # Preparing for active measurement
    ACTIV = auto()         # Active measurement
    FINISH = auto()        # Finishing measurement


# ===== Global State and Synchronization =====
class SystemState:
    """Thread-safe system state container"""
    def __init__(self):
        self._state = {
            "motor_position": MotorPosition.UNKNOWN,
            "motor_status": MotorStatus.NONE,
            "motor_status_raw": 0,
            "valve_status": ValveStatus.UNKNOWN,
            "hv_status": False,
            "cycle_count": 0,
            "last_error": None,
            "last_ip": "0.0.0.0",
            "connected_clients": 0,
            "measurement_active": False,
            "sampling_active": False,  # New flag for sampling status
            "start_time": time.time(),
            "measurement_start_time": None,
            "button_press_time": None,
            "defined_measurement_time": 60,  # Default measurement time in seconds
            "total_expected_samples": 6000,  # Default for 60s at 100Hz
            "q628_state": Q628State.POWER_UP,
            "q628_timer": time.time(),
            "profile_hin": 4,         # Default profile for movement
            "profile_zurueck": 4,     # Default profile for return
            "wait_acdc": 11,          # Default wait time for AC/DC
            "temperature": 23.5,      # Default temperature in °C
            "humidity": 45.0,         # Default humidity in %
            "field_strength": 0.0,    # Field strength
            "hv_ac_value": 230,       # Default HV AC value
            "hv_dc_value": 6500,      # Default HV DC value
            "adc_data": [],           # ADC measurement data
            "last_adc_value": 0,      # Last ADC value read
            "runtime": 0              # Measurement runtime
        }
        self._lock = threading.RLock()
        self._runtime_timer = None  # Timer for updating runtime

    def get_measurement_runtime(self) -> int:
        """
        Calculate elapsed time since measurement start (button press)
        """
        with self._lock:
            if not self._state["measurement_active"] or self._state["button_press_time"] is None:
                return 0
            return int(time.time() - self._state["button_press_time"])

    def is_measurement_time_elapsed(self) -> bool:
        """
        Check if the defined measurement time has elapsed or exact sample count reached.
        Compares against the dynamically set measurement time from Delphi.
        """
        with self._lock:
            if not self._state["measurement_active"] or self._state["button_press_time"] is None:
                return False
                
            # Check if time has elapsed
            elapsed = time.time() - self._state["button_press_time"]
            defined_time = self._state["defined_measurement_time"]
            time_elapsed = elapsed >= defined_time
            
            # Check if exact sample count reached
            expected_samples = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
            current_samples = len(adc_buffer.get_all())
            samples_complete = current_samples >= expected_samples
            
            # Log if either condition is met
            if time_elapsed and not samples_complete:
                logger.info(f"⏱️ Measurement time elapsed ({elapsed:.2f}s/{defined_time}s) but only have {current_samples}/{expected_samples} samples")
            elif samples_complete and not time_elapsed:
                logger.info(f"⏱️ Exact sample count reached ({current_samples}/{expected_samples}) at {elapsed:.2f}s/{defined_time}s")
            
            # Return true if either condition is met
            return time_elapsed or samples_complete

    def update(self, **kwargs) -> None:
        """Thread-safe update of multiple state attributes with runtime limit enforcement"""
        with self._lock:
            self._state.update(kwargs)
            
            # CRITICAL: Add runtime cap enforcer
            # If we have an active measurement, ensure runtime never exceeds defined time
            if (self._state["measurement_active"] and 
                self._state["button_press_time"] is not None and 
                "runtime" in self._state):
                
                defined_time = self._state["defined_measurement_time"]
                if self._state["runtime"] > defined_time:
                    self._state["runtime"] = defined_time
                    logger.debug(f"⚠️ Runtime capped at {defined_time}s by setter")

    def get(self, key: str) -> Any:
        """Thread-safe state access"""
        with self._lock:
            return self._state.get(key)

    def set(self, key: str, value: Any) -> None:
        """Thread-safe state update"""
        with self._lock:
            self._state[key] = value

    def is_measurement_active(self) -> bool:
        """Check if measurement is active"""
        with self._lock:
            return self._state["measurement_active"]

    def is_sampling_active(self) -> bool:
        """Check if sampling is active"""
        with self._lock:
            return self._state["sampling_active"]

    def set_button_press_time(self) -> None:
        """
        Set button press time to current time
        """
        with self._lock:
            self._state["button_press_time"] = time.time()
            logger.info(f"🕒 Button press time set to: {self._state['button_press_time']}")

    def start_sampling(self) -> None:
        """Enable sampling flag"""
        with self._lock:
            self._state["sampling_active"] = True
            logger.info("📊 Sampling activated")

    def stop_sampling(self) -> None:
        """Disable sampling flag"""
        with self._lock:
            self._state["sampling_active"] = False
            logger.info("📊 Sampling deactivated")

    def enforce_sample_count_limit(self) -> bool:
        """
        Check if we've reached the exact sample count limit for this measurement.
        Returns True if measurement should be stopped.
        """
        with self._lock:
            if not self._state["measurement_active"]:
                return False
                
            # Get expected sample count for this measurement duration
            expected_count = self._state["total_expected_samples"]
            
            # Get current sample count from buffer
            current_count = len(adc_buffer.get_all())
            
            # If we've reached or exceeded the expected count, stop the measurement
            if current_count >= expected_count:
                logger.info(f"⚠️ Reached exact sample count limit: {current_count}/{expected_count}")
                return True
                
            return False

    def start_measurement(self) -> bool:
        """Start measurement with explicit sample rate and count control"""
        with self._lock:
            if self._state["measurement_active"]:
                return False
            
            self._state["measurement_active"] = True
            self._state["measurement_start_time"] = time.time()
            self._state["runtime"] = 0
            defined_time = self._state["defined_measurement_time"]
            
            # CRITICAL: Calculate the exact number of samples that should be collected at 100Hz
            self._state["total_expected_samples"] = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
            
            # Clear buffer at start of measurement
            adc_buffer.clear()
            
            # Start runtime update timer
            self._start_runtime_timer()
            
            logger.info(f"🕒 Measurement started at: {self._state['measurement_start_time']} " +
                      f"with target of exactly {self._state['total_expected_samples']} samples " +
                      f"over {defined_time}s")
            return True

    def _start_runtime_timer(self) -> None:
        """Start timer to update runtime field with respect to configured time limit"""
        if self._runtime_timer is not None:
            return



    def update_runtime():
        while self._state["measurement_active"]:
            with self._lock:
                if self._state["button_press_time"] is not None:
                    elapsed = time.time() - self._state["button_press_time"]
                    # Use the configured measurement time from Delphi
                    defined_time = self._state["defined_measurement_time"]
                    
                    # Cap runtime at defined time
                    self._state["runtime"] = min(int(elapsed), defined_time)
                    
                    # CRITICAL: Force state transition exactly at defined time
                    if elapsed >= defined_time and self._state["q628_state"] == Q628State.ACTIV:
                        logger.info(f"⏱️ Runtime timer detected exact end of measurement ({defined_time}s)")
                        self._state["q628_state"] = Q628State.FINISH
                        self._state["q628_timer"] = time.time()
                        
                        # Stop sampling
                        sampling_active.clear()
                        self._state["sampling_active"] = False
                    
                    # CRITICAL: Also force state transition if we've collected the exact number of samples
                    expected_samples = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
                    current_samples = len(adc_buffer.get_all())
                    
                    if current_samples >= expected_samples and self._state["q628_state"] == Q628State.ACTIV:
                        logger.info(f"⏱️ Runtime timer detected exact sample count reached ({current_samples}/{expected_samples})")
                        self._state["q628_state"] = Q628State.FINISH
                        self._state["q628_timer"] = time.time()
                        
                        # Stop sampling
                        sampling_active.clear()
                        self._state["sampling_active"] = False
            
            # Check more frequently for precise timing
            time.sleep(0.01)  # Check 100 times per second

        self._runtime_timer = threading.Thread(target=update_runtime, daemon=True)
        self._runtime_timer.start()

    def end_measurement(self) -> None:
        """End measurement"""
        with self._lock:
            self._state["measurement_active"] = False
            self._state["measurement_start_time"] = None
            self._state["button_press_time"] = None
            self._state["runtime"] = 0
            logger.info("🕒 Measurement ended")

    def increment_cycle_count(self) -> int:
        """Increment cycle counter and return new value"""
        with self._lock:
            self._state["cycle_count"] += 1
            return self._state["cycle_count"]

    def increment_clients(self) -> int:
        """Increment connected clients counter"""
        with self._lock:
            self._state["connected_clients"] += 1
            return self._state["connected_clients"]

    def decrement_clients(self) -> int:
        """Decrement connected clients counter"""
        with self._lock:
            self._state["connected_clients"] = max(0, self._state["connected_clients"] - 1)
            return self._state["connected_clients"]

    def get_runtime(self) -> int:
        """Get runtime in seconds since start"""
        return int(time.time() - self._state["start_time"])

    def get_status_dict(self) -> Dict[str, Any]:
        """Get a copy of the entire state dictionary"""
        with self._lock:
            return self._state.copy()

    def update_motor_status(self, status: MotorStatus, raw_status: int) -> None:
        """Update motor status information"""
        with self._lock:
            self._state["motor_status"] = status
            self._state["motor_status_raw"] = raw_status

            # Update motor position based on status if possible
            if status & MotorStatus.HOME:
                self._state["motor_position"] = MotorPosition.UP

    def set_q628_state(self, new_state: Q628State) -> None:
        """Update Q628 state machine state"""
        with self._lock:
            old_state = self._state["q628_state"]
            self._state["q628_state"] = new_state
            self._state["q628_timer"] = time.time()
            if old_state != new_state:
                logger.info(f"🔄 Q628 State changed: {old_state.name} -> {new_state.name}")

    def get_q628_state(self) -> Q628State:
        """Get current Q628 state"""
        with self._lock:
            return self._state["q628_state"]

    def get_q628_elapsed(self) -> float:
        """Get elapsed time in current Q628 state"""
        with self._lock:
            return time.time() - self._state["q628_timer"]

    def add_adc_data(self, value: int) -> None:
        """Add ADC data to the measurement array"""
        with self._lock:
            # Update last ADC value
            self._state["last_adc_value"] = value
            
            if len(self._state["adc_data"]) < 50:  # Keep max 50 values for TCP response
                self._state["adc_data"].append(value)
            else:
                self._state["adc_data"] = self._state["adc_data"][1:] + [value]

    def clear_adc_data(self) -> None:
        """Clear ADC measurement data"""
        with self._lock:
            self._state["adc_data"] = []

    def get_profile_delay(self, profile_id: int) -> float:
        """  VERALTET: Diese Methode wird im neuen Ablauf nicht mehr verwendet,
        stattdessen wird FIXED_INITIALIZATION_TIME verwendet.
        Zur Rückwärtskompatibilität wird ein Warnhinweis ausgegeben. """

        logger.warning("⚠️ get_profile_delay() called but profiles are disabled in new process flow")
        return Config.FIXED_INITIALIZATION_TIME  # Immer feste Zeit zurückgeben

# ===== Ring Buffer for ADC Data =====
class RingBuffer:
    """Circular buffer for ADC samples with dynamic growth option"""

    def __init__(self, size: int):
        self.buffer = deque(maxlen=size)
        self.lock = threading.Lock()
        self.initial_size = size
        self.growth_factor = 1.5  # How much to grow when needed

    def get_all(self) -> List[int]:
        """Get all values as list without removing them"""
        with self.lock:
            return list(self.buffer)

    def enqueue(self, value: int) -> None:
        """Add value to buffer, growing if needed"""
        with self.lock:
            # Check if buffer is getting full
            if len(self.buffer) >= self.buffer.maxlen * 0.9:
                # Grow the buffer
                new_size = int(self.buffer.maxlen * self.growth_factor)
                new_buffer = deque(self.buffer, maxlen=new_size)
                self.buffer = new_buffer
                logger.warning(f"RingBuffer expanded from {self.buffer.maxlen} to {new_size}")
            
            self.buffer.append(value)

    def dequeue(self) -> Optional[int]:
        """Remove and return oldest value"""
        with self.lock:
            return None if self.is_empty() else self.buffer.popleft()

    def is_empty(self) -> bool:
        """Check if buffer is empty"""
        with self.lock:
            return len(self.buffer) == 0

    def is_full(self) -> bool:
        """Check if buffer is full"""
        with self.lock:
            return len(self.buffer) == self.buffer.maxlen

    def clear(self) -> None:
        """Clear all values and reset size"""
        with self.lock:
            self.buffer.clear()
            # Reset to initial size when cleared
            if self.buffer.maxlen != self.initial_size:
                self.buffer = deque(maxlen=self.initial_size)

    def __len__(self) -> int:
        """Get current buffer size"""
        with self.lock:
            return len(self.buffer)


# ===== Global Variables =====
system_state = SystemState()
measurement_start_time = 0
i2c_bus = None  # Will be initialized in hardware setup
adc_buffer = RingBuffer(Config.ADC_BUFFER_SIZE)  # Larger buffer for long measurements
temp_buffer_lock = threading.Lock()  # Lock for temp_buffer access
temp_buffer = {"latest_value": 0, "timestamp": 0.0}  # Temporary buffer for latest ADC value
# Add this to your global variables or system_state initialization
last_sent_index = 0  # Global tracker for last sent buffer position

# Synchronization primitives
i2c_lock = threading.Lock()
adc_lock = threading.Lock()
dac_lock = threading.Lock()
shutdown_requested = threading.Event()
enable_from_pc = threading.Event()
simulate_start = threading.Event()
sampling_active = threading.Event()
continuous_sampling_active = threading.Event()  # Für kontinuierliches Abtasten

# Setup logging
logger = logging.getLogger("qumat628")


# ===== Logging Setup =====
def setup_logging() -> None:
    """Configure logging to both file and console with proper formatting"""
    logger.setLevel(logging.INFO)

    # Create formatters
    console_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_format = logging.Formatter(
        '%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler
    try:
        file_handler = logging.FileHandler(Config.LOG_FILE, mode='a')
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

        # Log startup message
        logger.info("\n\n--- QUMAT628 System Starting: %s ---",
                    datetime.datetime.now().isoformat())
    except Exception as e:
        logger.warning("Could not set up file logging: %s", e)

def initialize_hardware() -> bool:
    """Initialize hardware with comprehensive error checking and 50Hz continuous sampling"""
    logger.info("🔌 Initializing hardware...")

    try:
        global i2c_bus
        i2c_bus = smbus2.SMBus(1)

        # Configure GPIO
        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)

        # Setup output pins
        output_pins = [
            Config.PIN_CS_ADC, Config.PIN_CLK_ADC, Config.PIN_DAT_DAC,
            Config.PIN_CLK_DAC, Config.PIN_CS_DAC, Config.MAGNET1_GPIO,
            Config.MAGNET2_GPIO, Config.START_LED_GPIO, Config.HVAC_GPIO
        ]

        for pin in output_pins:
            GPIO.setup(pin, GPIO.OUT)
            GPIO.output(pin, GPIO.LOW)  # Initialize all outputs to LOW

        # Setup input pins with pull-up resistors
        GPIO.setup(Config.PIN_SDO_ADC, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        GPIO.setup(Config.START_TASTE_GPIO, GPIO.IN, pull_up_down=GPIO.PUD_UP)

        # Set initial pin states for SPI-like interfaces
        GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)  # Chip select inactive
        GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
        GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Chip select inactive
        GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
        GPIO.output(Config.PIN_DAT_DAC, GPIO.LOW)
        GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
        time.sleep(0.01)
        GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
        time.sleep(0.02)
        logger.info("✅ ADC initialized with soft reset")

        # Initial state for DAC
        GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Chip select inactive
        GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
        GPIO.output(Config.PIN_DAT_DAC, GPIO.LOW)
        logger.info("✅ DAC pins initialized")

        # Test I2C connection to motor controller
        try:
            with i2c_lock:
                i2c_bus.read_byte(Config.I2C_ADDR_MOTOR)
            logger.info("✅ Motor I2C connection successfully tested")

            # Read initial motor status
            motor_status = read_motor_status()
            if motor_status:
                logger.info("✅ Initial motor status: %s", motor_status)
        except Exception as e:
            logger.warning("⚠️ Motor I2C connection test failed: %s", e)

        # Test I2C connection to humidity/temperature sensor
        try:
            with i2c_lock:
                i2c_bus.read_byte(Config.HYT_ADDR)
            logger.info("✅ HYT sensor I2C connection successfully tested")
        except Exception as e:
            logger.warning("⚠️ HYT sensor I2C connection test failed: %s", e)

        # Start motor status monitoring thread
        threading.Thread(
            target=motor_status_monitor,
            daemon=True
        ).start()
        logger.info("✅ Motor status monitoring started")

        # Set the continuous sampling flag immediately
        continuous_sampling_active.set()
        
        # Start continuous ADC sampling thread at fixed 50Hz (20ms)
        threading.Thread(
            target=continuous_adc_sampling_thread,
            daemon=True
        ).start()
        logger.info("✅ Continuous 50Hz (20ms) ADC sampling started for all data")

        # Start ADC monitoring thread (handles state transitions, not sampling)
        threading.Thread(
            target=adc_sampling_thread,
            daemon=True
        ).start()
        logger.info("✅ ADC state monitoring thread started")

        # We no longer need the 100Hz Delphi simulation timer as we're using direct 50Hz sampling
        # Just log that we're using a different approach
        logger.info("ℹ️ Using direct 50Hz sampling instead of simulated 100Hz timer")

        # Start temperature/humidity monitoring
        threading.Thread(
            target=temperature_humidity_monitor,
            daemon=True
        ).start()
        logger.info("✅ Temperature and humidity monitoring started")

        # Start Q628 state machine thread
        threading.Thread(
            target=q628_state_machine,
            daemon=True
        ).start()
        logger.info("✅ Q628 state machine started")

        # Start LED control thread
        threading.Thread(
            target=led_control_thread,
            daemon=True
        ).start()
        logger.info("✅ LED status indication started")

        logger.info("✅ Hardware initialization completed successfully")
        return True

    except Exception as e:
        logger.error("❌ Hardware initialization failed: %s", e, exc_info=True)
        return False


def temperature_humidity_monitor() -> None:
    """
    Continuously monitor temperature and humidity
    and update system state.
    """
    logger.info("🌡️ Temperature and humidity monitoring started")

    while not shutdown_requested.is_set():
        try:
            # Read temperature and humidity
            temp, humidity = read_temperature_humidity()
            logger.debug(f"Temperature: {temp:.1f}°C, Humidity: {humidity:.1f}%")
            
            # Log values periodically (every minute)
            if int(time.time()) % 60 == 0:
                logger.info(f"🌡️ Temperature: {temp:.1f}°C, Humidity: {humidity:.1f}%")

        except Exception as e:
            logger.error(f"❌ Error reading temperature/humidity: {e}")

        # Wait for the defined interval
        time.sleep(Config.TEMP_HUMID_SAMPLE_INTERVAL)

    logger.info("🌡️ Temperature and humidity monitoring terminated")



def continuous_adc_sampling_thread() -> None:
    """
    Thread for continuous ADC sampling.
    Samples at approximately 60Hz (~16.67ms) like the GUI implementation.
    """
    logger.info("📊 Continuous ADC sampling started")
    sample_count = 0
    last_sample_time = time.time()
    
    # Use the same interval as the GUI (60Hz - MCP3553's natural rate)
    ACTUAL_SAMPLE_INTERVAL = Config.ADC_SAMPLE_INTERVAL  # ~16.67ms
    
    # Perform initial soft reset
    GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
    time.sleep(0.01) 
    GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
    time.sleep(0.02)  # Short delay after reset
    
    while not shutdown_requested.is_set():
        try:
            current_time = time.time()
            
            # Sample at fixed interval, but don't be too strict
            if current_time - last_sample_time >= ACTUAL_SAMPLE_INTERVAL:
                # Read ADC - get raw value directly
                raw_value = read_adc_22bit()  # Returns single int value
                
                if raw_value != 0:
                    # Store raw value directly without conversion
                    system_state.set("field_strength", raw_value)
                    system_state.set("last_adc_value", raw_value)
                    
                    # Calculate voltage like GUI does
                    vref = Config.VREF_ADC
                    voltage = (float(raw_value) / ((1 << 21) - 1)) * vref * 2
                    
                    # Store voltage and raw value
                    with temp_buffer_lock:
                        temp_buffer["latest_value"] = raw_value
                        temp_buffer["voltage"] = voltage
                        temp_buffer["timestamp"] = current_time
                    
                    # Always add to ADC buffer regardless of measurement state
                    adc_buffer.enqueue(raw_value)
                    system_state.add_adc_data(raw_value)
                    
                    # Update sample count for logging
                    sample_count += 1
                    
                    # Periodic logging
                    if sample_count % 60 == 0:  # Log approximately every second at 60Hz
                        logger.info(f"Raw ADC value: 0x{raw_value:06X} ({raw_value}) = {voltage:.6f}V")
                
                # Update last sample time
                last_sample_time = current_time
            
            # Short sleep to prevent CPU hogging
            time.sleep(0.001)  # Shorter sleep for more precise timing
                
        except Exception as e:
            logger.error(f"❌ Error in continuous ADC sampling: {e}")
            time.sleep(0.1)  # Short pause on errors

def delphi_timer_thread() -> None:
    """
    100Hz timer thread to create a constant 100Hz stream for Delphi
    from the ~60Hz actual ADC data in temp_buffer.
    Enforces strict sample count limit based on defined measurement time.
    """
    logger.info("⏱️ 100Hz Delphi timer thread started")
    measurement_active = False
    last_sample_time = 0
    sample_count = 0
    expected_total_samples = 0
    
    while not shutdown_requested.is_set():
        try:
            # Check if sampling is active
            if sampling_active.is_set():
                current_time = time.time()
                
                # If we just became active, reset timing and counters
                if not measurement_active:
                    measurement_active = True
                    last_sample_time = current_time
                    sample_count = 0
                    
                    # Calculate expected total samples for this measurement
                    defined_time = system_state.get("defined_measurement_time")
                    expected_total_samples = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
                    logger.info(f"Starting 100Hz timer for Delphi simulation - will generate EXACTLY {expected_total_samples} samples for {defined_time}s")
                
                # CRITICAL: Check if we've reached the exact sample count
                if sample_count >= expected_total_samples:
                    # We've reached the exact required number of samples - stop sampling
                    logger.info(f"✅ Reached exact sample count: {sample_count}/{expected_total_samples} - stopping measurement")
                    stop_sampling()
                    
                    # Force the state machine to finish the measurement
                    if system_state.get_q628_state() == Q628State.ACTIV:
                        system_state.set_q628_state(Q628State.FINISH)
                    
                    # Reset measurement active flag to avoid logging multiple times
                    measurement_active = False
                    continue
                
                # Check if it's time for a new 10ms sample
                # We want exactly 100 samples per second
                elapsed = current_time - last_sample_time
                if elapsed >= 0.01:  # 10ms (100Hz)
                    # Get the most recent value from temp buffer (thread-safe)
                    with temp_buffer_lock:
                        latest_value = temp_buffer["latest_value"]
                    
                    # Add to Delphi buffer (100Hz)
                    adc_buffer.enqueue(latest_value)
                    sample_count += 1
                    
                    # Update timing - keep perfect 10ms intervals
                    last_sample_time += 0.01  # Add exactly 10ms
                    
                    # Log progress periodically (more frequently for debugging)
                    if sample_count % 1000 == 0:  # Every 10 seconds (1000 samples)
                        logger.info(f"Delphi timer: Generated {sample_count}/{expected_total_samples} samples at 100Hz - {sample_count/expected_total_samples*100:.1f}%")
                
                    # If we're more than 20ms behind, reset the timer to avoid huge catch-up loops
                    if current_time - last_sample_time > 0.02:
                        logger.warning(f"Delphi timer fell behind by {(current_time - last_sample_time)*1000:.1f}ms, resetting")
                        last_sample_time = current_time
            else:
                # Reset when sampling stops
                if measurement_active:
                    # Log stats when stopping
                    if sample_count > 0:
                        logger.info(f"Delphi timer stopped - generated {sample_count}/{expected_total_samples} samples at 100Hz")
                    
                    measurement_active = False
                    sample_count = 0
            
            # Sleep precisely to maintain timing
            time.sleep(0.001)  # 1ms for precise timing
            
        except Exception as e:
            logger.error(f"Error in Delphi timer thread: {e}")
            time.sleep(0.01)

def get_current_ip() -> str:
    """Get current IP address with error handling and fallback"""
    try:
        # Try to get IP from hostname command
        output = subprocess.check_output("hostname -I", shell=True)
        ip_addresses = output.decode().strip().split()

        if ip_addresses:
            # Filter out localhost and IPv6 addresses
            valid_ips = [ip for ip in ip_addresses
                         if ip != "127.0.0.1" and ":" not in ip]

            if valid_ips:
                ip = valid_ips[0]  # Take first valid IP
                logger.info("🌐 Current IP (DHCP): %s", ip)
                system_state.set("last_ip", ip)
                return ip

        # Fallback: try socket method
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # This doesn't actually establish a connection
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            logger.info("🌐 Current IP (socket method): %s", ip)
            system_state.set("last_ip", ip)
            return ip

    except Exception as e:
        logger.error("❌ Could not determine IP address: %s", e)
        return "0.0.0.0"


def wait_for_button(msg: str, timeout: Optional[float] = None) -> bool:
    """
    Wait for button press with timeout and debouncing

    Args:
        msg: Message to log while waiting
        timeout: Optional timeout in seconds

    Returns:
        True if button was pressed, False if timeout or shutdown requested
    """
    logger.info("⏳ %s (waiting for button press)...", msg)

    start_time = time.time()
    debounce_time = Config.BUTTON_DEBOUNCE_TIME

    # Wait for button press (HIGH to LOW transition)
    while GPIO.input(Config.START_TASTE_GPIO) == GPIO.HIGH:
        if shutdown_requested.is_set():
            return False
        if timeout and (time.time() - start_time > timeout):
            logger.info("⏰ Timeout waiting for button press (%ss)", timeout)
            return False
        time.sleep(Config.BUTTON_POLL_INTERVAL)

    # Button pressed - debounce
    time.sleep(debounce_time)
    if GPIO.input(Config.START_TASTE_GPIO) == GPIO.HIGH:
        # False trigger
        return wait_for_button(msg, timeout)

    logger.info("👇 Button pressed")

    # Set button press time for timer calculation
    system_state.set_button_press_time()

    # Wait for button release (LOW to HIGH transition)
    while GPIO.input(Config.START_TASTE_GPIO) == GPIO.LOW:
        if shutdown_requested.is_set():
            return False
        time.sleep(Config.BUTTON_POLL_INTERVAL)

    # Button released - debounce
    time.sleep(debounce_time)
    logger.info("👆 Button released")
    return True


def blink_led(mode: str = "slow", duration: float = 3.0) -> None:
    """
    Blink LED with specified pattern

    Args:
        mode: "slow" or "fast" blinking
        duration: How long to blink in seconds
    """
    interval = (Config.LED_BLINK_FAST if mode == "fast"
                else Config.LED_BLINK_SLOW)

    logger.debug("💡 LED blinking %s for %.1fs...", mode, duration)

    try:
        end_time = time.time() + duration

        while time.time() < end_time and not shutdown_requested.is_set():
            GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
            time.sleep(interval)
            if shutdown_requested.is_set():
                break
            GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
            time.sleep(interval)
    except Exception as e:
        logger.error("❌ Error during LED blinking: %s", e)
    finally:
        # Ensure LED is off when done
        try:
            GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
        except:
            pass

def led_control_thread() -> None:
    """
    Control LED status indication based on system state.

    LED patterns:
    - Slow blink: System initializing or in IDLE
    - Solid on: Ready to start (after 'S' command, waiting for button)
    - Fast blink: Measurement in progress (after button press)
    """
    logger.info("💡 LED control thread started")

    led_state = False
    shutdown_blink_count = 0  # Limited number of blinks during shutdown

    while not shutdown_requested.is_set():
        try:
            current_state = system_state.get_q628_state()
            measurement_active = system_state.is_measurement_active()
            
            # Check if waiting for button press (after 'S' command)
            waiting_for_button = (current_state == Q628State.WAIT_START_SW)

            if current_state in [Q628State.POWER_UP, Q628State.STARTUP, Q628State.PRE_IDLE]:
                # Slow blinking during initialization
                blink_pattern = Config.LED_BLINK_SLOW
                led_state = not led_state
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH if led_state else GPIO.LOW)
                time.sleep(blink_pattern)
            elif current_state == Q628State.IDLE:
                # Slow blinking in IDLE state too
                blink_pattern = Config.LED_BLINK_SLOW
                led_state = not led_state
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH if led_state else GPIO.LOW)
                time.sleep(blink_pattern)
            elif waiting_for_button:
                # Solid on when waiting for button press
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
                led_state = True
                time.sleep(0.1)
            elif measurement_active:
                # Fast blinking during any active measurement phase
                # (WAIT_HV, START, PRE_ACTIV, ACTIV)
                blink_pattern = Config.LED_BLINK_FAST
                led_state = not led_state
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH if led_state else GPIO.LOW)
                time.sleep(blink_pattern)
            else:
                # Default pattern for other states: slow blinking
                blink_pattern = Config.LED_BLINK_SLOW
                led_state = not led_state
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH if led_state else GPIO.LOW)
                time.sleep(blink_pattern)

        except Exception as e:
            logger.error("❌ Error in LED control thread: %s", e)
            try:
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)  # Error fallback - constant on
                led_state = True
            except:
                pass
            time.sleep(1.0)

    # Blink during shutdown (limited number)
    try:
        while shutdown_blink_count < 10:  # Maximum 10 blinks during shutdown
            led_state = not led_state
            GPIO.output(Config.START_LED_GPIO, GPIO.HIGH if led_state else GPIO.LOW)
            time.sleep(Config.LED_BLINK_SLOW)
            shutdown_blink_count += 1
        
        # Ensure LED is off at the end
        GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
    except:
        # Ensure LED is off if interrupted
        try:
            GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
        except:
            pass

    logger.info("💡 LED control thread terminated")


def set_valve(status: ValveStatus) -> None:
    """
    Set valve status (open/closed)

    Args:
        status: ValveStatus.OPEN or ValveStatus.CLOSED
    """
    status_str = str(status)
    logger.info("🔧 Setting valve: %s", status_str.upper())

    try:
        system_state.set("valve_status", status)

        # Both magnets are controlled together
        gpio_state = GPIO.HIGH if status == ValveStatus.OPEN else GPIO.LOW
        GPIO.output(Config.MAGNET1_GPIO, gpio_state)
        GPIO.output(Config.MAGNET2_GPIO, gpio_state)
    except Exception as e:
        logger.error("❌ Error setting valve: %s", e)
        system_state.set("last_error", f"Valve error: {str(e)}")


def read_motor_status() -> Optional[MotorStatus]:
    """
    Read motor status via I2C with improved error handling
    """
    try:
        with i2c_lock:
            # First send passive mode command with proper timing
            i2c_bus.write_byte(Config.I2C_ADDR_MOTOR, Config.CMD_PASSIV)
            time.sleep(0.05)  # Increased delay for stability

            # Then send status request command
            i2c_bus.write_byte(Config.I2C_ADDR_MOTOR, Config.CMD_STATUS)
            time.sleep(0.05)  # Increased delay for stability

            # Read status byte
            status_byte = i2c_bus.read_byte(Config.I2C_ADDR_MOTOR)
            
            # Log raw status for debugging
            logger.debug(f"Raw motor status byte: 0x{status_byte:02X} (binary: {bin(status_byte)[2:].zfill(8)})")

        # Convert to MotorStatus object
        status = MotorStatus.from_byte(status_byte)

        # Update system state
        system_state.update_motor_status(status, status_byte)

        return status
    except Exception as e:
        logger.error("❌ Error reading motor status: %s", e)
        return None


def motor_status_monitor() -> None:
    """
    Continuously monitor motor status in a separate thread
    """
    logger.info("🔄 Motor status monitoring started")
    error_logged = False  # Track if we've already logged an error
    last_status = None

    while not shutdown_requested.is_set():
        try:
            status = read_motor_status()
            if status:
                # Log full status periodically
                if status != last_status:
                    logger.debug("Motor status changed: %s", status)
                    last_status = status

                # If motor is in error state, log it (but avoid spamming logs)
                if status & MotorStatus.ERROR:
                    if not error_logged:
                        logger.warning("⚠️ Motor error detected! Status: %s", status)
                        error_logged = True
                else:
                    # Reset the error logged flag when error clears
                    error_logged = False

                # If motor just reached home position, log it
                prev_status = system_state.get("motor_status")
                if (status & MotorStatus.HOME) and not (prev_status & MotorStatus.HOME):
                    logger.info("🏠 Motor reached home position")

                # If motor just finished being busy, log it
                if (prev_status & MotorStatus.BUSY) and not (status & MotorStatus.BUSY):
                    logger.info("✅ Motor operation completed")
        except Exception as e:
            logger.error("❌ Error in motor status monitor: %s", e)

        # Sleep before next poll
        time.sleep(Config.MOTOR_STATUS_POLL_INTERVAL)


def move_motor(position: MotorPosition) -> bool:
    """
    Move motor to specified position

    Args:
        position: MotorPosition.UP or MotorPosition.DOWN

    Returns:
        True if successful, False if error occurred
    """
    position_str = str(position)
    logger.info("🛠️ Moving motor: %s", position_str.upper())

    try:
        system_state.set("motor_position", position)

        # Check current motor status
        status = read_motor_status()
        if status and (status & MotorStatus.BUSY):
            logger.warning("⚠️ Motor is busy, waiting before sending new command")
            # Wait for motor to be ready (up to 5 seconds)
            for _ in range(50):  # 50 * 0.1s = 5s
                if shutdown_requested.is_set():
                    return False
                time.sleep(0.1)
                status = read_motor_status()
                if status and not (status & MotorStatus.BUSY):
                    break
            else:
                logger.error("❌ Motor still busy after timeout, forcing command")

        # Send appropriate command to motor controller
        cmd = Config.CMD_UP if position == MotorPosition.UP else Config.CMD_DOWN
        with i2c_lock:
            i2c_bus.write_byte(Config.I2C_ADDR_MOTOR, cmd)

        # Wait briefly and check if command was accepted
        time.sleep(0.1)
        status = read_motor_status()
        if status and (status & MotorStatus.BUSY):
            logger.info("✅ Motor command accepted, motor is now busy")
            return True
        else:
            return True  # Still return True as we did send the command
    except Exception as e:
        logger.error("❌ Error moving motor: %s", e)
        system_state.set("last_error", f"Motor error: {str(e)}")
        return False


def set_high_voltage(enabled: bool) -> None:
    """
    Enable or disable high voltage (both AC and DC)
    """
    status_text = 'ON' if enabled else 'OFF'
    logger.info("⚡ High voltage: %s", status_text)

    try:
        system_state.set("hv_status", enabled)
        
        # Hauptsteuerung über HVAC_GPIO
        GPIO.output(Config.HVAC_GPIO, GPIO.HIGH if enabled else GPIO.LOW)
        
        # Zusätzlich bei Aktivierung Werte loggen
        if enabled:
            hv_ac = system_state.get("hv_ac_value")
            hv_dc = system_state.get("hv_dc_value")
            logger.info(f"⚡ Applied voltage values - AC:{hv_ac}V, DC:{hv_dc}V")
    except Exception as e:
        logger.error("❌ Error setting high voltage: %s", e)
        system_state.set("last_error", f"HV error: {str(e)}")

def read_temperature_humidity() -> Tuple[float, float]:
    """
    Read temperature and humidity from HYT sensor

    Returns:
        Tuple of (temperature in °C, humidity in %)
    """
    try:
        # Start measurement
        with i2c_lock:
            i2c_bus.write_byte(Config.HYT_ADDR, 0x00)
        time.sleep(0.1)  # Wait for measurement

        # Read 4 bytes of data
        with i2c_lock:
            data = i2c_bus.read_i2c_block_data(Config.HYT_ADDR, 0, 4)

        # First two bytes: humidity (14 bits)
        # Last two bytes: temperature (14 bits)
        humidity_raw = ((data[0] & 0x3F) << 8) | data[1]
        temp_raw = (data[2] << 6) | (data[3] >> 2)

        # Convert to physical values
        humidity = (humidity_raw / 16383.0) * 100.0
        temp = (temp_raw / 16383.0) * 165.0 - 40.0

        # Update system state
        system_state.update(temperature=temp, humidity=humidity)

        return (temp, humidity)
    except Exception as e:
        logger.error("❌ Error reading temperature/humidity: %s", e)
        return (23.5, 45.0)  # Return default values on error

def read_adc_22bit() -> int:
    """
    Read 22-bit raw value from MCP3553SN ADC via bit-banged SPI.
    Implements soft reset AFTER each sample for more reliable operation.
    
    Returns:
        Raw signed integer ADC value with no conversion
    """
    with adc_lock:
        try:
            # Wait for SDO to go LOW (indicates data ready)
            ready = False
            timeout_start = time.time()
            timeout_limit = 0.1  # Reduced timeout for more responsive behavior
            
            while time.time() - timeout_start < timeout_limit:
                if GPIO.input(Config.PIN_SDO_ADC) == GPIO.LOW:
                    ready = True
                    break
                time.sleep(0.0005)  # Check more frequently
            
            if not ready:
                logger.warning("MCP3553 data not ready - performing soft reset and returning 0")
                # Force reset when not ready
                GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
                time.sleep(0.01)
                GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
                return 0
            
            # SDO is LOW - read 24 bits from the ADC (MSB first)
            value = 0
            for _ in range(24):
                GPIO.output(Config.PIN_CLK_ADC, GPIO.HIGH)
                time.sleep(5e-6)  # 5μs for clock pulse (like in GUI)
                bit = GPIO.input(Config.PIN_SDO_ADC)
                value = (value << 1) | bit
                GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
                time.sleep(5e-6)  # 5μs for clock pulse
            
            # Extract overflow flags and actual 22-bit value
            ovh = (value >> 23) & 0x01
            ovl = (value >> 22) & 0x01
            signed_value = value & 0x3FFFFF
            
            # Handle two's complement for negative values
            if signed_value & (1 << 21):
                signed_value -= (1 << 22)
            
            # CRITICAL: Perform soft reset AFTER each sample, not before
            # This matches the successful example code pattern from adc_gui.py
            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
            time.sleep(0.01)
            GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
            
            # Debug info - show raw hex value
            if logger.level <= logging.DEBUG:
                hex_str = f"0x{value:06X}"
                binary_str = format(value, '024b')
                logger.debug(f"Raw ADC: {hex_str}, Binary: {binary_str}, Value: {signed_value}, OVL={ovl}, OVH={ovh}")
            
            return signed_value
            
        except Exception as e:
            logger.error(f"❌ MCP3553 SPI read error: {e}", exc_info=True)
            # Ensure CS is reset on error
            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
            time.sleep(0.01)
            GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
            return 0
            

def read_adc() -> Tuple[float, int]:
    """
    Read MCP3553 ADC value and return voltage and raw value.
    Acts as main interface for ADC readings.
    """
    return read_adc_22bit()

def set_dac(value: int) -> None:
    """
    Set DAC value using bit-banged SPI with timing from dac_gui.py
    """
    with dac_lock:
        try:
            # Ensure value is 16-bit
            value &= 0xFFFF

            # Start transmission
            GPIO.output(Config.PIN_CS_DAC, GPIO.LOW)
            time.sleep(Config.DAC_CLK_DELAY)  # Use 10μs like in dac_gui.py

            # Send 16 bits MSB first
            for i in range(15, -1, -1):
                bit = (value >> i) & 1
                GPIO.output(Config.PIN_DAT_DAC, bit)
                time.sleep(Config.DAC_CLK_DELAY)  # Wait before clock high
                GPIO.output(Config.PIN_CLK_DAC, GPIO.HIGH)
                time.sleep(Config.DAC_CLK_DELAY)  # Hold clock high
                GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
                time.sleep(Config.DAC_CLK_DELAY)  # Hold clock low

            # End transmission
            GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)
        except Exception as e:
            logger.error("❌ Error setting DAC: %s", e)
            GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Ensure CS is high on error


def set_dac_voltage(voltage: float) -> None:
    """
    Set DAC output voltage with gain compensation like in dac_gui.py
    """
    # Apply gain compensation (divide by GAIN like in dac_gui.py)
    v_dac = voltage / Config.DAC_GAIN
    
    # Calculate DAC value using the same method as dac_gui.py
    dac_value = int((v_dac / Config.VREF_DAC) * 65535)
    dac_value = max(0, min(65535, dac_value))  # Limit to valid range
    
    # Send to DAC
    set_dac(dac_value)
    logger.debug(f"DAC set to {voltage:.3f}V (DAC input: {v_dac:.3f}V, value: {dac_value} (0x{dac_value:04X}))")

def adc_sampling_thread() -> None:
    """
    Legacy ADC sampling thread - now just monitors sampling status.
    Actual sampling happens in continuous_adc_sampling_thread.
    """
    logger.info("🔄 ADC sampling monitor started (sampling now handled by continuous thread)")
    
    while not shutdown_requested.is_set():
        try:
            # Only handle state transitions and logging
            if sampling_active.is_set():
                # Monitor sampling status for logging purposes
                current_time = time.time()
                button_press_time = system_state.get("button_press_time")
                
                if button_press_time is not None:
                    elapsed_time = current_time - button_press_time
                    
                    # Check if measurement should end
                    defined_time = system_state.get("defined_measurement_time")
                    if elapsed_time >= defined_time:
                        logger.info(f"⏱️ Measurement duration reached: {elapsed_time:.2f}s / {defined_time}s")
                        
                        # Get real sample count
                        samples = len(adc_buffer.get_all())
                        logger.info(f"Measurement complete with {samples} actual samples at 50Hz")
                        
                        # End sampling state
                        sampling_active.clear()
                        system_state.stop_sampling()
                        
                        # Transition state machine
                        if system_state.get_q628_state() == Q628State.ACTIV:
                            system_state.set_q628_state(Q628State.FINISH)
            
            # Sleep longer - just monitoring state
            time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"❌ Error in ADC sampling monitor: {e}", exc_info=True)
            time.sleep(1.0)


def test_mcp3553_timing() -> None:
    """
    Test function to measure actual MCP3553 conversion rate.
    Helps verify if we're getting expected ~60Hz data rate.
    """
    logger.info("Starting MCP3553 timing test...")
    samples = []
    times = []
    
    # Collect 120 samples (should take ~2 seconds at 60Hz)
    for i in range(120):
        start_time = time.time()
        # Wait for ADC ready (SDO = LOW)
        timeout = start_time + 1.0
        while GPIO.input(Config.PIN_SDO_ADC) == GPIO.HIGH:
            if time.time() > timeout:
                logger.error("ADC ready timeout")
                break
            time.sleep(0.001)
            
        # Record time when SDO went LOW
        ready_time = time.time()
        
        # Read ADC value
        voltage, raw_value = read_adc()
        samples.append(raw_value)
        times.append(ready_time)
        
        # Log every 10 samples
        if i % 10 == 0:
            logger.info(f"Sample {i}: {raw_value} ({voltage:.6f}V)")
    
    # Calculate actual conversion rate
    if len(times) > 1:
        intervals = [times[i+1] - times[i] for i in range(len(times)-1)]
        avg_interval = sum(intervals) / len(intervals)
        rate = 1.0 / avg_interval
        logger.info(f"MCP3553 test complete. Average interval: {avg_interval*1000:.2f}ms, Rate: {rate:.2f}Hz")
        
        # Histogram of intervals
        interval_ms = [int(i*1000) for i in intervals]
        from collections import Counter
        histogram = Counter(interval_ms)
        logger.info(f"Interval histogram (ms): {dict(sorted(histogram.items()))}")
    else:
        logger.error("Not enough samples collected for MCP3553 timing test")


def start_sampling() -> None:
    """
    Start official measurement sampling.
    Note: ADC is continuously sampled regardless, this just sets the state.
    """
    logger.info("📊 Starting official measurement (ADC continuously sampled at 50Hz)")
    # Don't clear the buffer - we're continuously sampling
    # Just mark the sampling as active for the state machine
    system_state.start_sampling()
    sampling_active.set()

# Create a thread-safe flag to coordinate measurement ending
measurement_ending_lock = threading.Lock()
measurement_already_ending = False

def stop_sampling() -> None:
    """Stop ADC sampling with coordinated shutdown to prevent duplicates"""
    global measurement_already_ending
    
    with measurement_ending_lock:
        # Check if measurement is already being stopped by another thread
        if measurement_already_ending:
            logger.debug("📊 Sampling stop already in progress - skipping redundant call")
            return
            
        # Mark measurement as ending
        measurement_already_ending = True
    
    logger.info("📊 Stopping ADC sampling - enforcing measurement end") 
    sampling_active.clear()
    system_state.stop_sampling()  # Clear sampling active flag in system state
    
    # Log buffer statistics
    defined_time = system_state.get("defined_measurement_time")
    expected_samples = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
    current_samples = len(adc_buffer.get_all())
    
    logger.info(f"📊 Sampling stopped with {current_samples}/{expected_samples} samples in buffer")
    
    # Reset flag after a short delay to allow for proper cleanup
    def reset_flag():
        global measurement_already_ending
        time.sleep(1.0)  # Wait for other threads to notice sampling is inactive
        with measurement_ending_lock:
            measurement_already_ending = False
    
    threading.Thread(target=reset_flag, daemon=True).start()

def q628_state_machine() -> None:
    """
    Main Q628 state machine - Implements the final version with special timing rules:
    - HV off exactly 3.0s after motor down (modified from 1.5s)
    - Valves opened whenever motor moves
    - 3.5s delay after motor positioning before closing valves (modified from 1.5s)
    - Reports sample statistics at end of measurement
    """
    logger.info("🔄 Q628 state machine started")

    # Initial state
    system_state.set_q628_state(Q628State.POWER_UP)
    
    # Track motor movement events
    motor_down_time = None
    measurement_samples = 0
    
    # Main state machine loop
    while not shutdown_requested.is_set():
        try:
            # Get current state and elapsed time
            current_state = system_state.get_q628_state()
            elapsed = system_state.get_q628_elapsed()

            # State machine transitions
            if current_state == Q628State.POWER_UP:
                # Initial power-up state
                if elapsed > 2.0:
                    # After 2 seconds, transition to STARTUP
                    system_state.set_q628_state(Q628State.STARTUP)

            elif current_state == Q628State.STARTUP:
                # System startup - initialize hardware
                # Disable HV
                set_high_voltage(False)

                # Close valve
                set_valve(ValveStatus.CLOSED)

                # Before moving motor, ensure valves are open
                set_valve(ValveStatus.OPEN)
                logger.info("🔧 Opening valves for motor movement")
                
                # Move motor to home position
                move_motor(MotorPosition.UP)

                # Transition to PRE_IDLE
                system_state.set_q628_state(Q628State.PRE_IDLE)

            elif current_state == Q628State.PRE_IDLE:
                # Preparing for idle state
                if elapsed > 3.0:
                    # 3 seconds have passed, now wait 3.5 more seconds with valves open
                    if elapsed > 6.5:  # 3.0 + 3.5 seconds
                        # Only close valves 3.5 seconds after motor movement complete
                        set_valve(ValveStatus.CLOSED)
                        logger.info("🔧 Closed valves 3.5 seconds after motor reached position")

                        # Transition to IDLE
                        system_state.set_q628_state(Q628State.IDLE)

                        # LED blinks slowly in IDLE state
                        threading.Thread(
                            target=blink_led,
                            args=("slow", 3.0),
                            daemon=True
                        ).start()

            elif current_state == Q628State.IDLE:
                # System idle, waiting for commands
                # Reset measurement sample counter
                measurement_samples = 0
                
                # Check if start command received
                if enable_from_pc.is_set() or simulate_start.is_set():
                    if simulate_start.is_set():
                        # Automatic start (simulated button press)
                        logger.info("✅ Automatic measurement start (simulated)")
                        simulate_start.clear()

                        # Simulate button press - set timestamp
                        system_state.set_button_press_time()

                        # Turn on High Voltage immediately 
                        logger.info("⚡ Turning on High Voltage immediately")
                        set_high_voltage(True)
                        
                        # Start measurement and sampling
                        adc_buffer.clear()
                        system_state.clear_adc_data()
                        start_sampling()
                        system_state.start_measurement()

                        # Skip waiting for button, go directly to fixed initialization phase
                        system_state.set_q628_state(Q628State.WAIT_HV)
                    else:
                        # Manual start (requires button press)
                        logger.info("✅ Measurement enabled by PC command, waiting for button press")
                        enable_from_pc.clear()

                        # Turn ON LED steady while waiting for button
                        GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)

                        # Transition to WAIT_START_SW
                        system_state.set_q628_state(Q628State.WAIT_START_SW)

            elif current_state == Q628State.WAIT_START_SW:
                # When button pressed
                if GPIO.input(Config.START_TASTE_GPIO) == GPIO.LOW:
                    # Button pressed, debounce
                    time.sleep(Config.BUTTON_DEBOUNCE_TIME)
                    if GPIO.input(Config.START_TASTE_GPIO) == GPIO.LOW:
                        logger.info("👇 Button pressed, starting measurement")

                        # Important: Store button press time
                        system_state.set_button_press_time()
                        
                        # Turn on High Voltage immediately upon button press
                        logger.info("⚡ Turning on High Voltage immediately upon button press")
                        set_high_voltage(True)
                        
                        # START MEASUREMENT AND SAMPLING
                        adc_buffer.clear()
                        system_state.clear_adc_data()
                        start_sampling()
                        system_state.start_measurement()

                        # Transition to fixed initialization waiting phase
                        system_state.set_q628_state(Q628State.WAIT_HV)
                        
                        # Fast LED blinking during measurement
                        threading.Thread(
                            target=blink_led,
                            args=("fast", Config.FIXED_INITIALIZATION_TIME),
                            daemon=True
                        ).start()

                # Check for timeout (2 minutes)
                if elapsed > 120.0:
                    logger.warning("⏰ Timeout waiting for button press")
                    GPIO.output(Config.START_LED_GPIO, GPIO.LOW)

                    # Return to IDLE
                    system_state.set_q628_state(Q628State.IDLE)

            elif current_state == Q628State.WAIT_HV:
                # Fixed initialization time instead of profile-based time
                # HV is already on, just waiting for the fixed time
                
                if elapsed > Config.FIXED_INITIALIZATION_TIME:
                    logger.info(f"⏱️ Fixed initialization time of {Config.FIXED_INITIALIZATION_TIME}s completed")
                    
                    # After fixed time, transition to START state
                    # (Open valves & move motor down)
                    system_state.set_q628_state(Q628State.START)

            elif current_state == Q628State.START:
                # MODIFIED: Always ensure valves are open before moving motor
                logger.info("🔧 Opening valves and moving motor down")
                
                # Open valves (activate both magnets)
                set_valve(ValveStatus.OPEN)

                # Move motor down
                move_motor(MotorPosition.DOWN)
                
                # Store the exact time when motor down command was issued
                motor_down_time = time.time()
                logger.info(f"⏱️ Motor down command sent at {motor_down_time:.3f}")

                # Transition to PRE_ACTIV
                system_state.set_q628_state(Q628State.PRE_ACTIV)

            elif current_state == Q628State.PRE_ACTIV:
                # Wait for motor movement to complete (bottom position reached)
                motor_status = system_state.get("motor_status")
                
                # Check if motor has finished moving (not busy)
                if not (motor_status & MotorStatus.BUSY) or elapsed > 5.0:
                    # MODIFIED: Turn HV off exactly 3.0 seconds after motor reached bottom
                    if motor_down_time is not None:
                        time_since_motor_down = time.time() - motor_down_time
                        
                        if time_since_motor_down >= 3.0:  # CHANGED from 1.5 to 3.0 seconds
                            logger.info(f"⚡ Exactly 3.0 seconds after motor down - turning OFF High Voltage")
                            set_high_voltage(False)
                            
                            # Transition to ACTIV state
                            system_state.set_q628_state(Q628State.ACTIV)
                            logger.info("📊 Active measurement phase started")
                        else:
                            # Not yet 3.0 seconds - calculate remaining wait time
                            remaining = 3.0 - time_since_motor_down
                            logger.debug(f"⏱️ Waiting {remaining:.3f}s more before turning off HV")
                            # Stay in this state until exactly 3.0s has passed
                    else:
                        logger.warning("⚠️ Motor down time not recorded, using elapsed time")
                        if elapsed > 3.0:  # CHANGED from 1.5 to 3.0 seconds
                            logger.info("⚡ Turning OFF High Voltage (using elapsed time fallback)")
                            set_high_voltage(False)
                            
                            # Transition to ACTIV
                            system_state.set_q628_state(Q628State.ACTIV)
                            logger.info("📊 Active measurement phase started")

            elif current_state == Q628State.ACTIV:
                # Get precise timing information
                button_press_time = system_state.get("button_press_time")
                defined_time = system_state.get("defined_measurement_time")
                
                if button_press_time is not None:
                    elapsed_since_start = time.time() - button_press_time
                    
                    # Update sample count during measurement
                    current_samples = len(adc_buffer.get_all())
                    if current_samples > measurement_samples:
                        # Only log when count increases by 100+ samples
                        if current_samples - measurement_samples >= 100:
                            logger.debug(f"📊 Collected {current_samples} samples so far in this measurement")
                            measurement_samples = current_samples
                    
                    # CRITICAL: Enforce exact measurement time down to milliseconds
                    if elapsed_since_start >= defined_time:
                        logger.info(f"⏱️ Exact measurement time reached: {elapsed_since_start:.3f}s / {defined_time}s")
                        
                        # Stop sampling
                        stop_sampling()
                        
                        # Move to FINISH state
                        system_state.set_q628_state(Q628State.FINISH)
                    
                    # CRITICAL: Also enforce exact sample count
                    expected_samples = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
                    current_samples = len(adc_buffer.get_all())
                    
                    if current_samples >= expected_samples:
                        logger.info(f"⏱️ Exact sample count reached: {current_samples}/{expected_samples}")
                        
                        # Stop sampling
                        stop_sampling()
                        
                        # Move to FINISH state
                        system_state.set_q628_state(Q628State.FINISH)
                
                # Very short sleep time for precise timing
                time.sleep(0.001)

            elif current_state == Q628State.FINISH:
                # Ensure sampling is stopped
                if sampling_active.is_set() or system_state.is_sampling_active():
                    stop_sampling()
                    logger.info("📊 Ensuring sampling is stopped")
                
                # MODIFIED: Open valves before motor movement
                logger.info("🔧 Opening valves for motor movement")
                set_valve(ValveStatus.OPEN)
                
                # Move motor up
                logger.info("🔧 Moving motor back up to home position")
                move_motor(MotorPosition.UP)
                
                # Wait for motor to reach home position
                wait_for_motor_home(timeout=10)
                
                # MODIFIED: Wait exactly 3.5 seconds after motor reached position
                logger.info("⏱️ Waiting exactly 3.5 seconds after motor reached home position")
                time.sleep(3.5)  # CHANGED from 1.5 to 3.5 seconds
                
                # Close valves (deactivate magnets)
                logger.info("🔧 Closing valves 3.5s after motor reached home")
                set_valve(ValveStatus.CLOSED)

                # Report sample statistics at end of measurement
                total_samples = len(adc_buffer.get_all()) 
                defined_time = system_state.get("defined_measurement_time")
                expected_samples = int(defined_time * Config.SIMULATED_SAMPLE_RATE)
                
                # Provide comprehensive measurement summary at the end
                logger.info("=" * 80)
                logger.info(f"📊 MEASUREMENT COMPLETE - Sample Statistics:")
                logger.info(f"📊 Total samples collected: {total_samples}")
                logger.info(f"📊 Expected samples: {expected_samples}")
                logger.info(f"📊 Samples sent to client: {last_sent_index}")
                logger.info(f"📊 Measurement duration: {defined_time}s")
                logger.info(f"📊 Effective sample rate: {total_samples/defined_time:.2f} samples/second")
                
                if total_samples < expected_samples:
                    logger.warning(f"⚠️ Collected fewer samples than expected ({total_samples} vs {expected_samples})")
                elif total_samples > expected_samples:
                    logger.warning(f"⚠️ Collected more samples than expected ({total_samples} vs {expected_samples})")
                else:
                    logger.info(f"✅ Collected exactly the expected number of samples: {total_samples}")
                logger.info("=" * 80)

                # End measurement
                system_state.end_measurement()

                # Return to IDLE
                system_state.set_q628_state(Q628State.IDLE)

            # Sleep time to reduce CPU load
            # Short for ACTIV, longer for other states
            if current_state == Q628State.ACTIV:
                time.sleep(0.001)  # Very responsive during active measurement
            else:
                time.sleep(0.1)    # Less responsive for other states

        except Exception as e:
            logger.error("❌ Error in Q628 state machine: %s", e, exc_info=True)

            # Safety measures in case of error
            try:
                set_high_voltage(False)
                set_valve(ValveStatus.CLOSED)
                stop_sampling()
                system_state.end_measurement()

                # Return to IDLE after error
                system_state.set_q628_state(Q628State.IDLE)
            except Exception as cleanup_error:
                logger.error("Error during emergency cleanup: %s", cleanup_error)

            time.sleep(1.0)  # Longer sleep after error

def wait_for_motor_home(timeout: float = 10.0) -> bool:
    """
    Wait for motor to reach home position

    Args:
        timeout: Maximum time to wait in seconds

    Returns:
        True if motor reached home, False if timeout or error
    """
    logger.info("⏳ Waiting for motor to reach home position (timeout: %.1fs)", timeout)
    start_time = time.time()

    while time.time() - start_time < timeout:
        if shutdown_requested.is_set():
            return False

        status = read_motor_status()
        if status:
            # If motor is at home and not busy, we're done
            if (status & MotorStatus.HOME) and not (status & MotorStatus.BUSY):
                logger.info("✅ Motor reached home position")
                return True

            # If motor has error, log and return
            if status & MotorStatus.ERROR:
                logger.error("❌ Motor error while waiting for home position")
                return False

        time.sleep(0.5)  # Check every 500ms

    logger.warning("⏰ Timeout waiting for motor to reach home position")
    return False


def parse_ttcp_cmd(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse Delphi-compatible TTCP_CMD packet - ignoring profile settings
    as per the new process flow spec
    """
    try:
        if len(data) < 32:  # Delphi sends 8 x 4-byte ints = 32 bytes
            logger.error("Invalid TTCP_CMD size: %d bytes", len(data))
            return None

        # Unpack 8 integers from the data
        size, cmd_int, runtime, hvac, hvdc, profile1, profile2, wait_acdc = struct.unpack("<8i", data[:32])

        # Convert command integer to character
        cmd_char = chr(cmd_int) if 32 <= cmd_int <= 126 else '?'
        cmd_type = next((c for c in CommandType if c.value == cmd_char), CommandType.UNKNOWN)

        # GEÄNDERT: Logge, dass Profile ignoriert werden
        if profile1 != 0 or profile2 != 0 or wait_acdc != 0:
            logger.info("⚠️ Profile settings received but will be IGNORED (profile1=%d, profile2=%d, wait_acdc=%d)",
                    profile1, profile2, wait_acdc)

        logger.info("Parsed TTCP_CMD: %s (runtime=%ds, HVAC=%d, HVDC=%d)",
                    cmd_char, runtime, hvac, hvdc)

        # Update system state with received parameters
        # Die Profile werden zwar gespeichert, aber in der State-Machine nicht verwendet
        system_state.update(
            profile_hin=profile1,            # Wird gespeichert, aber ignoriert
            profile_zurueck=profile2,        # Wird gespeichert, aber ignoriert
            wait_acdc=wait_acdc,             # Wird gespeichert, aber ignoriert
            hv_ac_value=hvac,                # Wird für AC-Spannung verwendet
            hv_dc_value=hvdc                 # Wird für DC-Spannung verwendet
        )

        # When receiving START command, set the defined measurement time
        if cmd_type == CommandType.START:
            system_state.update(defined_measurement_time=runtime)
            logger.info(f"📏 Set defined measurement time to {runtime} seconds")

        return {
            'size': size,
            'cmd': cmd_char,
            'cmd_type': cmd_type,
            'runtime': runtime,
            'hvac': hvac,
            'hvdc': hvdc,
            'profile1': profile1,    # Wird zurückgegeben, aber ignoriert
            'profile2': profile2,    # Wird zurückgegeben, aber ignoriert
            'wait_acdc': wait_acdc   # Wird zurückgegeben, aber ignoriert
        }
    except Exception as e:
        logger.error("Error parsing TTCP_CMD: %s", e)
        return None



def build_ttcp_data() -> bytes:
    """
    Generates a formatted TTCP_DATA response for Delphi.
    Only includes NEW ADC data since the last transmission.
    Ensures correct conversion and prevents exceeding expected total.
    """
    global last_sent_index  # Use the global tracker
    
    try:
        # Get system status
        state_dict = system_state.get_status_dict()
        defined_time = state_dict["defined_measurement_time"]
        runtime = state_dict["runtime"]
        
        # Get the actual sample rate from profile1
        points_per_sec = state_dict["profile_hin"]
        if points_per_sec < 1 or points_per_sec > 100:
            points_per_sec = 20  # Default to 20 if value seems wrong
        
        # Calculate expected total samples based on Delphi's config
        expected_total = int(defined_time * points_per_sec)
        
        # Initialize buffer for 256 bytes
        data = bytearray(256)

        # Standard fields (1-42)
        struct.pack_into('<i', data, 0, 256)  # Size: 256 bytes
        struct.pack_into('<i', data, 4, runtime)  # Runtime
        
        # Temperature, etc.
        temp_int = int(state_dict["temperature"] * 100)
        struct.pack_into('<i', data, 8, temp_int)
        humidity_int = int(state_dict["humidity"] * 100)
        struct.pack_into('<i', data, 12, humidity_int)
        struct.pack_into('<i', data, 16, state_dict["hv_ac_value"])
        struct.pack_into('<i', data, 20, state_dict["hv_dc_value"])
        struct.pack_into('<i', data, 24, state_dict["profile_hin"])
        struct.pack_into('<i', data, 28, state_dict["profile_zurueck"])
        struct.pack_into('<i', data, 32, state_dict["wait_acdc"])
        struct.pack_into('<i', data, 36, Config.SERIAL_NUMBER)
        
        # Status bytes
        status1 = 0
        if state_dict["hv_status"]:
            status1 |= 0x01
        if state_dict["valve_status"] == ValveStatus.OPEN:
            status1 |= 0x02
        if state_dict["motor_position"] == MotorPosition.UP:
            status1 |= 0x04
        data[40] = status1

        status2 = 0
        if state_dict["hv_status"]:
            status2 |= 0x01
        if state_dict["sampling_active"]:
            status2 |= 0x02
        if state_dict["motor_position"] == MotorPosition.UP:
            status2 |= 0x04
        if state_dict["motor_position"] == MotorPosition.DOWN:
            status2 |= 0x08
        if state_dict["valve_status"] == ValveStatus.OPEN:
            status2 |= 0x10
        if state_dict["valve_status"] == ValveStatus.CLOSED:
            status2 |= 0x40
        if state_dict["measurement_active"]:
            status2 |= 0x80
        data[41] = status2

        # Get the real ADC data from buffer - BUT ONLY NEW SAMPLES!
        adc_data = []
        
        if not adc_buffer.is_empty():
            # Get all samples from buffer
            buffer_data = adc_buffer.get_all()
            total_samples = len(buffer_data)
            
            # CRITICAL: Reset last_sent_index if it exceeds the buffer size
            if last_sent_index >= total_samples:
                last_sent_index = 0
                logger.warning(f"last_sent_index {last_sent_index} >= total_samples {total_samples}, resetting to 0")
            
            # CRITICAL: Only get samples since last_sent_index
            if total_samples > last_sent_index:
                # We have new samples to send
                new_samples = buffer_data[last_sent_index:]
                logger.debug(f"Found {len(new_samples)} new samples since last transmission (index {last_sent_index})")
                
                # Update adc_data with only new samples
                adc_data = new_samples
            else:
                logger.debug(f"No new samples to send (current: {total_samples}, last sent: {last_sent_index})")
        
        # DataCount - number of values to send (max 50)
        data_count = min(len(adc_data), 50)
        
        # CRITICAL: Don't exceed expected_total with our last_sent_index + data_count
        if last_sent_index + data_count > expected_total and expected_total > 0:
            # Cap the data_count to avoid exceeding expected_total
            data_count = max(0, expected_total - last_sent_index)
            logger.info(f"Capped data_count to {data_count} to avoid exceeding expected total {expected_total}")
            
        data[42] = data_count
        
        # Dummy1 (not used)
        data[43] = 0

        # StartMeasureIndex - This is the total count of samples in the measurement
        if state_dict["measurement_active"] or runtime >= defined_time:
            # Use expected total during measurement, which is critical for Delphi timing
            struct.pack_into('<i', data, 44, expected_total)
            logger.debug(f"Setting start_index to {expected_total} for Delphi timing")
        else:
            # Not in measurement - just report current total
            current_total = len(adc_buffer.get_all())
            struct.pack_into('<i', data, 44, current_total)
            logger.debug(f"Setting start_index to current total: {current_total}")
        
        # Version and other fields
        data[48] = Config.FIRMWARE_VERSION_MINOR
        data[49] = Config.FIRMWARE_VERSION_MAJOR
        data[50] = 0
        struct.pack_into('<H', data, 51, 0)
        data[53] = state_dict["q628_state"].value
        data[54] = 0
        data[55] = 0

        # Pack the actual data values - ONLY NEW ONES
        # Keep track of what we're sending for debugging
        sent_values = []
        
        for i in range(50):
            if i < data_count:
                # Get the raw 22-bit value from buffer
                raw_22bit_value = adc_data[i]
                
                # IMPORTANT: Convert MCP3553 22-bit raw value to proper 32-bit signed int
                # The MCP3553 gives a 22-bit value in 2's complement format
                # When Delphi receives this value, it must be a proper 32-bit signed int
                
                # Method 1: Simple shift-based sign extension
                if raw_22bit_value & 0x200000:  # Check if bit 21 (sign bit) is set
                    # Negative value: Set all upper bits to 1 (sign extension)
                    value = raw_22bit_value | 0xFFC00000
                else:
                    # Positive value: Just use as is (upper bits are already 0)
                    value = raw_22bit_value
                
                sent_values.append((raw_22bit_value, value))
            else:
                # No more data to send in this position
                value = 0
                
            # Pack value into array (4 bytes per int)
            struct.pack_into('<i', data, 56 + i * 4, value)

        # Update the last sent index for next time - ONLY if we sent data
        if data_count > 0:
            # Update with actual count sent
            prev_last_sent = last_sent_index
            last_sent_index += data_count
            
            # CRITICAL: Ensure we never go beyond expected_total
            if expected_total > 0 and last_sent_index > expected_total:
                last_sent_index = expected_total
                logger.warning(f"Capped last_sent_index to expected_total ({expected_total})")
            
            # Only log at completion or major milestones, not for every packet
            if last_sent_index == expected_total:
                logger.info(f"📊 Completed sending all {expected_total} samples to Delphi client")
            elif last_sent_index % 1000 == 0:  # Only log every 1000 samples
                logger.info(f"📊 Progress: {last_sent_index}/{expected_total} samples sent ({(last_sent_index/expected_total*100):.1f}%)")
        else:
            # Don't log anything when no samples are sent
            pass

        # If measurement is complete, reset the tracker
        if (not state_dict["measurement_active"] and runtime >= defined_time) or last_sent_index >= expected_total:
            logger.info(f"Measurement complete - resetting last_sent_index from {last_sent_index} to 0")
            last_sent_index = 0

        return bytes(data)

    except Exception as e:
        logger.error(f"❌ Error creating TTCP_DATA: {e}", exc_info=True)
        return struct.pack('<I', 256) + bytes(252)

def handle_client(conn: socket.socket, addr: Tuple[str, int]) -> None:
    """
    Handle client connection with proper command processing and robust error handling.

    Args:
        conn: Connected socket object
        addr: Client address tuple (ip, port)
    """
    client_id = f"{addr[0]}:{addr[1]}"
    logger.info("🔌 Connected to %s", client_id)
    system_state.increment_clients()
    system_state.set("last_ip", addr[0])

    # Set TCP_NODELAY for this connection (critical for Delphi)
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception as e:
        logger.warning("⚠️ Could not set TCP_NODELAY for %s: %s", client_id, e)

    # Set a reasonable timeout
    conn.settimeout(0.05)

    # Track consecutive errors
    error_count = 0
    MAX_ERRORS = 5

    # Send immediate response on connection (required by Delphi)
    response = build_ttcp_data()
    try:
        conn.sendall(response)
        logger.info("📤 Sent initial data packet to %s (%d bytes)", client_id, len(response))
    except Exception as e:
        logger.error("❌ Error sending initial packet to %s: %s", client_id, e)
        conn.close()
        system_state.decrement_clients()
        return

    try:
        while not shutdown_requested.is_set():
            # Receive command with timeout
            try:
                cmd_data = conn.recv(Config.BUFFER_SIZE)
                if not cmd_data:
                    logger.info("👋 Client %s disconnected", client_id)
                    break

                # Reset error count on successful receive
                error_count = 0

            except socket.timeout:
                # Just continue the loop on timeout
                continue
            except ConnectionResetError:
                logger.warning("🔌 Connection reset by %s", client_id)
                break
            except Exception as e:
                error_count += 1
                logger.error("❌ Error receiving data from %s: %s (error #%d)",
                             client_id, e, error_count)

                if error_count >= MAX_ERRORS:
                    logger.error("⛔ Too many consecutive errors, closing connection to %s", client_id)
                    break

                # Short pause before retry
                time.sleep(0.5)
                continue

            # Log received command
            logger.info("Received from %s (%d bytes): %s",
                        client_id, len(cmd_data), cmd_data.hex())

            # Parse the command if it looks like a valid TTCP_CMD
            cmd_info = parse_ttcp_cmd(cmd_data) if len(cmd_data) >= 16 else None

            if cmd_info:
                logger.info(
                    f"✅ Executed command: {cmd_info['cmd']} - State after execution: {system_state.get_q628_state().name}")
                # Process command based on type
                # Inside parse_ttcp_cmd function:
                if cmd_info['cmd_type'] == CommandType.START:
                    logger.info(f"⚙️ Received configuration: Measurement time={cmd_info['runtime']}s, HVAC={cmd_info['hvac']}V, HVDC={cmd_info['hvdc']}V")
                    system_state.set("defined_measurement_time", cmd_info['runtime'])
                    # Set enable_from_pc flag for manual start mode
                    enable_from_pc.set()

                elif cmd_info['cmd_type'] == CommandType.TRIGGER:  # Simulate button press
                    logger.info("Received TRIGGER command (simulate button press)")
                    simulate_start.set()

                elif cmd_info['cmd_type'] == CommandType.GET_STATUS:  # Get status (polling)
                    logger.info("Received GET STATUS command")
                    # Just send current status (handled below)

                elif cmd_info['cmd_type'] == CommandType.MOTOR_UP:  # Move motor up
                    logger.info("Received MOTOR UP command")
                    move_motor(MotorPosition.UP)

                elif cmd_info['cmd_type'] == CommandType.MOTOR_DOWN:  # Move motor down
                    logger.info("Received MOTOR DOWN command")
                    move_motor(MotorPosition.DOWN)

                elif cmd_info['cmd_type'] == CommandType.RESET:  # Reset: disable HV, home motor
                    logger.info("Received RESET command")
                    # Clear state and reinitialize
                    system_state.end_measurement()
                    stop_sampling()
                    set_high_voltage(False)
                    set_valve(ValveStatus.CLOSED)
                    move_motor(MotorPosition.UP)

                else:
                    logger.warning("Unknown command: %s", cmd_info['cmd'])

            # Always send a response packet with current status
            # This is critical for Delphi compatibility
            try:
                response = build_ttcp_data()

                # Log detailed information about what we're sending
                state_dict = system_state.get_status_dict()
                runtime = state_dict["runtime"]

                # Create a summary of what we're sending
                data_summary = {
                    "size": 256,
                    "runtime": runtime,
                    "temperature": state_dict["temperature"],
                    "humidity": state_dict["humidity"],
                    "field_strength": state_dict["field_strength"],
                    "hv_ac": state_dict["hv_ac_value"],
                    "hv_dc": state_dict["hv_dc_value"],
                    "state": state_dict["q628_state"].name,
                    "measurement_active": state_dict["measurement_active"],
                    "sampling_active": state_dict["sampling_active"],
                    "datapoints": len(state_dict["adc_data"])
                }

                logger.debug(f"📤 TCP Response to Delphi: {data_summary}")

                conn.sendall(response)
                logger.debug("📤 Sent response to %s (%d bytes)", client_id, len(response))

                # Reset error count on successful send
                error_count = 0

            except socket.timeout:
                error_count += 1
                logger.warning("⏱️ Timeout sending response to %s (error #%d)",
                               client_id, error_count)
            except Exception as e:
                error_count += 1
                logger.error("❌ Error sending response to %s: %s (error #%d)",
                             client_id, e, error_count)

                if error_count >= MAX_ERRORS:
                    logger.error("⛔ Too many consecutive errors, closing connection to %s", client_id)
                    break

    except Exception as e:
        logger.error("❌ Unhandled error with client %s: %s", client_id, e, exc_info=True)
    finally:
        try:
            conn.close()
        except:
            pass
        system_state.decrement_clients()
        logger.info("👋 Connection to %s closed", client_id)


def free_port(port: int) -> None:
    """
    Kill any process that is using the specified port

    Args:
        port: TCP port number to free
    """
    try:
        output = subprocess.check_output(f"lsof -t -i:{port}", shell=True).decode()
        for pid in output.strip().split('\n'):
            if pid:  # Check if pid is not empty
                os.system(f"kill -9 {pid}")
        logger.info("🔪 Terminated previous processes on port %d", port)
    except subprocess.CalledProcessError:
        # This is expected if no process is using the port
        logger.info("✅ Port %d is free", port)
    except Exception as e:
        logger.warning("⚠️ Error freeing port %d: %s", port, e)

def ttcp_server(port: int) -> None:
    """
    TCP server with proper socket options and error handling

    Args:
        port: TCP port to listen on
    """
    logger.info("Starting TTCP server on port %d...", port)

    server = None

    while not shutdown_requested.is_set():
        try:
            # Free the port before binding
            free_port(port)

            # Create IPv4 socket
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Set TCP_NODELAY for the server socket
            server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            server.bind(('0.0.0.0', port))
            server.listen(5)

            logger.info("TTCP server running on port %d", port)

            # Accept connections in a loop
            while not shutdown_requested.is_set():
                try:
                    # Use a timeout to allow checking shutdown_requested
                    server.settimeout(1.0)
                    conn, addr = server.accept()

                    # Start a new thread to handle this client
                    client_thread = threading.Thread(
                        target=handle_client,
                        args=(conn, addr),
                        daemon=True
                    )
                    client_thread.start()

                except socket.timeout:
                    # This is expected due to the timeout we set
                    continue
                except Exception as e:
                    if not shutdown_requested.is_set():
                        logger.error("Error accepting connection: %s", e)
                        time.sleep(1)

            # Close the server socket when exiting the loop
            if server:
                server.close()
                server = None

        except Exception as e:
            logger.error("Critical error in TTCP server: %s", e, exc_info=True)

            # Close the server socket if it exists
            if server:
                try:
                    server.close()
                except:
                    pass
                server = None

            # Wait before retrying
            logger.info("Waiting 5 seconds before restarting server on port %d...", port)
            time.sleep(5)

    logger.info("TTCP server on port %d stopped", port)


def status_monitor() -> None:
    """Monitor and log system status periodically"""
    logger.info("📊 Status monitor started")

    while not shutdown_requested.is_set():
        try:
            state_dict = system_state.get_status_dict()
            motor_status = state_dict["motor_status"]
            q628_state = state_dict["q628_state"]

            status = (
                f"Status: HV={'ON' if state_dict['hv_status'] else 'OFF'}, "
                f"Valve={state_dict['valve_status']}, "
                f"Motor={state_dict['motor_position']}, "
                f"Motor Status={motor_status}, "
                f"Q628 State={q628_state.name}, "
                f"Cycles={state_dict['cycle_count']}, "
                f"Clients={state_dict['connected_clients']}, "
                f"Temp={state_dict['temperature']:.1f}°C, "
                f"Humid={state_dict['humidity']:.1f}%, "
                f"Field=0x{state_dict['field_strength']:06X}, "
                f"Measurement={'ACTIVE' if state_dict['measurement_active'] else 'INACTIVE'}, "
                f"Sampling={'ACTIVE' if state_dict['sampling_active'] else 'INACTIVE'}, "
                f"Runtime={state_dict['runtime']}s"
            )
            logger.info("📊 %s", status)

            # Check IP address periodically
            if state_dict["cycle_count"] % 5 == 0:
                get_current_ip()

        except Exception as e:
            logger.error("❌ Error in status monitor: %s", e)

        # Sleep for the configured interval, but check shutdown_requested more frequently
        for _ in range(Config.STATUS_UPDATE_INTERVAL):
            if shutdown_requested.is_set():
                break
            time.sleep(1)

    logger.info("📊 Status monitor terminated")


def cleanup() -> None:
    """Clean up hardware resources"""
    logger.info("🧹 Performing cleanup...")

    try:
        # Safety: ensure HV is off
        set_high_voltage(False)

        # Turn off LED
        GPIO.output(Config.START_LED_GPIO, GPIO.LOW)

        # Reset all outputs
        GPIO.output(Config.MAGNET1_GPIO, GPIO.LOW)
        GPIO.output(Config.MAGNET2_GPIO, GPIO.LOW)

        # Cleanup GPIO
        GPIO.cleanup()
        logger.info("✅ Hardware cleanup completed")
    except Exception as e:
        logger.error("❌ Error during cleanup: %s", e)
        

# ===== Main Application =====
def main() -> None:
    """Main application entry point"""
    # Setup logging first
    setup_logging()

    logger.info("Starting QUMAT628 control system...")

    # Initialize hardware
    if not initialize_hardware():
        logger.warning("Hardware initialization failed, continuing anyway...")

    # Get and log IP address
    get_current_ip()

    # Start main server on primary port
    main_server_thread = threading.Thread(
        target=ttcp_server,
        args=(Config.PRIMARY_PORT,),
        daemon=True
    )
    main_server_thread.start()
    logger.info("Main TCP server started on port %d", Config.PRIMARY_PORT)

    # Start compatibility server on secondary port
    compat_server_thread = threading.Thread(
        target=ttcp_server,
        args=(Config.COMPAT_PORT,),
        daemon=True
    )
    compat_server_thread.start()
    logger.info("Compatibility TCP server started on port %d", Config.COMPAT_PORT)

    # Start status monitor
    status_thread = threading.Thread(target=status_monitor, daemon=True)
    status_thread.start()
    logger.info("Status monitor started")

    try:
        # Main thread just waits for keyboard interrupt
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user (Ctrl+C)")
    finally:
        logger.info("Shutting down...")
        shutdown_requested.set()
        time.sleep(2)  # Give threads time to clean up
        cleanup()
        logger.info("Program terminated.")


if __name__ == "__main__":
    try:
        # Simple output at startup
        print("QUMAT628 Control System starting...")
        
        # Setup logging
        setup_logging()
        
        logger.info("QUMAT628 Control System started. Initializing system...")
        
        # Initialize hardware
        if not initialize_hardware():
            logger.error("Hardware initialization failed. Exiting program.")
            sys.exit(1)
        
        # Launch main function
        main()
    except Exception as e:
        logger.critical("Fatal error in main program: %s", e, exc_info=True)
        # Try to clean up GPIO before exiting
        try:
            GPIO.cleanup()
        except:
            pass
        sys.exit(1)
