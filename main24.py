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
import select  # Add this to the imports at the top of the file
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
    PREPARATION_TIME = 5         # Initial preparation time in seconds
    MOTOR_MOVEMENT_TIME = 3      # Time to wait for motor movement
    STATUS_UPDATE_INTERVAL = 30  # Status monitor update interval
    MOTOR_STATUS_POLL_INTERVAL = 0.5  # How often to poll motor status
    
    # ADC Timing Constants for MCP3553SN
    ADC_SAMPLE_RATE = 60.0    # MCP3553 delivers ~60 samples/second
    ADC_SAMPLE_INTERVAL = 1.0/ADC_SAMPLE_RATE  # ~16.67ms between samples
    
    # ===== NEW: Virtual Sampling Grid for Delphi Compatibility =====
    VIRTUAL_SAMPLE_RATE = 100.0  # Virtual 100Hz sample rate expected by Delphi
    VIRTUAL_SAMPLE_INTERVAL = 1.0/VIRTUAL_SAMPLE_RATE  # 10ms between virtual samples
    SAMPLES_PER_SECOND = 100     # Delphi expects 100 samples per second exactly
    TOTAL_EXPECTED_SAMPLES = 6000  # For 60s at 100Hz
    
    HYT_SAMPLE_INTERVAL = 1.0    # How often to measure temperature and humidity
    TEMP_HUMID_SAMPLE_INTERVAL = 1.0  # Interval for temperature and humidity measurement

    # File Paths
    LOG_FILE = "qumat628.log"
    
    # CSV Data Logging
    CSV_ENABLED = True
    CSV_BASE_FILENAME = "delphi_data"

    # Device Information
    SERIAL_NUMBER = 6280004
    FIRMWARE_VERSION_MAJOR = 6
    FIRMWARE_VERSION_MINOR = 2

    # ADC/DAC Configuration
    ADC_BUFFER_SIZE = 10000      # Size of ring buffer for ADC samples (increased to handle long measurements)
    VIRTUAL_BUFFER_SIZE = 6500   # Size of buffer for interpolated 100Hz virtual samples
    ADC_CLK_DELAY = 0.000002     # Clock delay for bit-banged SPI (2μs for ~250kHz)
    DAC_CLK_DELAY = 0.00001      # Clock delay for bit-banged SPI
    
    ADC_SAMPLE_RATE = 60.0       # MCP3553 delivers ~60 samples/second (from adc_gui.py)
    ADC_SAMPLE_INTERVAL = 1.0/ADC_SAMPLE_RATE  # ~16.67ms between samples
    ADC_CLK_DELAY = 0.000005     # 5μs (matches adc_gui.py)
    DAC_CLK_DELAY = 0.00001      # 10μs (matches dac_gui.py)
    FIXED_INITIALIZATION_TIME = 2 # Fixed initialization time for ADC/DAC
    
    # HV Control timing
    HV_OFF_DELAY_AFTER_DOWN = 4.0  # Delay in seconds before turning HV off after motor down


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
    """TCP command types with explicit mapping to ASCII values"""
    START = 'S'        # Enable measurement (requires button press) - ASCII 83
    TRIGGER = 'T'      # Simulate button press - ASCII 84
    GET_STATUS = 'G'   # Poll status - ASCII 71
    MOTOR_UP = 'U'     # Move motor up - ASCII 85
    MOTOR_DOWN = 'D'   # Move motor down - ASCII 68
    RESET = 'R'        # Reset system state - ASCII 82
    UNKNOWN = '?'      # Unknown command - ASCII 63
    
    @classmethod
    def from_int(cls, value: int) -> 'CommandType':
        """Create CommandType from integer ASCII value"""
        try:
            cmd_char = chr(value) if 32 <= value <= 126 else '?'
            return next((c for c in cls if c.value == cmd_char), cls.UNKNOWN)
        except:
            return cls.UNKNOWN


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
            "total_expected_samples": Config.TOTAL_EXPECTED_SAMPLES,  # 6000 for 60s at 100Hz
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
            expected_samples = int(defined_time * Config.VIRTUAL_SAMPLE_RATE)
            current_samples = len(virtual_buffer.get_all())
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
            
            # Get current sample count from virtual buffer
            current_count = len(virtual_buffer.get_all())
            
            # If we've reached or exceeded the expected count, stop the measurement
            if current_count >= expected_count:
                logger.info(f"⚠️ Reached exact virtual sample count limit: {current_count}/{expected_count}")
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
            self._state["total_expected_samples"] = int(defined_time * Config.VIRTUAL_SAMPLE_RATE)
            
            # Clear buffers at start of measurement
            adc_buffer.clear()
            virtual_buffer.clear()
            
            # Reset sent packet counter for CSV logging
            global sent_packet_counter
            sent_packet_counter = 0
            
            # Create a new CSV file for this measurement
            if Config.CSV_ENABLED:
                create_new_csv_file()
            
            # Start runtime update timer
            self._start_runtime_timer()
            
            logger.info(f"🕒 Measurement started at: {self._state['measurement_start_time']} " +
                      f"with target of exactly {self._state['total_expected_samples']} virtual samples " +
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
                        expected_samples = int(defined_time * Config.VIRTUAL_SAMPLE_RATE)
                        current_samples = len(virtual_buffer.get_all())
                        
                        if current_samples >= expected_samples and self._state["q628_state"] == Q628State.ACTIV:
                            logger.info(f"⏱️ Runtime timer detected exact virtual sample count reached ({current_samples}/{expected_samples})")
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
adc_buffer = RingBuffer(Config.ADC_BUFFER_SIZE)  # Larger buffer for raw ADC samples
virtual_buffer = RingBuffer(Config.VIRTUAL_BUFFER_SIZE)  # Buffer for 100Hz virtual samples
temp_buffer_lock = threading.Lock()  # Lock for temp_buffer access
temp_buffer = {"latest_value": 0, "timestamp": 0.0}  # Temporary buffer for latest ADC value

# For interpolation
sample_lock = threading.Lock()
last_two_samples = []  # For storing the last two real ADC samples with timestamps

# 9-bit shift equivalent for improved visualization
SCALE_FACTOR = 16

# Add this to your global variables or system_state initialization
last_sent_index = 0  # Global tracker for last sent buffer position
sent_packet_counter = 0  # Counter for packets sent to Delphi
csv_file_path = None  # Current CSV file path

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

def setup_logging() -> None:
    """Configure logging to both file and console with proper formatting"""
    logger = logging.getLogger("qumat628")
    
    # Clear any existing handlers to prevent duplicates
    if logger.handlers:
        logger.handlers.clear()
        
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

    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger

# ===== CSV Logging Functions =====
def create_new_csv_file() -> str:
    """
    Create a new CSV file with timestamp for logging Delphi data.
    
    Returns:
        Path to the created CSV file
    """
    global csv_file_path
    
    try:
        # Create filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{Config.CSV_BASE_FILENAME}_{timestamp}.csv"
        
        # Create CSV with headers
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'Timestamp', 'DateTime', 'PacketID', 
                'SampleIndex', 'RawValue', 'SentValue', 'Voltage',
                'ScaledValue',                        # NEW: Add scaled value column
                'VirtualIndex', 'VirtualTime',        # Virtual sample metadata
                'Runtime', 'HV_Status', 'HV_DC', 'HV_AC', 'DAC_Voltage',
                'Temperature', 'Humidity', 'MeasurementActive',
                'Q628State', 'MotorPosition', 'ValveStatus',
                'Interpolated', 'ScalingFactor'       # NEW: Add scaling factor column
            ])
        
        logger.info(f"📊 Created new CSV log file: {filename}")
        csv_file_path = filename
        return filename
        
    except Exception as e:
        logger.error(f"❌ Error creating CSV file: {e}")
        return None

def log_samples_to_csv(samples: list, packet_id: int, start_index: int) -> None:
    """
    Log samples sent to Delphi to a CSV file.
    
    Args:
        samples: List of tuples (raw_value, sent_value, is_interpolated, virtual_index)
        packet_id: Unique ID for this packet
        start_index: Starting index in the measurement
    """
    global csv_file_path
    
    if not Config.CSV_ENABLED or not samples:
        return
        
    if csv_file_path is None:
        csv_file_path = create_new_csv_file()
        if csv_file_path is None:
            return
    
    try:
        state_dict = system_state.get_status_dict()
        current_time = time.time()
        dt_str = datetime.datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Calculate DAC voltage from HV DC value
        dac_voltage = state_dict["hv_dc_value"] / 2000.0
        
        with open(csv_file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            for i, (raw_value, sent_value, is_interpolated, virtual_idx) in enumerate(samples):
                # Calculate voltage from raw ADC value
                voltage = (float(raw_value) / ((1 << 21) - 1)) * Config.VREF_ADC * 2
                
                # Calculate absolute sample index
                abs_index = start_index + i
                
                # Calculate virtual time (in 100Hz grid)
                virtual_time = virtual_idx / 100.0
                
                # Calculate scaled value for comparison with sent value
                scaled_value = raw_value * SCALE_FACTOR
                
                # Write complete row with all context data
                writer.writerow([
                    f"{current_time:.6f}",                 # Timestamp
                    dt_str,                                # Human-readable datetime
                    packet_id,                             # Packet ID
                    abs_index,                             # Sample index in measurement
                    raw_value,                             # Raw 22-bit ADC value
                    sent_value,                            # Value sent to Delphi (scaled)
                    f"{voltage:.6f}",                      # Calculated voltage
                    scaled_value,                          # NEW: Scaled value for debugging
                    virtual_idx,                           # Virtual sample index (100Hz grid)
                    f"{virtual_time:.3f}",                 # Virtual time in seconds (100Hz grid)
                    state_dict["runtime"],                 # Runtime in seconds
                    "ON" if state_dict["hv_status"] else "OFF",  # HV status
                    state_dict["hv_dc_value"],             # HV DC value
                    state_dict["hv_ac_value"],             # HV AC value
                    f"{dac_voltage:.6f}",                  # DAC voltage
                    f"{state_dict['temperature']:.2f}",    # Temperature
                    f"{state_dict['humidity']:.2f}",       # Humidity
                    "YES" if state_dict["measurement_active"] else "NO",  # Measurement active
                    state_dict["q628_state"].name,         # Q628 state
                    str(state_dict["motor_position"]),     # Motor position
                    str(state_dict["valve_status"]),       # Valve status
                    "YES" if is_interpolated else "NO",    # Interpolated flag
                    f"{SCALE_FACTOR}",                     # NEW: Scaling factor used
                ])
                
        # Log status periodically to avoid flooding logs
        if packet_id % 10 == 0:
            logger.debug(f"📊 Logged {len(samples)} samples to CSV (packet {packet_id}, indices {start_index}-{start_index+len(samples)-1})")
            
    except Exception as e:
        logger.error(f"❌ Error logging samples to CSV: {e}")

        
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

        # Initial state for DAC - ensure it's set to 0V
        GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Chip select inactive
        GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
        GPIO.output(Config.PIN_DAT_DAC, GPIO.LOW)
        logger.info("✅ DAC pins initialized")
        
        # Initialize DAC to 0V to ensure HV is off at startup
        try:
            set_dac_voltage(0.0)
            logger.info("✅ DAC initialized to 0V")
        except Exception as e:
            logger.error(f"❌ DAC initialization failed: {e}")
            # Continue anyway

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

  
        # Start continuous ADC sampling thread at fixed 16.67Hz
        threading.Thread(
            target=continuous_adc_sampling_thread,
            daemon=True
        ).start()
        logger.info("✅ Continuous 16.67Hz ADC sampling started")

        # Start virtual interpolation thread to create 100Hz timeline
        threading.Thread(
            target=virtual_sampling_thread,
            daemon=True
        ).start()
        logger.info("✅ Virtual 100Hz interpolation thread started")

        # Start ADC monitoring thread (handles state transitions, not sampling)
        threading.Thread(
            target=adc_sampling_thread,
            daemon=True
        ).start()
        logger.info("✅ ADC state monitoring thread started")

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

# Key changes to make:
# 1. Remove the virtual_sampling_thread that performs interpolation
# 2. Continue sampling at natural 60 Hz rate
# 3. Map the real 60 Hz time axis to a virtual 100 Hz grid for Delphi without interpolation
# 4. Ensure that runtime calculation matches what Delphi expects
def continuous_adc_sampling_thread() -> None:
    """
    Thread for continuous ADC sampling at precise 16.67Hz (60ms).
    Optimized for maximum real-world performance.
    """
    logger.info("📊 Continuous ADC sampling started at 16.67Hz target")
    sample_count = 0
    
    # Perform initial soft reset
    GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
    time.sleep(0.01) 
    GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
    time.sleep(0.02)
    
    # Use monotonic time for precise timing
    next_sample_time = time.monotonic()
    start_time = next_sample_time
    TARGET_RATE = 16.67  # Target Hz
    ACTUAL_SAMPLE_INTERVAL = 1.0 / TARGET_RATE  # ~60ms
    
    # Timing diagnostics
    read_times = []
    
    # Runtime tracking
    last_stats_time = time.monotonic()
    
    while not shutdown_requested.is_set():
        try:
            # Measure how long the ADC read takes
            read_start = time.monotonic()
            
            # Read ADC at precise intervals
            raw_value = read_adc_22bit()
            current_time = time.monotonic()
            
            # Track read time for performance tuning
            read_time = current_time - read_start
            read_times.append(read_time)
            if len(read_times) > 100:
                read_times.pop(0)
            
            if raw_value != 0:
                # Store raw value with timestamp
                with sample_lock:
                    # Store current sample with timestamp
                    if len(last_two_samples) >= 2:
                        last_two_samples.pop(0)  # Remove oldest
                    last_two_samples.append((current_time, raw_value))
                
                # Store state values
                system_state.set("field_strength", raw_value)
                system_state.set("last_adc_value", raw_value)
                
                # Calculate voltage
                vref = Config.VREF_ADC
                voltage = (float(raw_value) / ((1 << 21) - 1)) * vref * 2
                
                # Store in buffer
                adc_buffer.enqueue(raw_value)
                system_state.add_adc_data(raw_value)
                
                # Update sample count for logging
                sample_count += 1
            
            # Show sampling statistics every 5 seconds
            if current_time - last_stats_time >= 5.0:
                elapsed = current_time - start_time
                actual_rate = sample_count / elapsed if elapsed > 0 else 0
                avg_read_time = sum(read_times) / len(read_times) if read_times else 0
                
                logger.info(f"📊 ADC sampling stats: {sample_count} samples at {actual_rate:.2f} Hz " +
                           f"(target: {TARGET_RATE:.2f} Hz), avg read time: {avg_read_time*1000:.2f}ms")
                
                # Adjust timing if we're falling below 15 Hz
                if actual_rate < 15.0 and avg_read_time > 0:
                    # Reduce the delay proportionally
                    adjustment = min(0.01, avg_read_time * 0.1)  # Max 10ms adjustment
                    ACTUAL_SAMPLE_INTERVAL = max(0.01, ACTUAL_SAMPLE_INTERVAL - adjustment)
                    logger.warning(f"⚠️ ADC rate too low - adjusting interval to {ACTUAL_SAMPLE_INTERVAL*1000:.2f}ms " +
                                   f"to compensate for {avg_read_time*1000:.2f}ms read time")
                
                last_stats_time = current_time
            
            # Schedule next sample with precise timing
            next_sample_time += ACTUAL_SAMPLE_INTERVAL
            sleep_time = next_sample_time - time.monotonic()
            
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # We're falling behind schedule
                if sleep_time < -0.05:  # Only log if we're significantly behind
                    logger.warning(f"⚠️ ADC sampling falling behind by {-sleep_time:.3f}s - adjusting")
                    # Reset timing if we fall too far behind
                    next_sample_time = time.monotonic() + ACTUAL_SAMPLE_INTERVAL
                
        except Exception as e:
            logger.error(f"❌ Error in continuous ADC sampling: {e}")
            time.sleep(0.1)  # Short pause on errors

def interpolate_value(time_point: float) -> Tuple[int, bool]:
    """
    Interpolate a value at the given time point using the last two real samples.
    If no samples are available, creates reasonable estimates.
    
    Args:
        time_point: Time point to interpolate at
    
    Returns:
        Tuple of (interpolated value, is_interpolated flag)
    """
    with sample_lock:
        # If we don't have two samples yet, return the latest or zero
        if len(last_two_samples) == 0:
            return 0, True  # No samples yet, return zero with interpolated flag
        
        if len(last_two_samples) == 1:
            return last_two_samples[0][1], False  # Return the only sample (not interpolated)
        
        # Get the two samples with timestamps
        (t1, v1), (t2, v2) = last_two_samples
        
        # If the time point is outside the range of our samples
        if time_point <= t1:
            return v1, False  # Return the earlier sample (not interpolated)
        if time_point >= t2:
            return v2, False  # Return the later sample (not interpolated)
        
        # Calculate linear interpolation
        # y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
        ratio = (time_point - t1) / (t2 - t1)
        interpolated = v1 + int(ratio * (v2 - v1))
        
        return interpolated, True  # Return interpolated value with flag

def virtual_sampling_thread() -> None:
    """
    Generate virtual 100Hz sampling grid by interpolating between real samples.
    Optimized to maintain real-time performance by processing in batches.
    """
    logger.info("📊 Virtual 100Hz sampling interpolation started")
    
    # Initialize timing variables
    virtual_sample_count = 0
    virtual_start_time = None
    last_log_time = 0
    last_batch_time = 0
    
    # Virtual sample interval = 10ms (100Hz)
    VIRTUAL_SAMPLE_INTERVAL = 0.01
    BATCH_INTERVAL = 0.2  # Process in 200ms batches (20 virtual samples)
    
    while not shutdown_requested.is_set():
        try:
            current_time = time.monotonic()
            
            # Only process during active measurement
            if sampling_active.is_set() and system_state.is_sampling_active():
                # Initialize start time if needed
                if virtual_start_time is None:
                    virtual_start_time = current_time
                    last_batch_time = current_time
                    logger.info(f"📊 Virtual sampling grid initialized at {virtual_start_time}")
                
                # Check if it's time to process a batch of virtual samples
                if current_time - last_batch_time >= BATCH_INTERVAL:
                    # Calculate virtual sample times for this batch
                    elapsed = current_time - virtual_start_time
                    
                    # How many virtual samples should we have by now?
                    target_count = int(elapsed / VIRTUAL_SAMPLE_INTERVAL)
                    
                    # Calculate how many samples to generate in this batch
                    batch_start = virtual_sample_count
                    batch_end = min(target_count, batch_start + 50)  # Limit batch size
                    
                    # Generate the batch of virtual samples
                    for i in range(batch_start, batch_end):
                        # Calculate exact virtual time point
                        virtual_time = virtual_start_time + (i * VIRTUAL_SAMPLE_INTERVAL)
                        
                        # Get interpolated value at this exact time
                        value, is_interpolated = interpolate_value(virtual_time)
                        
                        # Add to virtual buffer with metadata
                        virtual_buffer.enqueue((value, is_interpolated, i))
                    
                    # Update count and batch time
                    if batch_end > virtual_sample_count:
                        virtual_sample_count = batch_end
                        last_batch_time = current_time
                    
                    # Log statistics periodically (every second)
                    if current_time - last_log_time > 1.0:
                        logger.debug(f"📊 Generated {virtual_sample_count} virtual samples at 100Hz grid " +
                                   f"({virtual_sample_count/elapsed if elapsed > 0 else 0:.1f} Hz effective)")
                        last_log_time = current_time
                
                # Check if we've reached exact sample count limit
                defined_time = system_state.get("defined_measurement_time")
                expected_samples = int(defined_time * 100)
                
                if virtual_sample_count >= expected_samples:
                    logger.info(f"📊 Reached exact virtual sample count: {virtual_sample_count}/{expected_samples}")
                    if system_state.get_q628_state() == Q628State.ACTIV:
                        system_state.enforce_sample_count_limit()
            else:
                # Reset timing if sampling not active
                virtual_start_time = None
                virtual_sample_count = 0
                
            # Sleep briefly to prevent CPU hogging
            time.sleep(0.005)
            
        except Exception as e:
            logger.error(f"❌ Error in virtual sampling: {e}")
            time.sleep(0.1)

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
    
    When enabled, sets the DAC voltage according to the 1:2000 ratio
    for the configured HV DC value.
    """
    status_text = 'ON' if enabled else 'OFF'
    logger.info("⚡ High voltage: %s", status_text)

    try:
        system_state.set("hv_status", enabled)
        
        if enabled:
            # Get configured HV DC value
            hv_dc = system_state.get("hv_dc_value")
            
            # Calculate DAC voltage using the 1:2000 ratio (6500V = 3.25V DAC)
            dac_voltage = hv_dc / 2000.0
            
            # Apply the voltage to the DAC
            set_dac_voltage(dac_voltage)
            
            # Then activate the HV AC/DC GPIO
            GPIO.output(Config.HVAC_GPIO, GPIO.HIGH)
            
            # Log the values being applied
            hv_ac = system_state.get("hv_ac_value")
            logger.info(f"⚡ Applied voltage values - AC:{hv_ac}V, DC:{hv_dc}V (DAC={dac_voltage:.4f}V)")
        else:
            # Turn off the main HV AC/DC GPIO
            GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
            
            # Set DAC to zero when turning off HV
            set_dac_voltage(0.0)
            logger.info("⚡ High voltage turned OFF, DAC set to 0V")
        
    except Exception as e:
        logger.error("❌ Error setting high voltage: %s", e)
        system_state.set("last_error", f"HV error: {str(e)}")


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
    Read 22-bit raw value from MCP3553SN ADC using simplified, reliable implementation.
    Maintains 16.67 Hz sampling rate with reset after each sample.
    
    Returns:
        Raw signed integer ADC value
    """
    with adc_lock:
        try:
            # Wait for SDO to go LOW (indicates data ready)
            ready = False
            timeout_start = time.time()
            timeout_limit = 0.05  # Short timeout for more responsive behavior
            
            while time.time() - timeout_start < timeout_limit:
                if GPIO.input(Config.PIN_SDO_ADC) == GPIO.LOW:
                    ready = True
                    break
                time.sleep(0.0005)  # Check frequently
            
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
                bit = GPIO.input(Config.PIN_SDO_ADC)
                value = (value << 1) | bit
                GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
                time.sleep(5e-6)  # 5μs delay
            
            # Extract overflow flags and actual 22-bit value
            ovh = (value >> 23) & 0x01
            ovl = (value >> 22) & 0x01
            signed_value = value & 0x3FFFFF
            
            # Handle two's complement for negative values
            if signed_value & (1 << 21):
                signed_value -= (1 << 22)
            
            # CRITICAL: Reset AFTER each sample as per the working example
            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
            time.sleep(0.01)
            GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
            
            return signed_value
            
        except Exception as e:
            logger.error(f"❌ MCP3553 SPI read error: {e}")
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
    raw_value = read_adc_22bit()
    # Calculate voltage
    voltage = (float(raw_value) / ((1 << 21) - 1)) * Config.VREF_ADC * 2
    return voltage, raw_value


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


def set_dac_voltage_for_hv_dc(hv_dc_value: int) -> None:
    """
    Sets the DAC output voltage to achieve the desired HV DC value.
    
    Follows the 1:2000 ratio where 3.25V DAC output = 6500V HV DC.
    
    Args:
        hv_dc_value: Desired high voltage DC value in volts
    """
    # Calculate required DAC voltage using the 1:2000 ratio (6500V HV = 3.25V DAC)
    dac_voltage = hv_dc_value / 2000.0  # This is the exact 1:2000 ratio
    
    # Log the conversion
    logger.info(f"⚡ Setting HV DC to {hv_dc_value}V (DAC output: {dac_voltage:.4f}V)")
    
    # Apply the voltage to the DAC
    set_dac_voltage(dac_voltage)
    
    # Store the set value in system state
    system_state.set("hv_dc_value", hv_dc_value)


def adc_sampling_thread() -> None:
    """
    Legacy ADC sampling thread - now just monitors sampling status.
    Actual sampling happens in continuous_adc_sampling_thread and virtual_sampling_thread.
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
                        samples = len(virtual_buffer.get_all())
                        expected = int(defined_time * Config.VIRTUAL_SAMPLE_RATE)
                        logger.info(f"Measurement complete with {samples}/{expected} virtual samples at 100Hz")
                        
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


def start_sampling() -> None:
    """
    Start official measurement sampling.
    This activates ADC sampling and resets data tracking.
    """
    global last_sent_index
    
    logger.info("📊 Starting official measurement")
    
    # Clear buffer for a fresh start
    adc_buffer.clear()
    system_state.clear_adc_data()
    
    # Reset transmission tracking
    last_sent_index = 0
    
    # Mark the sampling as active for the state machine
    system_state.start_sampling()
    sampling_active.set()


# Create a thread-safe flag to coordinate measurement ending
measurement_ending_lock = threading.Lock()
measurement_already_ending = False

def stop_sampling() -> None:
    """
    Stop ADC sampling with coordinated shutdown.
    IMPORTANT: This does not clear the buffer to ensure all data is transmitted.
    """
    global measurement_already_ending
    
    with measurement_ending_lock:
        # Check if measurement is already being stopped by another thread
        if measurement_already_ending:
            logger.debug("📊 Sampling stop already in progress - skipping redundant call")
            return
            
        # Mark measurement as ending
        measurement_already_ending = True
    
    logger.info("📊 Stopping ADC sampling - ensuring data will still be transmitted") 
    sampling_active.clear()
    system_state.stop_sampling()  # Clear sampling active flag in system state
    
    # Log buffer statistics
    defined_time = system_state.get("defined_measurement_time")
    expected_virtual_samples = int(defined_time * 100)  # 100Hz expected by Delphi
    current_samples = len(adc_buffer.get_all())
    
    logger.info(f"📊 Sampling stopped with {current_samples} real samples in buffer")
    logger.info(f"📊 {last_sent_index} samples sent to client so far")
    logger.info(f"📊 {current_samples - last_sent_index} samples remaining to be sent")
    
    # IMPORTANT: Do NOT clear the buffer here, as we still need to send remaining data
    
    # Reset flag after a short delay to allow for proper cleanup
    def reset_flag():
        global measurement_already_ending
        time.sleep(1.0)  # Wait for other threads to notice sampling is inactive
        with measurement_ending_lock:
            measurement_already_ending = False
    
    threading.Thread(target=reset_flag, daemon=True).start()


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

def q628_state_machine() -> None:
    """
    Main Q628 state machine - Implements the final version with special timing rules:
    - HV off exactly after configured delay after motor down
    - Valves opened whenever motor moves
    - 3.5s delay after motor positioning before closing valves
    - Reports sample statistics at end of measurement
    - Fixed to ensure all data is sent after measurement completion
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
                    if elapsed > 5.5:  # 3.0 + 2.5 seconds
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

                        # Reset the last_sent_index for new measurement
                        global last_sent_index
                        last_sent_index = 0

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

                        # Reset the last_sent_index for new measurement
                        last_sent_index = 0

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
                    # MODIFIED: Turn HV off exactly after configured delay since motor reached bottom
                    if motor_down_time is not None:
                        time_since_motor_down = time.time() - motor_down_time
                        
                        # Using the configured delay from Config
                        hv_off_delay = Config.HV_OFF_DELAY_AFTER_DOWN
                        
                        if time_since_motor_down >= hv_off_delay:
                            logger.info(f"⚡ Exactly {hv_off_delay}s after motor down - turning OFF High Voltage")
                            set_high_voltage(False)
                            
                            # Transition to ACTIV state
                            system_state.set_q628_state(Q628State.ACTIV)
                            logger.info("📊 Active measurement phase started")
                        else:
                            # Not yet reached the configured delay - calculate remaining wait time
                            remaining = hv_off_delay - time_since_motor_down
                            logger.debug(f"⏱️ Waiting {remaining:.3f}s more before turning off HV")
                            # Stay in this state until exactly the configured time has passed
                    else:
                        logger.warning("⚠️ Motor down time not recorded, using elapsed time")
                        if elapsed > Config.HV_OFF_DELAY_AFTER_DOWN:
                            logger.info(f"⚡ Turning OFF High Voltage after {Config.HV_OFF_DELAY_AFTER_DOWN}s (using elapsed time fallback)")
                            set_high_voltage(False)
                            
                            # Transition to ACTIV
                            system_state.set_q628_state(Q628State.ACTIV)
                            logger.info("📊 Active measurement phase started")

               # In the ACTIV state section:
            elif current_state == Q628State.ACTIV:
                # Get precise timing information
                button_press_time = system_state.get("button_press_time")
                defined_time = system_state.get("defined_measurement_time")
                
                if button_press_time is not None:
                    elapsed_since_start = time.time() - button_press_time
                    
                    # Update sample count during measurement
                    virtual_samples = len(virtual_buffer.get_all())
                    real_samples = len(adc_buffer.get_all())
                    expected_virtual = int(defined_time * 100)
                    expected_real = int(defined_time * 16.67)  # Our real rate is 16.67Hz
                    
                    # Only log when count increases by 100+ samples
                    if virtual_samples - measurement_samples >= 100:
                        logger.debug(f"📊 Collected {real_samples} real samples and {virtual_samples} virtual samples " +
                                f"({real_samples/(expected_real)*100:.1f}% real, {virtual_samples/(expected_virtual)*100:.1f}% virtual)")
                        measurement_samples = virtual_samples
                    
                    # CRITICAL: Enforce exact measurement time
                    if elapsed_since_start >= defined_time:
                        logger.info(f"⏱️ Exact measurement time reached: {elapsed_since_start:.3f}s / {defined_time}s")
                        logger.info(f"📊 Final sample count: {real_samples} real samples, {virtual_samples} virtual samples")
                        logger.info(f"📊 Expected sample count: {expected_real} real samples, {expected_virtual} virtual samples")
                        logger.info(f"📊 Sample rates: {real_samples/elapsed_since_start:.2f} Hz real, {virtual_samples/elapsed_since_start:.2f} Hz virtual")
                        
                        # Stop sampling
                        stop_sampling()
                        
                        # Move to FINISH state
                        system_state.set_q628_state(Q628State.FINISH)
                    
                    # CRITICAL: Also enforce exact virtual sample count if reached
                    if virtual_samples >= expected_virtual:
                        logger.info(f"⏱️ Exact virtual sample count reached: {virtual_samples}/{expected_virtual}")
                        
                        # Stop sampling
                        stop_sampling()
                        
                        # Move to FINISH state
                        system_state.set_q628_state(Q628State.FINISH)
                
                # Short sleep for precise timing
                time.sleep(0.001)  # Responsive during active measurement

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
                total_real_samples = len(adc_buffer.get_all())
                defined_time = system_state.get("defined_measurement_time")
                expected_real_samples = int(defined_time * Config.ADC_SAMPLE_RATE)  # ~60Hz
                expected_virtual_samples = int(defined_time * 100)  # 100Hz expected by Delphi
                
                # CRITICAL: Do NOT reset last_sent_index here
                # Keep tracking position to ensure all data is sent
                
                # Provide comprehensive measurement summary at the end
                logger.info("=" * 80)
                logger.info(f"📊 MEASUREMENT COMPLETE - Sample Statistics:")
                logger.info(f"📊 Total real ADC samples: {total_real_samples}")
                logger.info(f"📊 Real sample rate: {total_real_samples/defined_time:.2f} Hz")
                logger.info(f"📊 Expected virtual samples (100Hz): {expected_virtual_samples}")
                logger.info(f"📊 Samples sent to client: {last_sent_index}")
                logger.info(f"📊 Measurement duration: {defined_time}s")
                logger.info(f"📊 Remaining samples to send: {max(0, total_real_samples - last_sent_index)}")
                
                # Log warning if we have significantly fewer samples than expected
                if total_real_samples < expected_real_samples * 0.9:
                    logger.warning(f"⚠️ Collected fewer real samples than expected ({total_real_samples} vs ~{expected_real_samples})")
                logger.info("=" * 80)

                # End measurement but continue sending data
                # CRITICAL: Set end_measurement but don't clear the sample buffer
                system_state.end_measurement()
                
                # CRITICAL: Don't clear adc_buffer yet - the data transfer is still in progress
                # Data transfer completion is handled in the handle_client function

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


def parse_ttcp_cmd(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse Delphi-compatible TTCP_CMD packet with improved debugging
    """
    try:
        # Show raw command bytes for debugging
        logger.info(f"Raw command data: {data.hex()}")
        
        if len(data) < 32:  # Delphi sends 8 x 4-byte ints = 32 bytes
            logger.error(f"Invalid TTCP_CMD size: {len(data)} bytes, expected 32+")
            return None

        # Unpack 8 integers from the data
        size, cmd_int, runtime, hvac, hvdc, profile1, profile2, wait_acdc = struct.unpack("<8i", data[:32])
        
        # Debug log the integer values
        logger.info(f"Unpacked command: size={size}, cmd_int={cmd_int}, runtime={runtime}, " +
                   f"hvac={hvac}, hvdc={hvdc}, profile1={profile1}, profile2={profile2}, wait_acdc={wait_acdc}")

        # Convert command integer to character
        # This is critical - cmd_int is the ASCII value of the command character
        cmd_char = chr(cmd_int) if 32 <= cmd_int <= 126 else '?'
        logger.info(f"Command character from ASCII {cmd_int}: '{cmd_char}'")
        
        # Map to command type
        cmd_type = next((c for c in CommandType if c.value == cmd_char), CommandType.UNKNOWN)
        logger.info(f"Mapped to command type: {cmd_type.name}")

        # GEÄNDERT: Logge, dass Profile ignoriert werden
        if profile1 != 0 or profile2 != 0 or wait_acdc != 0:
            logger.info("⚠️ Profile settings received but will be IGNORED (profile1=%d, profile2=%d, wait_acdc=%d)",
                    profile1, profile2, wait_acdc)

        # Calculate DAC voltage for HV DC value using 1:2000 ratio
        dac_voltage = hvdc / 2000.0
        logger.info("Parsed TTCP_CMD: %s (runtime=%ds, HVAC=%d, HVDC=%d → DAC=%fV)",
                    cmd_char, runtime, hvac, hvdc, dac_voltage)

        # Update system state with received parameters
        system_state.update(
            profile_hin=profile1,
            profile_zurueck=profile2,
            wait_acdc=wait_acdc,
            hv_ac_value=hvac,
            hv_dc_value=hvdc
        )

        # When receiving START command, set the defined measurement time
        if cmd_type == CommandType.START:
            logger.info(f"🔄 START COMMAND RECOGNIZED!")
            system_state.update(defined_measurement_time=runtime)
            logger.info(f"📏 Set defined measurement time to {runtime} seconds")
            logger.info(f"⚡ Set HV DC value to {hvdc}V (DAC voltage will be {dac_voltage:.4f}V)")

        return {
            'size': size,
            'cmd': cmd_char,
            'cmd_type': cmd_type,
            'runtime': runtime,
            'hvac': hvac,
            'hvdc': hvdc,
            'profile1': profile1,
            'profile2': profile2,
            'wait_acdc': wait_acdc
        }
    except Exception as e:
        logger.error(f"Error parsing TTCP_CMD: {e}", exc_info=True)
        return None
    


def build_ttcp_data() -> bytes:
    """
    Generates a formatted TTCP_DATA response for Delphi using interpolated virtual samples.
    Also handles IDLE state properly by sending last known values.
    Values are scaled by SCALE_FACTOR to improve visual deflection in Delphi.
    """
    global last_sent_index, sent_packet_counter
    
    try:
        # Increment packet counter
        sent_packet_counter += 1
        
        # Get system status
        state_dict = system_state.get_status_dict()
        defined_time = state_dict["defined_measurement_time"]
        runtime = state_dict["runtime"]
        current_state = state_dict["q628_state"]
        
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

        # Check state and handle differently
        if current_state == Q628State.IDLE and not state_dict["measurement_active"]:
            # In IDLE state, send a small number of the latest samples (up to 5)
            # This ensures Delphi always has some data to display
            latest_samples = state_dict["adc_data"]
            data_count = min(len(latest_samples), 5)
            
            # Set DataCount
            data[42] = data_count
            
            # Dummy1 (not used)
            data[43] = 0
            
            # StartMeasureIndex - For IDLE, just use 0
            struct.pack_into('<i', data, 44, 0)
            
            # Version and other fields
            data[48] = Config.FIRMWARE_VERSION_MINOR
            data[49] = Config.FIRMWARE_VERSION_MAJOR
            data[50] = 0
            struct.pack_into('<H', data, 51, 0)
            data[53] = current_state.value
            data[54] = 0
            data[55] = 0
            
            # Pack the actual data values (latest samples)
            sent_values = []
            
            for i in range(50):
                if i < data_count:
                    raw_value = latest_samples[len(latest_samples) - data_count + i]
                    
                    # Apply scaling factor to improve visualization
                    scaled_value = raw_value * SCALE_FACTOR
                    
                    # Sign-extend if needed
                    if scaled_value & 0x80000000:  # Check sign bit
                        delphi_value = scaled_value
                    else:
                        delphi_value = scaled_value
                        
                    # Add to tracking for CSV logging
                    sent_values.append((raw_value, delphi_value, False, i))
                else:
                    delphi_value = 0
                
                # Pack value into array (4 bytes per int)
                struct.pack_into('<i', data, 56 + i * 4, delphi_value)
                
            # Log IDLE values to CSV if needed
            if Config.CSV_ENABLED and data_count > 0:
                log_samples_to_csv(sent_values, sent_packet_counter, 0)
                
            logger.debug(f"📤 IDLE state: Sending {data_count} latest samples (scaled ×{SCALE_FACTOR}) to maintain display")
            
        else:
            # During or after measurement - send virtual samples
            virtual_samples = virtual_buffer.get_all()
            total_virtual_samples = len(virtual_samples)
            
            # Calculate expected total samples at 100Hz
            expected_virtual_total = int(defined_time * 100)
            
            # Get new samples to send
            samples_to_send = []
            
            if total_virtual_samples > 0:
                # Ensure index is valid
                if last_sent_index >= total_virtual_samples:
                    # All samples sent
                    logger.debug(f"Already sent all {total_virtual_samples} virtual samples (index={last_sent_index})")
                    data_count = 0
                else:
                    # Get new virtual samples since last transmission
                    new_samples = virtual_samples[last_sent_index:]
                    
                    # Limit to 50 samples per packet
                    data_count = min(len(new_samples), 50)
                    samples_to_send = new_samples[:data_count]
                    
                    logger.debug(f"Sending {data_count} virtual samples from index {last_sent_index}")
            else:
                data_count = 0
            
            # Set DataCount
            data[42] = data_count
            
            # Dummy1 (not used)
            data[43] = 0
    
            # StartMeasureIndex - Tell Delphi total expected samples (always 100Hz)
            struct.pack_into('<i', data, 44, expected_virtual_total)
            
            # Version and other fields
            data[48] = Config.FIRMWARE_VERSION_MINOR
            data[49] = Config.FIRMWARE_VERSION_MAJOR
            data[50] = 0
            struct.pack_into('<H', data, 51, 0)
            data[53] = current_state.value
            data[54] = 0
            data[55] = 0
    
            # Pack the actual data values
            sent_values = []
            
            for i in range(50):
                if i < data_count:
                    # Each virtual sample is (value, is_interpolated, virtual_idx)
                    value, is_interpolated, virtual_idx = samples_to_send[i]
                    
                    # Apply scaling factor to improve visualization
                    scaled_value = value * SCALE_FACTOR
                    
                    # Sign-extend 22-bit to 32-bit for Delphi compatibility
                    if scaled_value & 0x80000000:  # Check sign bit in scaled value
                        delphi_value = scaled_value
                    else:
                        delphi_value = scaled_value
                    
                    # Add to our tracking for CSV logging
                    sent_values.append((value, delphi_value, is_interpolated, virtual_idx))
                else:
                    delphi_value = 0
                
                # Pack value into array (4 bytes per int)
                struct.pack_into('<i', data, 56 + i * 4, delphi_value)
            
            # Update index and log progress
            if data_count > 0:
                # Log to CSV
                if Config.CSV_ENABLED:
                    log_samples_to_csv(sent_values, sent_packet_counter, last_sent_index)
                
                # Update index
                prev_last_sent = last_sent_index
                last_sent_index += data_count
                
                # Log progress periodically
                if last_sent_index % 300 == 0 or last_sent_index >= total_virtual_samples:
                    progress_pct = (last_sent_index / max(1, expected_virtual_total)) * 100
                    logger.info(f"📊 Progress: {last_sent_index}/{expected_virtual_total} virtual samples sent ({progress_pct:.1f}%)")
                
                # Flag when we've sent all available data
                if last_sent_index >= total_virtual_samples:
                    logger.info(f"📊 Completed sending all {total_virtual_samples} virtual samples to Delphi client")

        return bytes(data)

    except Exception as e:
        logger.error(f"❌ Error creating TTCP_DATA: {e}", exc_info=True)
        
        # Return a minimal valid packet on error
        minimal_data = bytearray(256)
        struct.pack_into('<i', minimal_data, 0, 256)  # Size
        struct.pack_into('<i', minimal_data, 4, 0)    # Runtime
        return bytes(minimal_data)
    

def handle_client(conn: socket.socket, addr: Tuple[str, int]) -> None:
    """
    Handle client connection with proper command handling
    """
    global last_sent_index
    
    client_id = f"{addr[0]}:{addr[1]}"
    logger.info("🔌 Connected to %s", client_id)
    system_state.increment_clients()
    system_state.set("last_ip", addr[0])

    # Set TCP_NODELAY for this connection (critical for Delphi)
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.settimeout(1.0)  # Increased from 0.05 to ensure complete data transfer
    except Exception as e:
        logger.warning("⚠️ Could not set TCP_NODELAY for %s: %s", client_id, e)

    # Track consecutive errors
    error_count = 0
    MAX_ERRORS = 10  # Increased for more resilience

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

    # Track if we need to complete a data transfer after measurement end
    completed_measurement = False
    data_transfer_complete = False
    last_data_send_time = time.time()
    
    try:
        while not shutdown_requested.is_set():
            current_time = time.time()
            
            # === CRITICAL FIX: Check if we need to complete data transfer ===
            if (completed_measurement and not data_transfer_complete):
                # [... existing data transfer completion code ...]
                pass

            # === NEW: Always send status updates in IDLE state ===
            if (system_state.get_q628_state() == Q628State.IDLE and 
                current_time - last_data_send_time >= 0.5):
                try:
                    response = build_ttcp_data()
                    conn.sendall(response)
                    last_data_send_time = current_time
                    logger.debug("📤 Sent periodic status update in IDLE state")
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Error sending periodic status: {e} (error #{error_count})")
                    if error_count >= MAX_ERRORS:
                        break

            # Receive command with timeout
            try:
                cmd_data = conn.recv(Config.BUFFER_SIZE)
                if not cmd_data:
                    logger.info("👋 Client %s disconnected", client_id)
                    break

                # Log received command in detail
                logger.info("Received from %s (%d bytes): %s", 
                           client_id, len(cmd_data), cmd_data.hex())

                # Reset error count on successful receive
                error_count = 0

                # Parse the command if it looks like a valid TTCP_CMD
                cmd_info = None
                if len(cmd_data) >= 32:  # Ensure we have enough data for a command
                    cmd_info = parse_ttcp_cmd(cmd_data)

                if cmd_info:
                    logger.info(f"✅ Command recognized: {cmd_info['cmd']} - Current state: {system_state.get_q628_state().name}")
                    # Process command based on type
                    if cmd_info['cmd_type'] == CommandType.START:
                        logger.info(f"⚙️ START COMMAND received: Measurement time={cmd_info['runtime']}s, HVAC={cmd_info['hvac']}V, HVDC={cmd_info['hvdc']}V")
                        system_state.set("defined_measurement_time", cmd_info['runtime'])
                        
                        # Set enable_from_pc flag for manual start mode
                        enable_from_pc.set()
                        logger.info("✅ Enable flag set - waiting for button press")
                        
                        # Reset data transfer flags for new measurement
                        completed_measurement = False
                        data_transfer_complete = False
                        
                        # Reset last_sent_index for new measurement
                        last_sent_index = 0

                    elif cmd_info['cmd_type'] == CommandType.TRIGGER:  # Simulate button press
                        logger.info("🔄 TRIGGER command received (simulate button press)")
                        simulate_start.set()
                        
                        # Reset data transfer flags for new measurement
                        completed_measurement = False
                        data_transfer_complete = False

                    elif cmd_info['cmd_type'] == CommandType.GET_STATUS:  # Get status (polling)
                        logger.info("📊 GET STATUS command received")
                        # Just send current status (handled below)

                    elif cmd_info['cmd_type'] == CommandType.MOTOR_UP:  # Move motor up
                        logger.info("⬆️ MOTOR UP command received")
                        move_motor(MotorPosition.UP)

                    elif cmd_info['cmd_type'] == CommandType.MOTOR_DOWN:  # Move motor down
                        logger.info("⬇️ MOTOR DOWN command received")
                        move_motor(MotorPosition.DOWN)

                    elif cmd_info['cmd_type'] == CommandType.RESET:  # Reset: disable HV, home motor
                        logger.info("🔄 RESET command received")
                        # Clear state and reinitialize
                        system_state.end_measurement()
                        stop_sampling()
                        set_high_voltage(False)
                        set_valve(ValveStatus.CLOSED)
                        move_motor(MotorPosition.UP)
                        
                        # Reset data transfer flags
                        completed_measurement = False
                        data_transfer_complete = False

                    else:
                        logger.warning(f"⚠️ Unknown command: {cmd_info['cmd']}")
                else:
                    # Could not parse as a valid command
                    logger.warning(f"⚠️ Received invalid command data: {cmd_data.hex()}")

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

            # Always send a response packet with current status
            # This is critical for Delphi compatibility
            try:
                response = build_ttcp_data()
                last_data_send_time = current_time
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
                    
            # === Check if measurement just completed ===
            current_state = system_state.get_q628_state()
            previous_state = system_state.get("prev_q628_state")
            
            if (current_state == Q628State.IDLE and 
                previous_state == Q628State.FINISH and 
                not completed_measurement):
                logger.info("🏁 Measurement completed - ensuring all data is sent")
                completed_measurement = True
                data_transfer_complete = False
                
                # Send immediate response to start the completion process
                try:
                    response = build_ttcp_data()
                    conn.sendall(response)
                except Exception as e:
                    logger.error(f"❌ Error starting data transfer completion: {e}")
            
            # Keep track of previous state for state transition detection
            system_state.set("prev_q628_state", current_state)

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
            # Instead of calling free_port, just create a new socket with SO_REUSEADDR
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # Try to bind, might fail if port is in use
            try:
                server.bind(('0.0.0.0', port))
            except OSError as e:
                logger.error(f"⚠️ Could not bind to port {port}: {e}")
                logger.info(f"Retrying in 5 seconds...")
                time.sleep(5)
                continue
                
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

            # Add virtual sampling status
            if state_dict['measurement_active']:
                virtual_samples = len(virtual_buffer.get_all())
                real_samples = len(adc_buffer.get_all())
                expected = int(state_dict['defined_measurement_time'] * Config.VIRTUAL_SAMPLE_RATE)
                logger.info(f"📊 Virtual samples: {virtual_samples}/{expected} ({virtual_samples/expected*100:.1f}%), Real samples: {real_samples}")

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

def main() -> None:
    """Main application entry point with improved setup"""
    # Only setup logging once, at the start of the program
    global logger
    logger = logging.getLogger("qumat628")
    
    # Prevent duplicate logger initialization
    if not logger.handlers:
        logger = setup_logging()

    logger.info("Starting QUMAT628 control system with improved 100Hz virtual interpolation...")
    logger.info("📊 Targeting 16.67Hz real sampling with interpolation to 100Hz virtual grid")

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
        print("QUMAT628 Control System with 100Hz Delphi Compatibility starting...")
        
        # Run the main function - logging will be set up inside
        main()
    except Exception as e:
        # Get the logger if it exists
        try:
            logger = logging.getLogger("qumat628")
            logger.critical("Fatal error in main program: %s", e, exc_info=True)
        except:
            # Fallback if logger isn't initialized
            print(f"CRITICAL ERROR: {e}")
            traceback.print_exc()
            
        # Try to clean up GPIO before exiting
        try:
            GPIO.cleanup()
        except:
            pass
        sys.exit(1)
