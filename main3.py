#!/usr/bin/env python3
"""
QUMAT628 Control System
-----------------------
Headless control system for QUMAT628 hardware with Delphi-compatible TCP server
and DHCP support. Manages motor movement, valve control, high voltage, and
measurement cycles with enhanced motor status monitoring.

Author: Improved by Master Coder
Version: 2.2
Date: 2023
"""

import socket
import struct
import threading
import time
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
    I2C_ADDR_MOTOR = 0x36
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
    VREF_ADC = 2.048
    VREF_DAC = 4.096
    DAC_GAIN = 1.2
    
    # GPIO Pin Assignments
    PIN_CS_ADC = 10
    PIN_CLK_ADC = 11
    PIN_SDO_ADC = 9
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
    ADC_SAMPLE_INTERVAL = 0.1    # How often to sample ADC during measurement
    
    # File Paths
    LOG_FILE = "qumat628.log"
    
    # Device Information
    SERIAL_NUMBER = 6280004
    FIRMWARE_VERSION_MAJOR = 6
    FIRMWARE_VERSION_MINOR = 1
    
    # ADC/DAC Configuration
    ADC_BUFFER_SIZE = 2000       # Size of ring buffer for ADC samples (match C code)
    ADC_CLK_DELAY = 0.00001      # Clock delay for bit-banged SPI
    DAC_CLK_DELAY = 0.00001      # Clock delay for bit-banged SPI
    
    # Profile Delays (in seconds) - matching original C code
    PROFILE_DELAYS = {
        0: 0.2, 1: 0.3, 2: 0.6, 3: 1.0, 4: 2.0, 5: 4.0,
        6: 8.0, 7: 8.0, 8: 12.0, 9: 12.0, 10: 20.0,
        11: 20.0, 12: 40.0, 13: 40.0
    }
    DEFAULT_PROFILE_DELAY = 5.0


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
    """Q628 state machine states - matching original C code"""
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
            "start_time": time.time(),
            "q628_state": Q628State.POWER_UP,
            "q628_timer": time.time(),
            "profile_hin": 4,         # Default profile for movement
            "profile_zurueck": 4,     # Default profile for return
            "wait_acdc": 11,          # Default wait time for AC/DC
            "temperature": 23.5,      # Default temperature in °C
            "humidity": 45.0,         # Default humidity in %
            "hv_ac_value": 230,       # Default HV AC value
            "hv_dc_value": 6500,      # Default HV DC value
            "adc_data": []            # ADC measurement data
        }
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Any:
        """Thread-safe state access"""
        with self._lock:
            return self._state.get(key)
    
    def set(self, key: str, value: Any) -> None:
        """Thread-safe state update"""
        with self._lock:
            self._state[key] = value
    
    def update(self, **kwargs) -> None:
        """Thread-safe update of multiple state attributes"""
        with self._lock:
            self._state.update(kwargs)
    
    def is_measurement_active(self) -> bool:
        """Check if measurement is active"""
        with self._lock:
            return self._state["measurement_active"]
    
    def start_measurement(self) -> bool:
        """Attempt to start measurement"""
        with self._lock:
            if self._state["measurement_active"]:
                return False
            self._state["measurement_active"] = True
            return True
    
    def end_measurement(self) -> None:
        """End measurement"""
        with self._lock:
            self._state["measurement_active"] = False
    
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
            if len(self._state["adc_data"]) < 50:  # Keep max 50 values for TCP response
                self._state["adc_data"].append(value)
            else:
                self._state["adc_data"] = self._state["adc_data"][1:] + [value]
    
    def clear_adc_data(self) -> None:
        """Clear ADC measurement data"""
        with self._lock:
            self._state["adc_data"] = []
    
    def get_profile_delay(self, profile_id: int) -> float:
        """Get delay time for specified profile"""
        return Config.PROFILE_DELAYS.get(profile_id, Config.DEFAULT_PROFILE_DELAY)


# ===== Ring Buffer for ADC Data =====
class RingBuffer:
    """Circular buffer for ADC samples"""
    def __init__(self, size: int):
        self.buffer = deque(maxlen=size)
        self.lock = threading.Lock()
    
    def enqueue(self, value: int) -> None:
        """Add value to buffer"""
        with self.lock:
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
        """Clear all values"""
        with self.lock:
            self.buffer.clear()
    
    def get_all(self) -> List[int]:
        """Get all values as list"""
        with self.lock:
            return list(self.buffer)
    
    def __len__(self) -> int:
        """Get current buffer size"""
        with self.lock:
            return len(self.buffer)


# ===== Global Variables =====
system_state = SystemState()
i2c_bus = None  # Will be initialized in hardware setup
adc_buffer = RingBuffer(Config.ADC_BUFFER_SIZE)  # Larger buffer matching C code

# Synchronization primitives
i2c_lock = threading.Lock()
adc_lock = threading.Lock()
dac_lock = threading.Lock()
shutdown_requested = threading.Event()
enable_from_pc = threading.Event()
simulate_start = threading.Event()
sampling_active = threading.Event()

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


# ===== Hardware Control Functions =====
def initialize_hardware() -> bool:
    """Initialize hardware with comprehensive error checking"""
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
        
        # Start ADC sampling thread
        threading.Thread(
            target=adc_sampling_thread,
            daemon=True
        ).start()
        logger.info("✅ ADC sampling thread started")
        
        # Start Q628 state machine thread
        threading.Thread(
            target=q628_state_machine,
            daemon=True
        ).start()
        logger.info("✅ Q628 state machine started")
            
        logger.info("✅ Hardware initialization completed successfully")
        return True
        
    except Exception as e:
        logger.error("❌ Hardware initialization failed: %s", e, exc_info=True)
        return False


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
    Read motor status via I2C
    
    Returns:
        MotorStatus object or None if error
    """
    try:
        with i2c_lock:
            # First send passive mode command
            i2c_bus.write_byte(Config.I2C_ADDR_MOTOR, Config.CMD_PASSIV)
            time.sleep(0.01)  # Short delay
            
            # Then send status request command
            i2c_bus.write_byte(Config.I2C_ADDR_MOTOR, Config.CMD_STATUS)
            time.sleep(0.01)  # Short delay
            
            # Read status byte
            status_byte = i2c_bus.read_byte(Config.I2C_ADDR_MOTOR)
            
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
    
    while not shutdown_requested.is_set():
        try:
            status = read_motor_status()
            if status:
                logger.debug("Motor status: %s", status)
                
                # If motor is in error state, log it prominently
                if status & MotorStatus.ERROR:
                    logger.warning("⚠️ Motor error detected!")
                
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
            logger.warning("⚠️ Motor command may not have been accepted")
            return True  # Still return True as we did send the command
    except Exception as e:
        logger.error("❌ Error moving motor: %s", e)
        system_state.set("last_error", f"Motor error: {str(e)}")
        return False


def set_high_voltage(enabled: bool) -> None:
    """
    Enable or disable high voltage
    
    Args:
        enabled: True to enable, False to disable
    """
    status_text = 'ON' if enabled else 'OFF'
    logger.info("⚡ High voltage: %s", status_text)
    
    try:
        system_state.set("hv_status", enabled)
        GPIO.output(Config.HVAC_GPIO, GPIO.HIGH if enabled else GPIO.LOW)
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


# ===== ADC/DAC Functions =====
def read_adc() -> Tuple[float, int]:
    """
    Read ADC value using bit-banged SPI
    
    Returns:
        Tuple of (voltage, raw_value)
    """
    with adc_lock:
        try:
            # Start conversion
            GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
            
            # Wait for ADC to be ready (SDO goes LOW)
            timeout = time.time() + 1.0  # 1 second timeout
            while GPIO.input(Config.PIN_SDO_ADC) == GPIO.HIGH:
                if time.time() > timeout:
                    GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
                    raise TimeoutError("ADC ready signal timeout")
                time.sleep(0.001)
            
            # Read 24 bits
            result = 0
            for _ in range(24):
                GPIO.output(Config.PIN_CLK_ADC, GPIO.HIGH)
                time.sleep(Config.ADC_CLK_DELAY)
                bit = GPIO.input(Config.PIN_SDO_ADC)
                result = (result << 1) | bit
                GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
                time.sleep(Config.ADC_CLK_DELAY)
            
            # End conversion
            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
            
            # Extract 22-bit signed value
            value = result & 0x3FFFFF
            if value & (1 << 21):  # Check sign bit
                value -= (1 << 22)  # Convert to signed
            
            # Convert to voltage
            voltage = (value / (1 << 21)) * Config.VREF_ADC
            
            return voltage, value
        except Exception as e:
            logger.error("❌ Error reading ADC: %s", e)
            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)  # Ensure CS is high on error
            return 0.0, 0


def set_dac(value: int) -> None:
    """
    Set DAC value using bit-banged SPI
    
    Args:
        value: 16-bit DAC value
    """
    with dac_lock:
        try:
            # Ensure value is 16-bit
            value &= 0xFFFF
            
            # Start transmission
            GPIO.output(Config.PIN_CS_DAC, GPIO.LOW)
            time.sleep(Config.DAC_CLK_DELAY)
            
            # Send 16 bits MSB first
            for i in range(15, -1, -1):
                bit = (value >> i) & 1
                GPIO.output(Config.PIN_DAT_DAC, bit)
                GPIO.output(Config.PIN_CLK_DAC, GPIO.HIGH)
                time.sleep(Config.DAC_CLK_DELAY)
                GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
                time.sleep(Config.DAC_CLK_DELAY)
            
            # End transmission
            GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)
        except Exception as e:
            logger.error("❌ Error setting DAC: %s", e)
            GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Ensure CS is high on error


def set_dac_voltage(voltage: float) -> None:
    """
    Set DAC output voltage
    
    Args:
        voltage: Desired output voltage
    """
    # Calculate DAC value
    dac_value = int((voltage / Config.VREF_DAC) * 65535)
    set_dac(dac_value)
    logger.debug("DAC set to %.3fV (value: %d)", voltage, dac_value)


def adc_sampling_thread() -> None:
    """
    Thread for continuous ADC sampling when active
    """
    logger.info("🔄 ADC sampling thread started")
    sample_count = 0
    
    while not shutdown_requested.is_set():
        try:
            if sampling_active.is_set():
                # Read ADC
                voltage, raw_value = read_adc()
                
                # Store in ring buffer
                adc_buffer.enqueue(raw_value)
                
                # Update system state with latest values (keep max 50 for TCP response)
                system_state.add_adc_data(raw_value)
                
                # Log every 100 samples
                sample_count += 1
                if sample_count % 100 == 0:
                    logger.debug("ADC sample #%d: %.3fV", sample_count, voltage)
                
                # Sleep for sample interval
                time.sleep(Config.ADC_SAMPLE_INTERVAL)
            else:
                # Reset sample count when not sampling
                if sample_count > 0:
                    logger.info("ADC sampling stopped after %d samples", sample_count)
                    sample_count = 0
                
                # Sleep longer when not sampling
                time.sleep(0.5)
        except Exception as e:
            logger.error("❌ Error in ADC sampling thread: %s", e)
            time.sleep(1.0)  # Sleep longer on error


def start_sampling() -> None:
    """Start ADC sampling"""
    logger.info("📊 Starting ADC sampling")
    adc_buffer.clear()
    system_state.clear_adc_data()
    sampling_active.set()


def stop_sampling() -> None:
    """Stop ADC sampling"""
    logger.info("📊 Stopping ADC sampling")
    sampling_active.clear()


# ===== Q628 State Machine =====
def q628_state_machine() -> None:
    """
    Main Q628 state machine thread - implements the original C code state machine
    """
    logger.info("🔄 Q628 state machine started")
    
    # Initial state
    system_state.set_q628_state(Q628State.POWER_UP)
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
                
                # Open valve
                set_valve(ValveStatus.OPEN)
                
                # Move motor to home position
                move_motor(MotorPosition.UP)
                
                # Transition to PRE_IDLE
                system_state.set_q628_state(Q628State.PRE_IDLE)
                
            elif current_state == Q628State.PRE_IDLE:
                # Preparing for idle state
                if elapsed > 3.0:
                    # After motor has time to move, close valve
                    set_valve(ValveStatus.CLOSED)
                    
                    # Transition to IDLE
                    system_state.set_q628_state(Q628State.IDLE)
                    
                    # Blink LED slowly to indicate ready
                    threading.Thread(
                        target=blink_led,
                        args=("slow", 3.0),
                        daemon=True
                    ).start()
                
            elif current_state == Q628State.IDLE:
                # System idle, waiting for commands
                # Check if start command received
                if enable_from_pc.is_set() or simulate_start.is_set():
                    if simulate_start.is_set():
                        # Automatic start (simulated button press)
                        logger.info("✅ Automatic measurement start (simulated)")
                        simulate_start.clear()
                        
                        # Skip waiting for button, go directly to WAIT_HV
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
                # Waiting for start button press
                # Check if button pressed (handled in separate thread)
                if GPIO.input(Config.START_TASTE_GPIO) == GPIO.LOW:
                    # Button pressed, debounce
                    time.sleep(Config.BUTTON_DEBOUNCE_TIME)
                    if GPIO.input(Config.START_TASTE_GPIO) == GPIO.LOW:
                        logger.info("👇 Button pressed, starting measurement")
                        
                        # Transition to WAIT_HV
                        system_state.set_q628_state(Q628State.WAIT_HV)
                
                # Check for timeout (2 minutes)
                if elapsed > 120.0:
                    logger.warning("⏰ Timeout waiting for button press")
                    GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
                    
                    # Return to IDLE
                    system_state.set_q628_state(Q628State.IDLE)
            
            elif current_state == Q628State.WAIT_HV:
                # Waiting for HV stabilization
                # Start measurement
                system_state.start_measurement()
                system_state.increment_cycle_count()
                
                # Turn ON LED steady to signal measurement is running
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
                
                # Blink LED fast for preparation
                threading.Thread(
                    target=blink_led,
                    args=("fast", 5.0),
                    daemon=True
                ).start()
                
                # Turn ON High Voltage
                set_high_voltage(True)
                
                # Open Valve
                set_valve(ValveStatus.OPEN)
                
                # Wait for profile delay
                profile_hin = system_state.get("profile_hin")
                wait_time = system_state.get_profile_delay(profile_hin)
                
                if elapsed > wait_time:
                    # Transition to START
                    system_state.set_q628_state(Q628State.START)
            
            elif current_state == Q628State.START:
                # Starting measurement
                # Start ADC sampling
                start_sampling()
                
                # Move motor down
                move_motor(MotorPosition.DOWN)
                
                # Transition to PRE_ACTIV
                system_state.set_q628_state(Q628State.PRE_ACTIV)
            
            elif current_state == Q628State.PRE_ACTIV:
                # Preparing for active measurement
                # Wait for motor to complete movement
                motor_status = system_state.get("motor_status")
                
                if not (motor_status & MotorStatus.BUSY) or elapsed > 5.0:
                    # Turn OFF High Voltage
                    set_high_voltage(False)
                    
                    # Transition to ACTIV
                    system_state.set_q628_state(Q628State.ACTIV)
                    
                    # Calculate total measurement time
                    measurement_time = Config.MEASUREMENT_TOTAL_TIME - elapsed
                    logger.info(f"⏱️ Active measurement for {measurement_time:.1f} seconds")
            
            elif current_state == Q628State.ACTIV:
                # Active measurement
                # Check if measurement time completed
                if elapsed > Config.MEASUREMENT_TOTAL_TIME:
                    # Stop sampling
                    stop_sampling()
                    
                    # Transition to FINISH
                    system_state.set_q628_state(Q628State.FINISH)
            
            elif current_state == Q628State.FINISH:
                # Finishing measurement
                # Move motor up
                move_motor(MotorPosition.UP)
                
                # Wait for motor to reach home
                wait_for_motor_home(timeout=10)
                
                # Close valve
                set_valve(ValveStatus.CLOSED)
                
                # End measurement
                system_state.end_measurement()
                
                # Blink LED slowly to indicate ready for next cycle
                threading.Thread(
                    target=blink_led,
                    args=("slow", 3.0),
                    daemon=True
                ).start()
                
                # Return to IDLE
                system_state.set_q628_state(Q628State.IDLE)
            
            # Sleep a short time to prevent CPU hogging
            time.sleep(0.1)
            
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
            
            time.sleep(1.0)  # Sleep longer on error


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


# ===== Network Protocol Functions =====
def parse_ttcp_cmd(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse Delphi-compatible TTCP_CMD packet
    
    Args:
        data: Raw bytes from TCP connection
        
    Returns:
        Dictionary with parsed command or None if invalid
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

        logger.info("Parsed TTCP_CMD: %s (runtime=%ds, HVAC=%d, HVDC=%d, profiles=%d/%d)",
                   cmd_char, runtime, hvac, hvdc, profile1, profile2)
                   
        # Update system state with received parameters
        system_state.update(
            profile_hin=profile1,
            profile_zurueck=profile2,
            wait_acdc=wait_acdc,
            hv_ac_value=hvac,
            hv_dc_value=hvdc
        )

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
        logger.error("Error parsing TTCP_CMD: %s", e)
        return None


def build_ttcp_data() -> bytes:
    """
    Build correctly formatted 280-byte TTCP_DATA response
    
    Returns:
        Byte array with formatted response data
    """
    try:
        # Create a 280-byte data structure (initialized to zeros)
        data = bytearray(280)
        
        # Get current system state for accurate reporting
        state_dict = system_state.get_status_dict()
        active = state_dict["measurement_active"]
        
        # Calculate status bits
        status1 = 0
        if state_dict["hv_status"]:
            status1 |= 0x01  # HV on
        if state_dict["valve_status"] == ValveStatus.OPEN:
            status1 |= 0x02  # Valve open
        if state_dict["motor_position"] == MotorPosition.UP:
            status1 |= 0x04  # Motor up
        if active:
            status1 |= 0x08  # Measurement active
            
        # Add motor status bits
        motor_status = state_dict["motor_status"]
        if motor_status & MotorStatus.HOME:
            status1 |= 0x10  # Motor home position
        if motor_status & MotorStatus.ERROR:
            status1 |= 0x20  # Motor error
        if motor_status & MotorStatus.BUSY:
            status1 |= 0x40  # Motor busy
            
        # Set Status2: bit 0 always set for compatibility, bit 1 indicates sampling active
        status2 = 0x01  # Always set first bit for compatibility
        if sampling_active.is_set():
            status2 |= 0x02  # Bit 1 = Sampling is active
        
        # Get temperature and humidity
        temp = state_dict["temperature"]
        humidity = state_dict["humidity"]
        temp_int = int(temp * 100)  # Convert to centidegrees
        humidity_int = int(humidity * 100)  # Convert to centi-percent
        
        # Get HV values
        hv_ac = state_dict["hv_ac_value"]
        hv_dc = state_dict["hv_dc_value"]
        
        # Get profile values
        profile_hin = state_dict["profile_hin"]
        profile_zurueck = state_dict["profile_zurueck"]
        wait_acdc = state_dict["wait_acdc"]
        
        # Basic header (all values little-endian)
        struct.pack_into('<I', data, 0, 280)                    # Size
        struct.pack_into('<I', data, 4, system_state.get_runtime())  # Runtime (seconds)
        
        # Sensor data
        struct.pack_into('<i', data, 8, temp_int)               # Temperature
        struct.pack_into('<i', data, 12, humidity_int)          # Humidity
        struct.pack_into('<i', data, 16, hv_ac)                 # HV_AC value
        struct.pack_into('<i', data, 20, hv_dc)                 # HV_DC value
        
        # Profiles and wait times
        struct.pack_into('<i', data, 24, profile_hin)           # ProfileHin
        struct.pack_into('<i', data, 28, profile_zurueck)       # ProfileZurueck
        struct.pack_into('<i', data, 32, wait_acdc)             # Wait_ACDC
        struct.pack_into('<i', data, 36, Config.SERIAL_NUMBER)  # SerialNumber
        
        # Status bytes
        data[40] = status1                                      # Status1 (bitflags)
        data[41] = status2                                      # Status2 (bitflags)
        
        # Get ADC data and set DataCount
        adc_data = state_dict["adc_data"]
        data_count = min(50, len(adc_data))  # Maximum 50 values as per Delphi structure
        data[42] = data_count                                   # DataCount - actual number of values
        data[43] = 0                                            # Reserved
        
        # Measurement data array (up to 50 int32 values)
        # Starting at byte offset 44, each value is a 4-byte integer
        for i in range(data_count):
            struct.pack_into('<i', data, 44 + i*4, adc_data[i])
        
        # Zero out any unused data slots
        for i in range(data_count, 50):
            struct.pack_into('<i', data, 44 + i*4, 0)
        
        # Additional firmware parameters
        struct.pack_into('<i', data, 244, 0)                    # StartMeasureIndex
        data[248] = Config.FIRMWARE_VERSION_MINOR               # Minor version
        data[249] = Config.FIRMWARE_VERSION_MAJOR               # Major version
        data[250] = 0                                           # MoveNext
        struct.pack_into('<H', data, 251, 0)                    # MoveWait
        data[253] = 1                                           # Q528_State
        data[254] = 0                                           # Reserved
        data[255] = 0                                           # Reserved
        
        # Remaining bytes (256-279) should be zero
        
        return bytes(data)
    except Exception as e:
        logger.error("Error building TTCP_DATA: %s", e)
        # Return a minimal valid packet in case of error
        return struct.pack('<I', 280) + bytes(276)


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


def handle_client(conn: socket.socket, addr: Tuple[str, int]) -> None:
    """
    Handle client connection with proper command processing
    
    Args:
        conn: Connected socket object
        addr: Client address tuple (ip, port)
    """
    client_id = f"{addr[0]}:{addr[1]}"
    logger.info("Connected to %s", client_id)
    system_state.increment_clients()
    
    # Set TCP_NODELAY for this connection (critical for Delphi)
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    
    # Send immediate response on connection (required by Delphi)
    response = build_ttcp_data()
    try:
        conn.sendall(response)
        logger.info("Sent initial data packet to %s (%d bytes)", client_id, len(response))
    except Exception as e:
        logger.error("Error sending initial packet: %s", e)
        conn.close()
        system_state.decrement_clients()
        return

    try:
        while not shutdown_requested.is_set():
            # Receive command with timeout
            conn.settimeout(1.0)
            try:
                cmd_data = conn.recv(Config.BUFFER_SIZE)
                if not cmd_data:
                    logger.info("Client %s disconnected", client_id)
                    break
            except socket.timeout:
                # Just continue the loop on timeout
                continue
            except Exception as e:
                logger.error("Error receiving data: %s", e)
                break
                
            # Log received command
            logger.info("Received from %s (%d bytes): %s", 
                       client_id, len(cmd_data), cmd_data.hex())
            
            # Parse the command if it looks like a valid TTCP_CMD
            cmd_info = parse_ttcp_cmd(cmd_data) if len(cmd_data) >= 16 else None
            
            if cmd_info:
                # Process command based on type
                if cmd_info['cmd_type'] == CommandType.START:  # Start measurement (ENABLE)
                    logger.info("Received START command")
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
                    set_high_voltage(False)
                    set_valve(ValveStatus.CLOSED)
                    move_motor(MotorPosition.UP)
                    
                else:
                    logger.warning("Unknown command: %s", cmd_info['cmd'])
            
            # Always send a response packet with current status
            # This is critical for Delphi compatibility
            response = build_ttcp_data()
            try:
                conn.sendall(response)
                logger.debug("Sent response to %s (%d bytes)", client_id, len(response))
            except Exception as e:
                logger.error("Error sending response: %s", e)
                break
            
    except Exception as e:
        logger.error("Error handling client %s: %s", client_id, e, exc_info=True)
    finally:
        try:
            conn.close()
        except:
            pass
        system_state.decrement_clients()
        logger.info("Connection to %s closed", client_id)


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
                f"Measurement={'ACTIVE' if state_dict['measurement_active'] else 'INACTIVE'}"
            )
            logger.info("📊 %s", status)
            
            # Read temperature and humidity periodically
            read_temperature_humidity()
            
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
    main()
    
    