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
import select
import os
import psutil  # 💡 Hardened: For monitoring system resources
import signal  # 💡 Hardened: For handling timeouts
import atexit  # 💡 Hardened: For registering cleanup handlers
import fcntl   # 💡 Hardened: For file locking during CSV operations
import errno   # 💡 Hardened: For detailed error classification
from functools import wraps  # 💡 Hardened: For decorator functions
from smbus2 import SMBus, i2c_msg
from enum import Enum, Flag, auto
from typing import Dict, Any, Optional, Tuple, Union, List, Callable
from collections import deque
import hashlib  # 💡 Hardened: For data integrity verification


# 💡 Hardened: Global watchdog and recovery flags
global_system_healthy = True
last_successful_operation = time.time()
last_successful_sampling = time.time()
recovery_in_progress = False
watchdog_last_tick = time.time()
system_startup_time = time.time()


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
    
    # ADC Timing Constants 
    ADC_TARGET_RATE = 50.0       # Target sample rate: 50Hz
    ADC_SAMPLE_INTERVAL = 1.0/ADC_TARGET_RATE  # 20ms between samples
    
    # Virtual Sampling Grid for Delphi Compatibility
    VIRTUAL_SAMPLE_RATE = 100.0  # Virtual 100Hz sample rate expected by Delphi
    VIRTUAL_SAMPLE_INTERVAL = 1.0/VIRTUAL_SAMPLE_RATE  # 10ms between virtual samples
    SAMPLES_PER_SECOND = 100     # Delphi expects 100 samples per second exactly
    
    HYT_SAMPLE_INTERVAL = 1.0    # How often to measure temperature and humidity
    TEMP_HUMID_SAMPLE_INTERVAL = 1.0  # Interval for temperature and humidity measurement

    # File Paths
    LOG_FILE = "qumat628.log"
    
    # CSV Data Logging
    CSV_ENABLED = True
    CSV_FILENAME = "delphi_data.csv"  # Single CSV file for all measurements
    # 💡 Hardened: Added backup CSV file name and directory
    CSV_BACKUP_DIR = "csv_backups"
    BACKUP_CSV_FILENAME = "delphi_data_backup.csv"

    # Device Information
    SERIAL_NUMBER = 6280004
    FIRMWARE_VERSION_MAJOR = 6
    FIRMWARE_VERSION_MINOR = 2

    # ADC/DAC Configuration
    ADC_BUFFER_SIZE = 10000      # Size of ring buffer for ADC samples
    VIRTUAL_BUFFER_SIZE = 12000  # Size of buffer for 100Hz virtual samples - increased for longer measurements
    ADC_CLK_DELAY = 0.000002     # Clock delay for bit-banged SPI (2μs for ~250kHz)
    DAC_CLK_DELAY = 0.00001      # Clock delay for bit-banged SPI
    FIXED_INITIALIZATION_TIME = 2 # Fixed initialization time for ADC/DAC
    
    # HV Control timing
    HV_OFF_DELAY_AFTER_DOWN = 4.0  # Delay in seconds before turning HV off after motor down
    
    # Scale factor for better visualization in Delphi
    SCALE_FACTOR = 16            # 9-bit shift equivalent
    
    # 💡 Hardened: Added watchdog and recovery configuration
    WATCHDOG_INTERVAL = 1.0      # How often to check system health
    WATCHDOG_TIMEOUT = 5.0       # Max time without activity before recovery
    MAX_RECOVERY_ATTEMPTS = 3    # Maximum number of recovery attempts
    I2C_RECOVERY_DELAY = 0.5     # Delay between I2C recovery attempts
    
    # 💡 Hardened: Error recovery thresholds
    MAX_I2C_RETRIES = 3          # Maximum retries for I2C operations
    MAX_ADC_RETRIES = 3          # Maximum retries for ADC operations
    MAX_DAC_RETRIES = 3          # Maximum retries for DAC operations
    OPERATION_TIMEOUT = 2.0      # General operation timeout
    
    # 💡 Hardened: Hardware integrity check intervals
    INTEGRITY_CHECK_INTERVAL = 60.0  # Check hardware every minute
    CSV_FLUSH_INTERVAL = 5.0     # Flush CSV every 5 seconds
    
    # 💡 Hardened: Resource monitoring thresholds
    MIN_FREE_DISK_SPACE_MB = 50  # Minimum free disk space in MB
    MAX_CPU_USAGE_PCT = 90       # Maximum CPU usage percentage


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
    # 💡 Hardened: Added recovery state for handling fault conditions
    RECOVERY = auto()      # Trying to recover from fault


# 💡 Hardened: Thread-safe decorator for timeout functionality
def timeout(seconds: float):
    """
    Thread-safe decorator that applies a timeout to a function call.
    Raises TimeoutError if the function doesn't complete in time.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = [None]
            exception = [None]
            completed = threading.Event()
            
            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e
                finally:
                    completed.set()
            
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            
            success = completed.wait(timeout=seconds)
            if not success:
                exception[0] = TimeoutError(f"Function {func.__name__} timed out after {seconds} seconds")
                
            if exception[0]:
                raise exception[0]
            return result[0]
        return wrapper
    return decorator


# 💡 Hardened: Error recovery decorator 
def with_retry(retries=3, delay=0.5, operation_name=None, allowed_exceptions=(Exception,)):
    """
    Retry decorator with configurable retries, delay, and exception types.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            last_exception = None
            
            for attempt in range(retries + 1):  # +1 for the initial attempt
                try:
                    if attempt > 0:
                        logger.warning(f"⚠️ Retry {attempt}/{retries} for {name}...")
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        logger.info(f"✅ {name} succeeded on retry {attempt}")
                    return result
                except allowed_exceptions as e:
                    last_exception = e
                    if attempt < retries:
                        logger.warning(f"⚠️ {name} failed with {type(e).__name__}: {e}, retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"❌ {name} failed after {retries} retries: {e}")
            
            # All retries failed
            raise last_exception
        return wrapper
    return decorator


# 💡 Hardened: Decorator for critical operations
def critical_operation(func):
    """
    Decorator for critical operations that should update health status
    and can trigger system recovery if they fail repeatedly.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        global global_system_healthy, last_successful_operation
        
        try:
            # Try to execute the operation
            result = func(*args, **kwargs)
            
            # Update healthy status
            global_system_healthy = True
            last_successful_operation = time.time()
            return result
            
        except Exception as e:
            # Log the failure
            logger.error(f"❌ Critical operation {func.__name__} failed: {e}")
            
            # Mark system as unhealthy
            global_system_healthy = False
            
            # Re-raise to be handled by caller
            raise
    
    return wrapper


# ===== Ring Buffer for ADC Data =====
class RingBuffer:
    """Circular buffer for samples with dynamic growth option"""

    def __init__(self, size: int):
        self.buffer = deque(maxlen=size)
        self.lock = threading.Lock()
        self.initial_size = size
        self.growth_factor = 1.5  # How much to grow when needed
        # 💡 Hardened: Add overflow counter
        self.overflow_count = 0
        self.last_overflow_time = 0

    def get_all(self) -> List[Any]:
        """Get all values as list without removing them"""
        with self.lock:
            return list(self.buffer)

    def enqueue(self, value: Any) -> None:
        """Add value to buffer, growing if needed"""
        with self.lock:
            # 💡 Hardened: Check for buffer health
            buffer_length = len(self.buffer)
            buffer_capacity = self.buffer.maxlen
            fill_percentage = (buffer_length / buffer_capacity) * 100 if buffer_capacity else 0
            
            # Check if buffer is getting full
            if buffer_length >= self.buffer.maxlen * 0.9:
                # Record overflow event
                self.overflow_count += 1
                self.last_overflow_time = time.time()
                
                # Grow the buffer
                new_size = int(self.buffer.maxlen * self.growth_factor)
                new_buffer = deque(self.buffer, maxlen=new_size)
                self.buffer = new_buffer
                logger.warning(f"💡 RingBuffer expanded from {buffer_capacity} to {new_size} (overflow #{self.overflow_count})")
            
            # 💡 Hardened: Periodically log buffer utilization
            if buffer_length % 1000 == 0:
                logger.debug(f"💡 Buffer utilization: {fill_percentage:.1f}% ({buffer_length}/{buffer_capacity})")
                
            self.buffer.append(value)

    def dequeue(self) -> Optional[Any]:
        """Remove and return oldest value"""
        with self.lock:
            # 💡 Hardened: Add protection against empty buffer dequeue
            if self.is_empty():
                logger.warning("💡 Attempted to dequeue from empty buffer")
                return None
            return self.buffer.popleft()

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
            # 💡 Hardened: Record buffer stats before clearing
            prev_size = len(self.buffer)
            prev_capacity = self.buffer.maxlen
            
            self.buffer.clear()
            # Reset to initial size when cleared
            if self.buffer.maxlen != self.initial_size:
                self.buffer = deque(maxlen=self.initial_size)
                logger.info(f"💡 Buffer reset: {prev_size}/{prev_capacity} → 0/{self.initial_size}")

    def __len__(self) -> int:
        """Get current buffer size"""
        with self.lock:
            return len(self.buffer)

    # 💡 Hardened: Added buffer health check method
    def health_check(self) -> Dict[str, Any]:
        """Return buffer health statistics"""
        with self.lock:
            capacity = self.buffer.maxlen
            size = len(self.buffer)
            fill_pct = (size / capacity) * 100 if capacity else 0
            return {
                "capacity": capacity,
                "size": size, 
                "fill_percentage": fill_pct,
                "overflow_count": self.overflow_count,
                "last_overflow_time": self.last_overflow_time
            }


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
            "sampling_active": False,
            "start_time": time.time(),
            "measurement_start_time": None,
            "button_press_time": None,
            "defined_measurement_time": 60,  # Default measurement time in seconds
            "total_expected_samples": 6000,  # 6000 for 60s at 100Hz
            "q628_state": Q628State.POWER_UP,
            "q628_timer": time.time(),
            "profile_hin": 4,
            "profile_zurueck": 4,
            "wait_acdc": 11,
            "temperature": 23.5,
            "humidity": 45.0,
            "field_strength": 0.0,
            "hv_ac_value": 230,
            "hv_dc_value": 6500,
            "adc_data": [],
            "last_adc_value": 0,
            "runtime": 0,
            "prev_q628_state": None,  # For tracking state transitions
            "virtual_sample_count": 0,  # Count of samples in 100Hz grid
            # 💡 Hardened: Added state tracking for diagnostics and recovery
            "last_error_time": None,
            "error_count": 0,
            "recovery_attempts": 0,
            "last_recovery_time": None,
            "i2c_error_count": 0,
            "adc_error_count": 0,
            "dac_error_count": 0,
            "network_error_count": 0,
            "last_successful_i2c": time.time(),
            "last_successful_adc": time.time(),
            "last_successful_dac": time.time(),
            "last_watchdog_tick": time.time(),
            "system_uptime": 0,
            "cpu_usage": 0.0,
            "mem_usage": 0.0,
            "disk_free_mb": 0,
            "threads_active": 0,
            "healthy": True
        }
        self._lock = threading.RLock()
        self._runtime_timer = None
        # 💡 Hardened: Create a state history for debugging
        self._state_history = deque(maxlen=100)
        self._last_update_time = time.time()
        self._health_check_time = time.time()

    # 💡 Hardened: Record state transitions for diagnostics
    def _record_state_transition(self):
        now = time.time()
        # Only record if at least 100ms has passed or q628_state changed
        with self._lock:
            current_state = self._state["q628_state"]
            prev_state = self._state.get("prev_q628_state")
            
            if (now - self._last_update_time > 0.1 or 
                (prev_state is not None and current_state != prev_state)):
                # Create a snapshot of critical state variables
                snapshot = {
                    "timestamp": now,
                    "q628_state": current_state,
                    "motor_position": self._state["motor_position"],
                    "valve_status": self._state["valve_status"],
                    "hv_status": self._state["hv_status"],
                    "measurement_active": self._state["measurement_active"],
                    "sampling_active": self._state["sampling_active"],
                    "virtual_sample_count": self._state["virtual_sample_count"],
                    "error_count": self._state["error_count"]
                }
                self._state_history.append(snapshot)
                self._last_update_time = now
                self._state["prev_q628_state"] = current_state

    def get_measurement_runtime(self) -> int:
        """Calculate elapsed time since measurement start (button press)"""
        with self._lock:
            if not self._state["measurement_active"] or self._state["button_press_time"] is None:
                return 0
            return int(time.time() - self._state["button_press_time"])

    def is_measurement_time_elapsed(self) -> bool:
        """
        Check if the defined measurement time has elapsed or exact sample count reached.
        """
        with self._lock:
            if not self._state["measurement_active"] or self._state["button_press_time"] is None:
                return False
                
            # Check if we've reached the exact sample count
            expected_samples = int(self._state["defined_measurement_time"] * Config.VIRTUAL_SAMPLE_RATE)
            current_samples = self._state["virtual_sample_count"]
            
            if current_samples >= expected_samples:
                logger.info(f"⏱️ Exact sample count reached: {current_samples}/{expected_samples}")
                return True
                
            # Also check time as a fallback
            elapsed = time.time() - self._state["button_press_time"]
            defined_time = self._state["defined_measurement_time"]
            if elapsed >= defined_time:
                logger.info(f"⏱️ Measurement time elapsed: {elapsed:.2f}s/{defined_time}s")
                return True
                
            return False

    def update(self, **kwargs) -> None:
        """Thread-safe update of multiple state attributes"""
        with self._lock:
            self._state.update(kwargs)
            
            # Cap runtime at defined time if active measurement
            if (self._state["measurement_active"] and 
                self._state["button_press_time"] is not None and 
                "runtime" in self._state):
                
                defined_time = self._state["defined_measurement_time"]
                if self._state["runtime"] > defined_time:
                    self._state["runtime"] = defined_time
                    
            # 💡 Hardened: Record state transition
            self._record_state_transition()
            
            # 💡 Hardened: Perform periodic health check
            now = time.time()
            if now - self._health_check_time > 5.0:  # Every 5 seconds
                self._update_health_metrics()
                self._health_check_time = now

    # 💡 Hardened: System health monitoring
    def _update_health_metrics(self):
        """Update system resource metrics"""
        try:
            # Update uptime
            self._state["system_uptime"] = time.time() - system_startup_time
            
            # CPU and memory usage
            self._state["cpu_usage"] = psutil.cpu_percent()
            self._state["mem_usage"] = psutil.virtual_memory().percent
            
            # Disk space
            disk_usage = psutil.disk_usage('/')
            self._state["disk_free_mb"] = disk_usage.free / (1024 * 1024)
            
            # Count active threads
            self._state["threads_active"] = threading.active_count()
            
            # Check if resources are healthy
            is_healthy = (
                self._state["disk_free_mb"] > Config.MIN_FREE_DISK_SPACE_MB and
                self._state["cpu_usage"] < Config.MAX_CPU_USAGE_PCT and
                global_system_healthy
            )
            
            # Update health status
            self._state["healthy"] = is_healthy
            
            # Log health status if unhealthy
            if not is_healthy:
                logger.warning(f"⚠️ System health check failed: " +
                               f"CPU={self._state['cpu_usage']:.1f}%, " +
                               f"Mem={self._state['mem_usage']:.1f}%, " +
                               f"Disk={self._state['disk_free_mb']:.1f}MB, " +
                               f"Global={global_system_healthy}")
        
        except Exception as e:
            logger.error(f"❌ Error updating health metrics: {e}")

    def get(self, key: str) -> Any:
        """Thread-safe state access"""
        with self._lock:
            # 💡 Hardened: Handle missing keys gracefully
            if key not in self._state:
                logger.warning(f"⚠️ Attempted to access missing state key: {key}")
                return None
            return self._state.get(key)

    def set(self, key: str, value: Any) -> None:
        """Thread-safe state update"""
        with self._lock:
            # 💡 Hardened: Validate key types to catch programming errors
            if key == "q628_state" and not isinstance(value, Q628State):
                logger.error(f"❌ Invalid Q628State value: {value}")
                return
            elif key == "motor_position" and not isinstance(value, MotorPosition):
                logger.error(f"❌ Invalid MotorPosition value: {value}")
                return
            elif key == "valve_status" and not isinstance(value, ValveStatus):
                logger.error(f"❌ Invalid ValveStatus value: {value}")
                return
                
            self._state[key] = value
            
            # 💡 Hardened: Record error time if last_error is updated
            if key == "last_error":
                self._state["last_error_time"] = time.time()
                self._state["error_count"] += 1
                
            # 💡 Hardened: Record state transition
            self._record_state_transition()

    def is_measurement_active(self) -> bool:
        """Check if measurement is active"""
        with self._lock:
            return self._state["measurement_active"]

    def is_sampling_active(self) -> bool:
        """Check if sampling is active"""
        with self._lock:
            return self._state["sampling_active"]

    def set_button_press_time(self) -> None:
        """Set button press time to current time"""
        with self._lock:
            self._state["button_press_time"] = time.time()
            logger.info(f"🕒 Button press time set to: {self._state['button_press_time']}")

    def start_sampling(self) -> None:
        """Enable sampling flag"""
        with self._lock:
            self._state["sampling_active"] = True
            self._state["virtual_sample_count"] = 0  # Reset virtual sample count
            logger.info("📊 Sampling activated")

    def stop_sampling(self) -> None:
        """Disable sampling flag"""
        with self._lock:
            self._state["sampling_active"] = False
            logger.info("📊 Sampling deactivated")

    def increment_virtual_sample_count(self, count: int = 1) -> int:
        """Increment virtual sample counter and return new value"""
        with self._lock:
            self._state["virtual_sample_count"] += count
            return self._state["virtual_sample_count"]

    def check_sample_count_limit(self) -> bool:
        """
        Check if we've reached the exact sample count limit for this measurement.
        Returns True if measurement should be stopped.
        """
        with self._lock:
            if not self._state["measurement_active"]:
                return False
                
            # Get expected sample count for this measurement duration
            expected_count = self._state["total_expected_samples"]
            
            # Get current sample count
            current_count = self._state["virtual_sample_count"]
            
            # If we've reached or exceeded the expected count, stop the measurement
            if current_count >= expected_count:
                logger.info(f"⏱️ Reached exact virtual sample count limit: {current_count}/{expected_count}")
                return True
                
            # CRITICAL: Allow measurement to continue even if time is up, until we reach the target
            # We only terminate when we hit the exact sample count
            return False

    def start_measurement(self) -> bool:
        """Start measurement with explicit sample count control"""
        with self._lock:
            if self._state["measurement_active"]:
                return False
            
            self._state["measurement_active"] = True
            self._state["measurement_start_time"] = time.time()
            self._state["runtime"] = 0
            defined_time = self._state["defined_measurement_time"]
            
            # Calculate the exact number of samples that should be collected at 100Hz
            self._state["total_expected_samples"] = int(defined_time * Config.VIRTUAL_SAMPLE_RATE)
            self._state["virtual_sample_count"] = 0
            
            # Clear buffers at start of measurement
            adc_buffer.clear()
            virtual_buffer.clear()
            
            # Reset sent packet counter for CSV logging
            global sent_packet_counter
            sent_packet_counter = 0
            
            # Start runtime update timer
            self._start_runtime_timer()
            
            logger.info(f"🕒 Measurement started with target of exactly {self._state['total_expected_samples']} " +
                      f"virtual samples over {defined_time}s")
            return True

    def _start_runtime_timer(self) -> None:
        """Start timer to update runtime field with respect to configured time limit"""
        if self._runtime_timer is not None:
            return

        def update_runtime():
            try:  # 💡 Hardened: Exception handling for timer thread
                while self._state["measurement_active"]:
                    with self._lock:
                        if self._state["button_press_time"] is not None:
                            elapsed = time.time() - self._state["button_press_time"]
                            defined_time = self._state["defined_measurement_time"]
                            
                            # Cap runtime display at defined time, but don't terminate measurement
                            self._state["runtime"] = min(int(elapsed), defined_time)
                            
                            # ONLY check sample count, not time
                            expected_samples = int(defined_time * Config.VIRTUAL_SAMPLE_RATE)
                            current_samples = self._state["virtual_sample_count"]
                            
                            if (current_samples >= expected_samples and 
                                self._state["q628_state"] == Q628State.ACTIV):
                                logger.info(f"⏱️ Runtime timer detected exact virtual sample count reached ({current_samples}/{expected_samples})")
                                self._state["q628_state"] = Q628State.FINISH
                                self._state["q628_timer"] = time.time()
                                
                                # Stop sampling
                                sampling_active.clear()
                                self._state["sampling_active"] = False
                    
                    # Check frequently for precise timing
                    time.sleep(0.01)
            except Exception as e:
                # 💡 Hardened: Error handling in runtime timer thread
                logger.error(f"❌ Runtime timer thread error: {e}")
                # Try to restart the timer if it crashes
                if self._state["measurement_active"]:
                    logger.info("🔄 Attempting to restart runtime timer thread")
                    time.sleep(1.0)
                    threading.Thread(target=update_runtime, daemon=True).start()
                    return

        self._runtime_timer = threading.Thread(target=update_runtime, daemon=True)
        self._runtime_timer.name = "RuntimeTimer"  # 💡 Hardened: Named thread for debugging
        self._runtime_timer.start()

    def end_measurement(self) -> None:
        """End measurement and finalize CSV logging"""
        global csv_file_handle
        
        with self._lock:
            # Skip if measurement is already inactive
            if not self._state["measurement_active"]:
                return
                
            self._state["measurement_active"] = False
            self._state["measurement_start_time"] = None
            self._state["button_press_time"] = None
            self._state["runtime"] = 0
            logger.info("🕒 Measurement ended")
            
            # 💡 Hardened: Create a backup copy of the CSV file at end of measurement
            if Config.CSV_ENABLED and 'csv_file_handle' in globals() and csv_file_handle is not None:
                try:
                    # First flush to ensure all data is written
                    csv_file_handle.flush()
                    
                    # Ensure backup directory exists
                    if not os.path.exists(Config.CSV_BACKUP_DIR):
                        os.makedirs(Config.CSV_BACKUP_DIR)
                    
                    # Create timestamped backup filename
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_filename = f"{Config.CSV_BACKUP_DIR}/delphi_data_{timestamp}.csv"
                    
                    # Copy the file
                    with open(Config.CSV_FILENAME, 'rb') as src_file:
                        file_contents = src_file.read()
                        file_checksum = hashlib.md5(file_contents).hexdigest()
                        with open(backup_filename, 'wb') as dst_file:
                            dst_file.write(file_contents)
                    
                    logger.info(f"📊 Created backup CSV file: {backup_filename} (MD5: {file_checksum})")
                except Exception as e:
                    logger.error(f"❌ Error creating CSV backup: {e}")

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
                
                # 💡 Hardened: Log extended info about state transition
                if new_state == Q628State.RECOVERY:
                    self._state["recovery_attempts"] += 1
                    self._state["last_recovery_time"] = time.time()
                    logger.warning(f"⚠️ Entering RECOVERY state (attempt #{self._state['recovery_attempts']})")

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
            # 💡 Hardened: Validate ADC value range
            if value < -2100000 or value > 2100000:  # Typical MCP3553 range
                logger.warning(f"⚠️ ADC value {value} outside of expected range")
                # Still record but cap the value
                value = max(-2100000, min(value, 2100000))
            
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
        """This method is deprecated but kept for backwards compatibility"""
        logger.warning("⚠️ get_profile_delay() called but profiles are disabled in new process flow")
        return Config.FIXED_INITIALIZATION_TIME
    
    # 💡 Hardened: Added method to get state history for debugging
    def get_state_history(self) -> List[Dict[str, Any]]:
        """Get a copy of recent state history for debugging"""
        with self._lock:
            return list(self._state_history)
    
    # 💡 Hardened: Added method to record hardware errors
    def record_hardware_error(self, error_type: str, error_msg: str) -> None:
        """Record a hardware error by type"""
        with self._lock:
            if error_type == "i2c":
                self._state["i2c_error_count"] += 1
            elif error_type == "adc":
                self._state["adc_error_count"] += 1
            elif error_type == "dac":
                self._state["dac_error_count"] += 1
            elif error_type == "network":
                self._state["network_error_count"] += 1
                
            self._state["last_error"] = f"{error_type.upper()}: {error_msg}"
            self._state["last_error_time"] = time.time()
            self._state["error_count"] += 1
            
            logger.error(f"❌ Hardware error ({error_type}): {error_msg}")
    
    # 💡 Hardened: Added method to check system health
    def check_system_health(self) -> bool:
        """Check if the system is healthy based on various metrics"""
        with self._lock:
            # Update metrics first
            self._update_health_metrics()
            
            # Check for disk space issues
            if self._state["disk_free_mb"] < Config.MIN_FREE_DISK_SPACE_MB:
                logger.error(f"❌ Disk space critically low: {self._state['disk_free_mb']:.1f}MB free")
                return False
                
            # Check CPU usage
            if self._state["cpu_usage"] > Config.MAX_CPU_USAGE_PCT:
                logger.error(f"❌ CPU usage too high: {self._state['cpu_usage']:.1f}%")
                return False
                
            # Check for hardware error count thresholds
            if (self._state["i2c_error_count"] > 50 or
                self._state["adc_error_count"] > 50 or
                self._state["dac_error_count"] > 50):
                logger.error(f"❌ Too many hardware errors: " +
                           f"I2C={self._state['i2c_error_count']}, " +
                           f"ADC={self._state['adc_error_count']}, " +
                           f"DAC={self._state['dac_error_count']}")
                return False
                
            return True


# ===== Global Variables =====
system_state = SystemState()
measurement_start_time = 0
i2c_bus = None  # Will be initialized in hardware setup
adc_buffer = RingBuffer(Config.ADC_BUFFER_SIZE)  # For raw ADC samples
virtual_buffer = RingBuffer(Config.VIRTUAL_BUFFER_SIZE)  # For 100Hz virtual samples

# Scale factor for better visualization
SCALE_FACTOR = Config.SCALE_FACTOR

# Tracking variables
last_sent_index = 0  # Global tracker for last sent buffer position
sent_packet_counter = 0  # Counter for packets sent to Delphi

# CSV handling
csv_file_handle = None
# 💡 Hardened: Add CSV file lock
csv_file_lock = threading.Lock()

# Synchronization primitives
i2c_lock = threading.Lock()
adc_lock = threading.Lock()
dac_lock = threading.Lock()
shutdown_requested = threading.Event()
enable_from_pc = threading.Event()
simulate_start = threading.Event()
sampling_active = threading.Event()
# 💡 Hardened: Add recovery synchronization
recovery_in_progress = threading.Event()
hardware_init_complete = threading.Event()

# Setup logging
logger = logging.getLogger("qumat628")

# 💡 Hardened: Thread tracking for monitoring
active_threads = {}
thread_lock = threading.Lock()

# 💡 Hardened: Function to register a thread for monitoring
def register_thread(thread_name: str, thread: threading.Thread) -> None:
    """Register a thread for health monitoring"""
    with thread_lock:
        active_threads[thread_name] = {
            "thread": thread,
            "start_time": time.time(),
            "last_heartbeat": time.time(),
            "healthy": True
        }
    logger.debug(f"💡 Registered thread: {thread_name}")

# 💡 Hardened: Function to report thread heartbeat
def thread_heartbeat(thread_name: str) -> None:
    """Report that a thread is still alive"""
    with thread_lock:
        if thread_name in active_threads:
            active_threads[thread_name]["last_heartbeat"] = time.time()
            active_threads[thread_name]["healthy"] = True

# 💡 Hardened: Function to check thread health
def check_thread_health() -> Dict[str, Any]:
    """Check health of all registered threads"""
    unhealthy_threads = []
    all_threads = {}
    
    current_time = time.time()
    with thread_lock:
        for name, info in active_threads.items():
            thread = info["thread"]
            last_heartbeat = info["last_heartbeat"]
            
            # Thread is unhealthy if no heartbeat in 10 seconds and still alive
            if current_time - last_heartbeat > 10 and thread.is_alive():
                info["healthy"] = False
                unhealthy_threads.append(name)
            
            all_threads[name] = {
                "alive": thread.is_alive(),
                "healthy": info["healthy"],
                "uptime": current_time - info["start_time"],
                "last_heartbeat_age": current_time - last_heartbeat
            }
    
    return {
        "healthy": len(unhealthy_threads) == 0,
        "unhealthy_threads": unhealthy_threads,
        "all_threads": all_threads
    }

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

    # 💡 Hardened: Create log directory if it doesn't exist
    log_dir = os.path.dirname(Config.LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except Exception as e:
            print(f"Warning: Could not create log directory: {e}")

    # File handler with rotation
    try:
        # 💡 Hardened: Use rotating file handler to prevent log file from growing too large
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            Config.LOG_FILE, 
            mode='a',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            delay=False
        )
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

# 💡 Hardened: watchdog timer function
def system_watchdog():
    """
    Watchdog timer to detect and recover from system hangs.
    """
    global global_system_healthy, watchdog_last_tick
    
    logger.info("🔍 System watchdog started")
    
    last_check_time = time.time()
    recovery_counter = 0
    
    while not shutdown_requested.is_set():
        try:
            # Get current time
            current_time = time.time()
            
            # Check if main operations are responsive
            main_healthy = (current_time - last_successful_operation) < Config.WATCHDOG_TIMEOUT
            sampling_healthy = (current_time - last_successful_sampling) < Config.WATCHDOG_TIMEOUT
            
            # Update watchdog timestamp
            watchdog_last_tick = current_time
            
            # Check thread health
            thread_health = check_thread_health()
            
            # Combined health check
            system_healthy = (
                main_healthy and 
                sampling_healthy and 
                thread_health["healthy"] and
                not recovery_in_progress.is_set()
            )
            
            # Update global health status
            global_system_healthy = system_healthy
            
            # If system is unhealthy and not in recovery, attempt recovery
            if not system_healthy and not recovery_in_progress.is_set():
                recovery_counter += 1
                logger.warning(f"⚠️ System unhealthy: recovery attempt #{recovery_counter}")
                
                # Check if we've exceeded max recovery attempts
                if recovery_counter <= Config.MAX_RECOVERY_ATTEMPTS:
                    # Start recovery process
                    recovery_in_progress.set()
                    try:
                        # Basic recovery: restart ADC thread if needed
                        if not sampling_healthy:
                            logger.error("❌ Sampling thread unresponsive, restarting...")
                            # Create a new ADC sampling thread
                            adc_thread = threading.Thread(
                                target=adc_sampling_thread,
                                daemon=True,
                                name="ADCSampling-Recovery"
                            )
                            adc_thread.start()
                            register_thread("ADCSampling-Recovery", adc_thread)
                        
                                                # Check for unhealthy threads and restart them
                        for thread_name in thread_health["unhealthy_threads"]:
                            logger.error(f"❌ Thread {thread_name} unresponsive, attempting restart")
                            
                            # Special handling based on thread type
                            if "ADCSampling" in thread_name:
                                new_thread = threading.Thread(
                                    target=adc_sampling_thread,
                                    daemon=True,
                                    name=f"{thread_name}-Recovery"
                                )
                                new_thread.start()
                                register_thread(f"{thread_name}-Recovery", new_thread)
                            
                            elif "MotorStatus" in thread_name:
                                new_thread = threading.Thread(
                                    target=motor_status_monitor,
                                    daemon=True,
                                    name=f"{thread_name}-Recovery"
                                )
                                new_thread.start()
                                register_thread(f"{thread_name}-Recovery", new_thread)
                            
                            elif "TempHumidity" in thread_name:
                                new_thread = threading.Thread(
                                    target=temperature_humidity_monitor,
                                    daemon=True,
                                    name=f"{thread_name}-Recovery"
                                )
                                new_thread.start()
                                register_thread(f"{thread_name}-Recovery", new_thread)
                                
                        # If system was in ACTIV state and froze, try to gracefully end measurement
                        if system_state.get_q628_state() == Q628State.ACTIV:
                            logger.warning("⚠️ System froze during active measurement, trying to end gracefully")
                            system_state.end_measurement()
                            stop_sampling()
                            system_state.set_q628_state(Q628State.IDLE)
                            
                    except Exception as e:
                        logger.error(f"❌ Error during recovery attempt: {e}")
                    finally:
                        recovery_in_progress.clear()
                else:
                    logger.error(f"❌ Max recovery attempts ({Config.MAX_RECOVERY_ATTEMPTS}) reached, system needs manual intervention")
                    # Continue monitoring but stop attempting recovery
            else:
                # System healthy, reset recovery counter
                recovery_counter = max(0, recovery_counter - 1)  # Decay counter gradually
            
            # Periodically log system status
            if current_time - last_check_time >= 60:  # Every minute
                uptime_hours = (current_time - system_startup_time) / 3600
                logger.info(f"🔍 Watchdog health check: Main={main_healthy}, Sampling={sampling_healthy}, " + 
                           f"Threads={thread_health['healthy']}, Uptime={uptime_hours:.2f}h")
                last_check_time = current_time
                
                # 💡 Hardened: Log memory usage periodically
                try:
                    memory_info = psutil.virtual_memory()
                    cpu_percent = psutil.cpu_percent(interval=0.1)
                    logger.info(f"🖥️ System resources: CPU={cpu_percent}%, RAM={memory_info.percent}% " +
                               f"({memory_info.used/1024/1024:.1f}MB/{memory_info.total/1024/1024:.1f}MB)")
                except Exception as e:
                    logger.warning(f"⚠️ Could not get system resources: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error in watchdog thread: {e}")
        
        # Sleep until next check
        time.sleep(Config.WATCHDOG_INTERVAL)
        
    logger.info("🔍 System watchdog stopped")

# ===== CSV Logging Functions =====
def initialize_csv_file() -> bool:
    """
    Initialize the single CSV file for logging all Delphi data.
    """
    global csv_file_handle
    
    try:
        with csv_file_lock:  # 💡 Hardened: Thread-safe file operations
            # Close any existing file handle
            if csv_file_handle is not None:
                try:
                    csv_file_handle.flush()
                    csv_file_handle.close()
                    csv_file_handle = None
                except Exception as e:
                    logger.error(f"❌ Error closing previous CSV file: {e}")
            
            # 💡 Hardened: Ensure backup directory exists
            if not os.path.exists(Config.CSV_BACKUP_DIR):
                try:
                    os.makedirs(Config.CSV_BACKUP_DIR)
                except Exception as e:
                    logger.error(f"❌ Could not create backup directory: {e}")
        
            # 💡 Hardened: Check if CSV file exists and create backup before opening
            if os.path.exists(Config.CSV_FILENAME) and os.path.getsize(Config.CSV_FILENAME) > 0:
                try:
                    # Create timestamped backup before opening
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_filename = f"{Config.CSV_BACKUP_DIR}/delphi_data_backup_{timestamp}.csv"
                    with open(Config.CSV_FILENAME, 'rb') as src_file:
                        with open(backup_filename, 'wb') as dst_file:
                            dst_file.write(src_file.read())
                    logger.info(f"💡 Created backup of existing CSV file: {backup_filename}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not create CSV backup: {e}")
            
            # Open CSV file in append mode
            csv_file_handle = open(Config.CSV_FILENAME, 'a', newline='')
            
            # 💡 Hardened: Lock file to prevent other processes from accessing it
            try:
                fcntl.flock(csv_file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug("💡 Acquired exclusive lock on CSV file")
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    logger.warning("⚠️ CSV file is locked by another process, waiting...")
                    # Wait for lock
                    try:
                        fcntl.flock(csv_file_handle.fileno(), fcntl.LOCK_EX)
                        logger.info("✅ Acquired CSV file lock after waiting")
                    except Exception as e2:
                        logger.error(f"❌ Failed to acquire CSV file lock: {e2}")
                else:
                    logger.error(f"❌ Error locking CSV file: {e}")
            
            writer = csv.writer(csv_file_handle)
            
            # If file is new (empty), write headers
            if os.path.getsize(Config.CSV_FILENAME) == 0:
                writer.writerow([
                    'Timestamp', 'DateTime', 'PacketID', 'MeasurementID',
                    'SampleIndex', 'RawValue', 'SentValue', 'Voltage',
                    'ScaledValue', 'VirtualIndex', 'Runtime', 'HV_Status', 
                    'HV_DC', 'HV_AC', 'DAC_Voltage', 'Temperature', 'Humidity', 
                    'MeasurementActive', 'Q628State', 'MotorPosition', 'ValveStatus',
                    'Checksum'  # 💡 Hardened: Added checksum for data integrity
                ])
            
            csv_file_handle.flush()
            os.fsync(csv_file_handle.fileno())  # 💡 Hardened: Force write to disk
            logger.info(f"📊 Initialized CSV log file: {Config.CSV_FILENAME}")
            return True
        
    except Exception as e:
        logger.error(f"❌ Error initializing CSV file: {e}")
        
        # 💡 Hardened: Recovery attempt if file is corrupted
        try:
            # Check if file exists but is corrupted
            if os.path.exists(Config.CSV_FILENAME):
                # Try to create a backup of the corrupted file
                corrupt_backup = f"{Config.CSV_BACKUP_DIR}/corrupted_{int(time.time())}.csv"
                os.rename(Config.CSV_FILENAME, corrupt_backup)
                logger.warning(f"⚠️ Moved potentially corrupted CSV file to {corrupt_backup}")
                
                # Try to create a fresh file
                with open(Config.CSV_FILENAME, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Timestamp', 'DateTime', 'PacketID', 'MeasurementID',
                        'SampleIndex', 'RawValue', 'SentValue', 'Voltage',
                        'ScaledValue', 'VirtualIndex', 'Runtime', 'HV_Status', 
                        'HV_DC', 'HV_AC', 'DAC_Voltage', 'Temperature', 'Humidity', 
                        'MeasurementActive', 'Q628State', 'MotorPosition', 'ValveStatus',
                        'Checksum'
                    ])
                
                # Try to open in append mode again
                csv_file_handle = open(Config.CSV_FILENAME, 'a', newline='')
                logger.info("✅ Successfully recovered from CSV file corruption")
                return True
        except Exception as recovery_error:
            logger.error(f"❌ CSV recovery failed: {recovery_error}")
        
        return False

# 💡 Hardened: Function to safely flush CSV file
def safe_flush_csv():
    """Safely flush CSV file to disk with error handling"""
    global csv_file_handle
    
    if not Config.CSV_ENABLED or csv_file_handle is None:
        return
    
    try:
        with csv_file_lock:
            csv_file_handle.flush()
            os.fsync(csv_file_handle.fileno())  # Force write to physical storage
    except Exception as e:
        logger.error(f"❌ Error flushing CSV file: {e}")
        
        # Try to recover the CSV file
        try:
            logger.warning("⚠️ Attempting to recover CSV file after flush error")
            initialize_csv_file()
        except:
            pass
    
def log_samples_to_csv(samples: list, packet_id: int, start_index: int) -> None:
    """
    Log samples sent to Delphi to the CSV file.
    
    Args:
        samples: List of tuples (raw_value, sent_value, is_virtual, virtual_index)
        packet_id: Unique ID for this packet
        start_index: Starting index in the measurement
    """
    global csv_file_handle
    
    if not Config.CSV_ENABLED or not samples:
        return
    
    # 💡 Hardened: Initialize file if needed before acquiring lock
    if csv_file_handle is None:
        if not initialize_csv_file():
            return
    
    try:
        with csv_file_lock:  # 💡 Hardened: Thread-safe CSV access
            if csv_file_handle is None:
                logger.error("❌ CSV file handle is None despite initialization check")
                return
                
            state_dict = system_state.get_status_dict()
            current_time = time.time()
            dt_str = datetime.datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # Generate a unique measurement ID based on start time
            measurement_id = f"{state_dict['measurement_start_time']}" if state_dict["measurement_start_time"] else "IDLE"
            
            # Calculate DAC voltage from HV DC value
            dac_voltage = state_dict["hv_dc_value"] / 2000.0
            
            writer = csv.writer(csv_file_handle)
            
            rows_written = 0
            checksum_errors = 0
            
            for i, (raw_value, sent_value, is_virtual, virtual_idx) in enumerate(samples):
                try:
                    # Calculate voltage from raw ADC value
                    voltage = (float(raw_value) / ((1 << 21) - 1)) * Config.VREF_ADC * 2
                    
                    # Calculate absolute sample index
                    abs_index = start_index + i
                    
                    # Calculate scaled value for comparison with sent value
                    scaled_value = raw_value * SCALE_FACTOR
                    
                    # 💡 Hardened: Create checksum for data integrity verification 
                    row_data = [
                        f"{current_time:.6f}",                 # Timestamp
                        dt_str,                                # Human-readable datetime
                        packet_id,                             # Packet ID
                        measurement_id,                        # Measurement ID
                        abs_index,                             # Sample index in measurement
                        raw_value,                             # Raw 22-bit ADC value
                        sent_value,                            # Value sent to Delphi (scaled)
                        f"{voltage:.6f}",                      # Calculated voltage
                        scaled_value,                          # Scaled value for debugging
                        virtual_idx,                           # Virtual sample index (100Hz grid)
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
                    ]
                    
                    # Calculate checksum from row data
                    checksum = hashlib.md5(str(row_data).encode()).hexdigest()[:8]
                    row_data.append(checksum)
                    
                    # Write the row with checksum
                    writer.writerow(row_data)
                    rows_written += 1
                    
                except Exception as row_error:
                    logger.error(f"❌ Error writing CSV row: {row_error}")
                    checksum_errors += 1
            
            # 💡 Hardened: Flush periodically but not on every packet
            if packet_id % 10 == 0:
                try:
                    csv_file_handle.flush()
                except Exception as e:
                    logger.error(f"❌ Error flushing CSV file: {e}")
            
            # Log status periodically to avoid flooding logs
            if packet_id % 10 == 0:
                if checksum_errors > 0:
                    logger.warning(f"⚠️ CSV logging had {checksum_errors} errors out of {len(samples)} rows")
                else:
                    logger.debug(f"📊 Logged {rows_written} samples to CSV (packet {packet_id}, indices {start_index}-{start_index+len(samples)-1})")
            
    except Exception as e:
        logger.error(f"❌ Error logging samples to CSV: {e}")
        
        # 💡 Hardened: Try to recover from CSV file error
        try:
            # Close existing file handle if it exists
            if csv_file_handle is not None:
                csv_file_handle.close()
                csv_file_handle = None
            
            # Try to reinitialize CSV file
            initialize_csv_file()
        except Exception as recovery_error:
            logger.error(f"❌ CSV recovery failed: {recovery_error}")

# 💡 Hardened: CSV file periodic flush thread
def csv_flush_thread():
    """Thread that periodically flushes the CSV file to disk"""
    logger.info("📊 CSV periodic flush thread started")
    
    while not shutdown_requested.is_set():
        try:
            # Sleep first to avoid immediate flush
            for _ in range(int(Config.CSV_FLUSH_INTERVAL)):
                if shutdown_requested.is_set():
                    break
                time.sleep(1)
            
            # Flush CSV if it exists
            if csv_file_handle is not None:
                safe_flush_csv()
                
        except Exception as e:
            logger.error(f"❌ Error in CSV flush thread: {e}")
            time.sleep(5)  # Longer sleep on error
    
    logger.info("📊 CSV flush thread terminated")

# 💡 Hardened: Function to check system resource health
def check_system_resources() -> Dict[str, Any]:
    """Check system resources and return status"""
    try:
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        # Get memory usage
        memory = psutil.virtual_memory()
        
        # Get disk usage
        disk = psutil.disk_usage('/')
        disk_free_mb = disk.free / (1024 * 1024)
        
        # Check if resources are healthy
        cpu_healthy = cpu_percent < Config.MAX_CPU_USAGE_PCT
        mem_healthy = memory.percent < 90  # 90% memory usage threshold
        disk_healthy = disk_free_mb > Config.MIN_FREE_DISK_SPACE_MB
        
        return {
            "healthy": cpu_healthy and mem_healthy and disk_healthy,
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "disk_free_mb": disk_free_mb,
            "cpu_healthy": cpu_healthy,
            "memory_healthy": mem_healthy,
            "disk_healthy": disk_healthy
        }
    except Exception as e:
        logger.error(f"❌ Error checking system resources: {e}")
        return {
            "healthy": False,
            "error": str(e)
        }

def initialize_hardware() -> bool:
    """Initialize hardware with comprehensive error checking"""
    logger.info("🔌 Initializing hardware...")
    
    # 💡 Hardened: Track initialization status for each component
    initialization_status = {
        "gpio": False,
        "i2c": False,
        "adc": False,
        "dac": False,
        "motor": False,
        "temp_sensor": False
    }
    
    try:
        global i2c_bus
        
        # 💡 Hardened: Retry I2C initialization
        i2c_retry_count = 0
        while i2c_retry_count < Config.MAX_I2C_RETRIES:
            try:
                i2c_bus = smbus2.SMBus(1)
                initialization_status["i2c"] = True
                break
            except Exception as e:
                i2c_retry_count += 1
                logger.warning(f"⚠️ I2C initialization attempt {i2c_retry_count} failed: {e}")
                time.sleep(1.0)
        
        if not initialization_status["i2c"]:
            logger.error("❌ Could not initialize I2C bus after multiple attempts")
            # Continue anyway since some functionality might still work

        # Configure GPIO
        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)

        # Setup output pins
        output_pins = [
            Config.PIN_CS_ADC, Config.PIN_CLK_ADC, Config.PIN_DAT_DAC,
            Config.PIN_CLK_DAC, Config.PIN_CS_DAC, Config.MAGNET1_GPIO,
            Config.MAGNET2_GPIO, Config.START_LED_GPIO, Config.HVAC_GPIO
        ]

        # 💡 Hardened: Pin initialization with verification
        gpio_failures = []
        for pin in output_pins:
            try:
                GPIO.setup(pin, GPIO.OUT)
                GPIO.output(pin, GPIO.LOW)  # Initialize all outputs to LOW
                
                # Verify pin is set as output and is LOW
                if GPIO.gpio_function(pin) != GPIO.OUT:
                    gpio_failures.append(pin)
                    logger.warning(f"⚠️ Failed to set pin {pin} as output")
            except Exception as e:
                gpio_failures.append(pin)
                logger.error(f"❌ Error initializing GPIO pin {pin}: {e}")
        
        # Log GPIO initialization results
        if not gpio_failures:
            initialization_status["gpio"] = True
            logger.info("✅ Output GPIO pins initialized successfully")
        else:
            logger.error(f"❌ Failed to initialize GPIO pins: {gpio_failures}")

        # 💡 Hardened: Input pin initialization with verification
        input_success = True
        try:
            GPIO.setup(Config.PIN_SDO_ADC, GPIO.IN, pull_up_down=GPIO.PUD_UP)
            if GPIO.gpio_function(Config.PIN_SDO_ADC) != GPIO.IN:
                input_success = False
                logger.warning(f"⚠️ Failed to set ADC SDO pin {Config.PIN_SDO_ADC} as input")
        except Exception as e:
            input_success = False
            logger.error(f"❌ Error initializing ADC SDO pin: {e}")
        
        try:
            GPIO.setup(Config.START_TASTE_GPIO, GPIO.IN, pull_up_down=GPIO.PUD_UP)
            if GPIO.gpio_function(Config.START_TASTE_GPIO) != GPIO.IN:
                input_success = False
                logger.warning(f"⚠️ Failed to set button pin {Config.START_TASTE_GPIO} as input")
        except Exception as e:
            input_success = False
            logger.error(f"❌ Error initializing button pin: {e}")
        
        if input_success:
            logger.info("✅ Input GPIO pins initialized successfully")

        # 💡 Hardened: ADC initialization with retry
        adc_init_success = False
        for retry in range(3):
            try:
                # Set initial pin states for SPI-like interfaces
                GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)  # Chip select inactive
                GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
                time.sleep(0.01)
                GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
                time.sleep(0.02)
                
                # Check if ADC is responding by attempting a read
                test_value = read_adc_22bit()
                if test_value is not None:  # Any value is acceptable for initialization test
                    adc_init_success = True
                    initialization_status["adc"] = True
                    logger.info(f"✅ ADC initialized with soft reset (test value: {test_value})")
                    break
            except Exception as e:
                logger.warning(f"⚠️ ADC initialization attempt {retry+1} failed: {e}")
                time.sleep(0.5)  # Wait before retrying
        
        if not adc_init_success:
            logger.error("❌ Failed to initialize ADC after multiple attempts")

        # 💡 Hardened: DAC initialization with retry
        dac_init_success = False
        for retry in range(3):
            try:
                # Initial state for DAC - ensure it's set to 0V
                GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Chip select inactive
                GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
                GPIO.output(Config.PIN_DAT_DAC, GPIO.LOW)
                time.sleep(0.01)
                
                # Try setting DAC to 0V
                set_dac_voltage(0.0)
                dac_init_success = True
                initialization_status["dac"] = True
                logger.info("✅ DAC initialized to 0V")
                break
            except Exception as e:
                logger.warning(f"⚠️ DAC initialization attempt {retry+1} failed: {e}")
                time.sleep(0.5)  # Wait before retrying
        
        if not dac_init_success:
            logger.error("❌ Failed to initialize DAC after multiple attempts")
        
        # 💡 Hardened: Test I2C connection to motor controller with retry
        motor_i2c_success = False
        for retry in range(3):
            try:
                with i2c_lock:
                    with SMBus(1) as bus:
                        read_test = i2c_msg.read(Config.I2C_ADDR_MOTOR, 1)
                        bus.i2c_rdwr(read_test)
                motor_i2c_success = True
                initialization_status["motor"] = True
                logger.info("✅ Motor I2C connection successfully tested")

                # Read initial motor status
                motor_status = read_motor_status()
                if motor_status:
                    logger.info("✅ Initial motor status: %s", motor_status)
                break
            except Exception as e:
                logger.warning(f"⚠️ Motor I2C test attempt {retry+1} failed: {e}")
                time.sleep(0.5)  # Wait before retrying
        
        if not motor_i2c_success:
            logger.error("❌ Motor I2C connection test failed after multiple attempts")

        # 💡 Hardened: Test I2C connection to humidity/temperature sensor with retry
        temp_sensor_success = False
        for retry in range(3):
            try:
                with i2c_lock:
                    with SMBus(1) as bus:
                        read_test = i2c_msg.read(Config.HYT_ADDR, 1)
                        bus.i2c_rdwr(read_test)
                temp_sensor_success = True
                initialization_status["temp_sensor"] = True
                logger.info("✅ HYT sensor I2C connection successfully tested")
                break
            except Exception as e:
                logger.warning(f"⚠️ HYT sensor I2C test attempt {retry+1} failed: {e}")
                time.sleep(0.5)  # Wait before retrying
        
        if not temp_sensor_success:
            logger.warning("⚠️ HYT sensor I2C connection test failed after multiple attempts")

        # 💡 Hardened: Start threads with proper naming and monitoring
        # Start motor status monitoring thread
        motor_thread = threading.Thread(
            target=motor_status_monitor,
            daemon=True,
            name="MotorStatusMonitor"
        )
        motor_thread.start()
        register_thread("MotorStatusMonitor", motor_thread)
        logger.info("✅ Motor status monitoring started")

        # Start 50Hz ADC sampling thread
        adc_thread = threading.Thread(
            target=adc_sampling_thread,
            daemon=True,
            name="ADCSampling"
        )
        adc_thread.start()
        register_thread("ADCSampling", adc_thread)
        logger.info("✅ 50Hz ADC sampling started")

        # Start temperature/humidity monitoring
        temp_thread = threading.Thread(
            target=temperature_humidity_monitor,
            daemon=True,
            name="TempHumidityMonitor"
        )
        temp_thread.start()
        register_thread("TempHumidityMonitor", temp_thread)
        logger.info("✅ Temperature and humidity monitoring started")

        # Start Q628 state machine thread
        state_thread = threading.Thread(
            target=q628_state_machine,
            daemon=True,
            name="Q628StateMachine"
        )
        state_thread.start()
        register_thread("Q628StateMachine", state_thread)
        logger.info("✅ Q628 state machine started")

        # Start LED control thread
        led_thread = threading.Thread(
            target=led_control_thread,
            daemon=True,
            name="LEDControl"
        )
        led_thread.start()
        register_thread("LEDControl", led_thread)
        logger.info("✅ LED status indication started")
        
        # 💡 Hardened: Start CSV flush thread
        csv_thread = threading.Thread(
            target=csv_flush_thread,
            daemon=True,
            name="CSVFlush"
        )
        csv_thread.start()
        register_thread("CSVFlush", csv_thread)
        logger.info("✅ CSV periodic flush thread started")
        
        # 💡 Hardened: Start system watchdog thread
        watchdog_thread = threading.Thread(
            target=system_watchdog,
            daemon=True,
            name="SystemWatchdog"
        )
        watchdog_thread.start()
        register_thread("SystemWatchdog", watchdog_thread)
        logger.info("✅ System watchdog started")

        # Initialize CSV file if enabled
        if Config.CSV_ENABLED:
            initialize_csv_file()

        # 💡 Hardened: Log summary of initialization status
        success_count = sum(1 for status in initialization_status.values() if status)
        total_components = len(initialization_status)
        if success_count == total_components:
            logger.info("✅ All hardware components initialized successfully")
        else:
            logger.warning(f"⚠️ Hardware initialization partial: {success_count}/{total_components} components ready")
            # Log specifics of what failed
            failed_components = [comp for comp, status in initialization_status.items() if not status]
            logger.warning(f"⚠️ Failed components: {', '.join(failed_components)}")
        
        # Signal that hardware initialization is complete (even if partial)
        hardware_init_complete.set()
        
        return success_count > 0  # Return success if at least one component initialized
    
    except Exception as e:
        logger.error("❌ Hardware initialization failed: %s", e, exc_info=True)
        hardware_init_complete.set()  # Signal that we're done trying even though it failed
        return False

def temperature_humidity_monitor() -> None:
    """
    Continuously monitor temperature and humidity
    and update system state.
    """
    logger.info("🌡️ Temperature and humidity monitoring started")
    thread_heartbeat("TempHumidityMonitor")  # 💡 Hardened: Register thread heartbeat
    
    error_count = 0
    last_successful_read = time.time()

    while not shutdown_requested.is_set():
        try:
            # 💡 Hardened: Periodically report thread is alive
            thread_heartbeat("TempHumidityMonitor")
            
            # Read temperature and humidity
            temp, humidity = read_temperature_humidity()
            
            # 💡 Hardened: Validate readings in realistic range
            if -40 <= temp <= 125 and 0 <= humidity <= 100:
                logger.debug(f"Temperature: {temp:.1f}°C, Humidity: {humidity:.1f}%")
                error_count = 0  # Reset error counter on success
                last_successful_read = time.time()
            else:
                error_count += 1
                logger.warning(f"⚠️ Unrealistic temperature/humidity values: {temp:.1f}°C, {humidity:.1f}%")
                if error_count >= 3:
                    # After 3 consecutive errors, revert to default values
                    temp, humidity = 23.5, 45.0
                    logger.error(f"❌ Using default temperature/humidity values after {error_count} errors")
            
            # Log values periodically (every minute)
            if int(time.time()) % 60 == 0:
                logger.info(f"🌡️ Temperature: {temp:.1f}°C, Humidity: {humidity:.1f}%")

        except Exception as e:
            error_count += 1
            elapsed = time.time() - last_successful_read
            logger.error(f"❌ Error reading temperature/humidity (#{error_count}, {elapsed:.1f}s since last success): {e}")
            
            # 💡 Hardened: Recover after repeated failures
            if error_count >= 5:
                # Record hardware error
                system_state.record_hardware_error("i2c", f"Temp/humidity sensor failed: {e}")
                
                # Use default values after 5 consecutive errors
                temp, humidity = 23.5, 45.0
                system_state.update(temperature=temp, humidity=humidity)
                
                # Longer delay after repeated errors to prevent log spam
                time.sleep(5.0)
                
                # 💡 Hardened: Try to recover I2C bus
                try:
                    logger.warning("⚠️ Attempting I2C bus recovery for temperature sensor")
                    with i2c_lock:
                        i2c_bus.close()
                        time.sleep(0.5)
                        i2c_bus = smbus2.SMBus(1)
                        logger.info("✅ I2C bus reinitialized")
                        error_count = 0  # Reset error counter after recovery attempt
                except Exception as recovery_error:
                    logger.error(f"❌ I2C bus recovery failed: {recovery_error}")

        # 💡 Hardened: Dynamic sleep interval - longer after errors
        sleep_time = min(Config.TEMP_HUMID_SAMPLE_INTERVAL * (1 + error_count * 0.2), 5.0)
        
        # Wait for the defined interval, checking shutdown frequently
        end_time = time.time() + sleep_time
        while time.time() < end_time:
            if shutdown_requested.is_set():
                break
            time.sleep(0.1)

    logger.info("🌡️ Temperature and humidity monitoring terminated")

def adc_sampling_thread() -> None:
    """
    Thread for 50Hz ADC sampling with precise timing control.
    Each real sample is duplicated to create a 100Hz timeline.
    Ensures exact sample count regardless of timing drift.
    """
    logger.info("📊 ADC sampling started at 50Hz with 100Hz virtual grid mapping")
    thread_heartbeat("ADCSampling")  # 💡 Hardened: Register thread heartbeat
    
    sample_count = 0
    real_sample_count = 0
    error_count = 0
    consecutive_errors = 0
    
    # 💡 Hardened: Adaptive timing correction variables
    timing_corrections = 0
    cumulative_drift = 0.0
    
    # 💡 Hardened: Track ADC reliability metrics
    successful_samples = 0
    failed_samples = 0
    last_sample_time = time.time()
    
    # Perform initial soft reset
    GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
    time.sleep(0.01) 
    GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
    time.sleep(0.02)
    
    # Use monotonic clock for accurate timing
    next_sample_time = time.monotonic()
    last_stats_time = time.monotonic()
    
    # Target rate and interval for strict timing
    TARGET_RATE = Config.ADC_TARGET_RATE
    SAMPLE_INTERVAL = 1.0 / TARGET_RATE  # 20ms for 50Hz
    
    # 💡 Hardened: Wait for hardware initialization
    if not hardware_init_complete.is_set():
        logger.info("📊 ADC thread waiting for hardware initialization...")
        hardware_init_complete.wait(timeout=10.0)
    
    while not shutdown_requested.is_set():
        try:
            # 💡 Hardened: Periodically report thread is alive
            thread_heartbeat("ADCSampling")
            
            current_time = time.monotonic()
            
            # 💡 Hardened: Check if we have severe timing drift
            sample_delay = current_time - next_sample_time
            if sample_delay > 0.1:  # More than 100ms behind
                logger.warning(f"⚠️ ADC sampling severely behind schedule: {sample_delay*1000:.1f}ms")
                # Reset timing after severe drift
                next_sample_time = current_time
                timing_corrections += 1
            
            # Take sample at precise intervals (50Hz)
            if current_time >= next_sample_time:
                                # Record time before reading
                sample_start_time = time.monotonic()
                
                # 💡 Hardened: Wrap ADC read in try-except for robustness
                try:
                    # Read ADC value
                    raw_value = read_adc_22bit()
                    sample_timestamp = time.time()
                    
                    # 💡 Hardened: Validate ADC reading
                    if raw_value is None or abs(raw_value) > 2097151:  # 2^21-1
                        logger.warning(f"⚠️ Invalid ADC value: {raw_value}, using previous value")
                        # Use previous value if available, otherwise use 0
                        raw_value = system_state.get("last_adc_value") or 0
                        failed_samples += 1
                    else:
                        successful_samples += 1
                        last_sample_time = time.time()
                        global last_successful_sampling
                        last_successful_sampling = time.time()
                        consecutive_errors = 0  # Reset consecutive error counter
                    
                    # Store state values
                    system_state.set("field_strength", raw_value)
                    system_state.set("last_adc_value", raw_value)
                    
                    # Calculate voltage
                    vref = Config.VREF_ADC
                    voltage = (float(raw_value) / ((1 << 21) - 1)) * vref * 2
                    
                    # Store in buffer and add to system state
                    adc_buffer.enqueue((sample_timestamp, raw_value))
                    system_state.add_adc_data(raw_value)
                    
                    # If sampling is active, map this 50Hz sample to 100Hz grid (duplicate samples)
                    if sampling_active.is_set() and system_state.is_sampling_active():
                        # Add this sample to virtual buffer 
                        virtual_idx = system_state.get("virtual_sample_count")
                        
                        # Add sample to virtual buffer with data: (value, is_virtual, virtual_idx)
                        virtual_buffer.enqueue((raw_value, False, virtual_idx))
                        
                        # Increment virtual sample count by 1
                        system_state.increment_virtual_sample_count()
                        
                        # Add a duplicate sample to virtual buffer
                        virtual_idx = system_state.get("virtual_sample_count")
                        virtual_buffer.enqueue((raw_value, True, virtual_idx))
                        
                        # Increment virtual sample count by 1 for the duplicate
                        system_state.increment_virtual_sample_count()
                        
                        # CRITICAL: Check if we've reached the exact sample count
                        virtual_count = system_state.get("virtual_sample_count")
                        expected_count = system_state.get("total_expected_samples")
                        
                        # Every 100 samples, log progress
                        if virtual_count % 100 == 0:
                            logger.debug(f"📊 Virtual samples: {virtual_count}/{expected_count} " +
                                       f"({virtual_count/expected_count*100:.1f}%)")
                    
                    # Update sample count for logging
                    sample_count += 1
                    real_sample_count += 1
                    
                    # 💡 Hardened: Calculate time spent processing this sample
                    processing_time = time.monotonic() - sample_start_time
                    if processing_time > SAMPLE_INTERVAL * 0.5:
                        logger.warning(f"⚠️ ADC processing took {processing_time*1000:.1f}ms " +
                                       f"({processing_time/SAMPLE_INTERVAL*100:.1f}% of interval)")
                
                except Exception as e:
                    error_count += 1
                    consecutive_errors += 1
                    failed_samples += 1
                    logger.error(f"❌ Error reading ADC (#{error_count}): {e}")
                    
                    # 💡 Hardened: Use last known good value if available
                    raw_value = system_state.get("last_adc_value") or 0
                    
                    # 💡 Hardened: Reset ADC after consecutive errors
                    if consecutive_errors >= 3:
                        try:
                            logger.warning(f"⚠️ Attempting ADC reset after {consecutive_errors} consecutive errors")
                            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
                            time.sleep(0.01)
                            GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
                            time.sleep(0.02)
                        except Exception as reset_error:
                            logger.error(f"❌ ADC reset failed: {reset_error}")
                
                # Schedule next sample with precise timing
                next_sample_time += SAMPLE_INTERVAL
                
                # 💡 Hardened: Calculate timing drift and correct if needed
                drift = current_time - (next_sample_time - SAMPLE_INTERVAL)
                cumulative_drift += drift
                
                # If cumulative drift exceeds 1ms, apply a small correction
                if abs(cumulative_drift) > 0.001:
                    correction = -cumulative_drift * 0.1  # Apply 10% correction
                    next_sample_time += correction
                    cumulative_drift *= 0.9  # Reduce accumulated drift
                
                # If we're too far behind schedule (more than 2 intervals), reset timing
                if current_time > next_sample_time + (2 * SAMPLE_INTERVAL):
                    logger.warning(f"⚠️ ADC sampling falling too far behind - resetting timing")
                    next_sample_time = current_time + SAMPLE_INTERVAL
                    timing_corrections += 1
                    cumulative_drift = 0.0
            
            # Show sampling statistics every 5 seconds
            if current_time - last_stats_time >= 5.0:
                elapsed = current_time - last_stats_time
                actual_rate = sample_count / elapsed if elapsed > 0 else 0
                sample_count = 0  # Reset for next period
                
                # 💡 Hardened: Calculate reliability metrics
                success_rate = (successful_samples / max(1, successful_samples + failed_samples)) * 100
                time_since_last_sample = time.time() - last_sample_time
                
                # Log real and virtual sample rates
                virtual_count = system_state.get("virtual_sample_count")
                expected_count = system_state.get("total_expected_samples")
                if expected_count > 0:
                    progress = (virtual_count / expected_count) * 100
                    logger.info(f"📊 ADC sampling stats: {actual_rate:.2f} Hz real rate, " +
                              f"{virtual_count} virtual samples ({progress:.1f}% complete), " +
                              f"reliability: {success_rate:.1f}%, corrections: {timing_corrections}")
                else:
                    logger.info(f"📊 ADC sampling stats: {actual_rate:.2f} Hz real rate, " +
                              f"reliability: {success_rate:.1f}%, last sample: {time_since_last_sample:.1f}s ago")
                
                # Reset counters for next period
                successful_samples = 0
                failed_samples = 0
                timing_corrections = 0
                
                last_stats_time = current_time
            
            # Sleep efficiently until next sample time
            remaining_time = next_sample_time - time.monotonic()
            if remaining_time > 0:
                time.sleep(min(remaining_time, 0.001))  # Sleep at most 1ms for responsiveness
                
        except Exception as e:
            logger.error(f"❌ Error in ADC sampling: {e}")
            time.sleep(0.1)  # Short pause on errors


# 💡 Hardened: I2C read with retry and exception handling
@with_retry(retries=Config.MAX_I2C_RETRIES, delay=Config.I2C_RECOVERY_DELAY, operation_name="read_motor_status")
def read_motor_status() -> Optional[MotorStatus]:
    """
    Read motor status via I2C with improved error handling using i2c_rdwr
    """
    try:
        with i2c_lock:
            with SMBus(1) as bus:
                # First send passive mode command with proper timing
                write_passiv = i2c_msg.write(Config.I2C_ADDR_MOTOR, [Config.CMD_PASSIV])
                bus.i2c_rdwr(write_passiv)
                time.sleep(0.05)  # Increased delay for stability

                # Then send status request command and read response
                write_status = i2c_msg.write(Config.I2C_ADDR_MOTOR, [Config.CMD_STATUS])
                bus.i2c_rdwr(write_status)
                time.sleep(0.05)  # Increased delay for stability
                
                # Read byte separately since we need the delay between write and read
                read_status = i2c_msg.read(Config.I2C_ADDR_MOTOR, 1)
                bus.i2c_rdwr(read_status)
                
                # Get the status byte from the read message
                status_byte = list(read_status)[0]
                
                # 💡 Hardened: Validate status byte
                if status_byte > 255 or status_byte < 0:
                    raise ValueError(f"Invalid status byte: {status_byte}")
                
                # Log raw status for debugging
                logger.debug(f"Raw motor status byte: 0x{status_byte:02X} (binary: {bin(status_byte)[2:].zfill(8)})")

        # 💡 Hardened: Update last successful I2C operation timestamp
        system_state.set("last_successful_i2c", time.time())
        global last_successful_operation
        last_successful_operation = time.time()

        # Convert to MotorStatus object
        status = MotorStatus.from_byte(status_byte)

        # Update system state
        system_state.update_motor_status(status, status_byte)

        return status
    except Exception as e:
        # 💡 Hardened: Record I2C error
        system_state.record_hardware_error("i2c", f"Motor status read error: {e}")
        logger.error("❌ Error reading motor status: %s", e)
        return None

# 💡 Hardened: Temperature/humidity reading with retry and validation
@with_retry(retries=Config.MAX_I2C_RETRIES, delay=Config.I2C_RECOVERY_DELAY, operation_name="read_temperature_humidity")
def read_temperature_humidity() -> Tuple[float, float]:
    """
    Read temperature and humidity from HYT sensor using i2c_rdwr
    
    Returns:
        Tuple of (temperature in °C, humidity in %)
    """
    try:
        # Start measurement
        with i2c_lock:
            with SMBus(1) as bus:
                # Send measurement command
                write_cmd = i2c_msg.write(Config.HYT_ADDR, [0x00])
                bus.i2c_rdwr(write_cmd)
                
        time.sleep(0.1)  # Wait for measurement

        # Read 4 bytes of data
        with i2c_lock:
            with SMBus(1) as bus:
                read_data = i2c_msg.read(Config.HYT_ADDR, 4)
                bus.i2c_rdwr(read_data)
                
                # Convert the read message to a list of bytes
                data = list(read_data)
                
                # 💡 Hardened: Validate data length
                if len(data) != 4:
                    raise ValueError(f"Expected 4 bytes from HYT sensor, got {len(data)}")

        # First two bytes: humidity (14 bits)
        # Last two bytes: temperature (14 bits)
        humidity_raw = ((data[0] & 0x3F) << 8) | data[1]
        temp_raw = (data[2] << 6) | (data[3] >> 2)

        # Convert to physical values
        humidity = (humidity_raw / 16383.0) * 100.0
        temp = (temp_raw / 16383.0) * 165.0 - 40.0
        
        # 💡 Hardened: Validate readings are in normal range
        if temp < -40 or temp > 125:
            logger.warning(f"⚠️ Temperature reading out of range: {temp}°C")
            # Clamp to valid range
            temp = max(-40, min(125, temp))
            
        if humidity < 0 or humidity > 100:
            logger.warning(f"⚠️ Humidity reading out of range: {humidity}%")
            # Clamp to valid range
            humidity = max(0, min(100, humidity))

        # Update system state
        system_state.update(temperature=temp, humidity=humidity)
        
        # 💡 Hardened: Update last successful I2C operation timestamp
        system_state.set("last_successful_i2c", time.time())
        global last_successful_operation 
        last_successful_operation = time.time()

        return (temp, humidity)
    except Exception as e:
        # 💡 Hardened: Record I2C error
        system_state.record_hardware_error("i2c", f"Temperature/humidity read error: {e}")
        logger.error("❌ Error reading temperature/humidity: %s", e)
        return (23.5, 45.0)  # Return default values on error

# 💡 Hardened: ADC read with timeout and error recovery
@timeout(1.0)  # Set 1 second timeout for ADC read
@with_retry(retries=Config.MAX_ADC_RETRIES, delay=0.05, operation_name="read_adc_22bit")
def read_adc_22bit() -> int:
    """
    Read 22-bit raw value from MCP3553SN ADC using simplified, reliable implementation.
    
    Returns:
        Raw signed integer ADC value
    """
    with adc_lock:
        try:
            # 💡 Hardened: Thread-safe timeout implementation
            # Create a timeout with a simple deadline
            deadline = time.monotonic() + 1.0  # 1 second timeout
            
            # 💡 Hardened: First check if SDO pin is connected/accessible
            try:
                initial_sdo_state = GPIO.input(Config.PIN_SDO_ADC)
            except Exception as e:
                logger.error(f"❌ ADC SDO pin not accessible: {e}")
                raise RuntimeError(f"ADC SDO pin not accessible: {e}")
            
            # Wait for SDO to go LOW (indicates data ready)
            ready = False
            timeout_start = time.monotonic()
            timeout_limit = 0.05  # Short timeout for more responsive behavior
            
            while time.monotonic() - timeout_start < timeout_limit:
                # Check overall deadline
                if time.monotonic() > deadline:
                    raise TimeoutError("ADC read operation timed out")
                    
                if GPIO.input(Config.PIN_SDO_ADC) == GPIO.LOW:
                    ready = True
                    break
                time.sleep(0.0005)  # Check frequently
            
            if not ready:
                logger.warning("MCP3553 data not ready - performing soft reset and returning 0")
                # Force reset when not ready
                GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
                time.sleep(0.0005)
                GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
                # 💡 Hardened: Record ADC error
                system_state.record_hardware_error("adc", "ADC not ready - timeout waiting for SDO LOW")
                return 0
            
            # SDO is LOW - read 24 bits from the ADC (MSB first)
            value = 0
            bit_errors = 0
            
            for i in range(24):
                # Check overall deadline
                if time.monotonic() > deadline:
                    raise TimeoutError("ADC read operation timed out")
                    
                # 💡 Hardened: Detect bit errors
                try:
                    GPIO.output(Config.PIN_CLK_ADC, GPIO.HIGH)
                    bit = GPIO.input(Config.PIN_SDO_ADC)
                    value = (value << 1) | bit
                    GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
                    time.sleep(5e-6)  # 5μs delay
                except Exception as e:
                    bit_errors += 1
                    logger.error(f"❌ Error reading bit {i} from ADC: {e}")
                    # Continue with default bit value of 0
                    value = value << 1
                    GPIO.output(Config.PIN_CLK_ADC, GPIO.LOW)
            
            # 💡 Hardened: Handle bit error cases
            if bit_errors > 0:
                logger.warning(f"⚠️ ADC read completed with {bit_errors} bit errors")
                if bit_errors > 12:  # More than half the bits had errors
                    logger.error("❌ Too many bit errors, ADC read unreliable")
                    # Force reset
                    GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
                    time.sleep(0.0005)
                    GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
                    # Return last known good value or 0
                    return system_state.get("last_adc_value") or 0
            
            # Extract overflow flags and actual 22-bit value
            ovh = (value >> 23) & 0x01
            ovl = (value >> 22) & 0x01
            signed_value = value & 0x3FFFFF
            
            # 💡 Hardened: Check for overflow conditions
            if ovh or ovl:
                logger.warning(f"⚠️ ADC overflow detected: OVH={ovh}, OVL={ovl}")
            
            # Handle two's complement for negative values
            if signed_value & (1 << 21):
                signed_value -= (1 << 22)
            
            # Reset AFTER each sample
            GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
            time.sleep(0.0005)
            GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
            
            # 💡 Hardened: Update last successful ADC operation timestamp
            system_state.set("last_successful_adc", time.time())
            global last_successful_operation
            last_successful_operation = time.time()
            
            return signed_value
            
        except Exception as e:
            # 💡 Hardened: Record ADC error
            system_state.record_hardware_error("adc", f"ADC read error: {e}")
            logger.error(f"❌ MCP3553 SPI read error: {e}")
            
            # Ensure CS is reset on error
            try:
                GPIO.output(Config.PIN_CS_ADC, GPIO.HIGH)
                time.sleep(0.0005)
                GPIO.output(Config.PIN_CS_ADC, GPIO.LOW)
            except:
                pass
                
            # Re-raise to trigger retry
            raise


def read_adc() -> Tuple[float, int]:
    """
    Read MCP3553 ADC value and return voltage and raw value.
    Acts as main interface for ADC readings.
    """
    try:
        raw_value = read_adc_22bit()
        # Calculate voltage
        voltage = (float(raw_value) / ((1 << 21) - 1)) * Config.VREF_ADC * 2
        return voltage, raw_value
    except Exception as e:
        # 💡 Hardened: Handle ADC read failure gracefully
        logger.error(f"❌ ADC read failed in read_adc(): {e}")
        # Use last known value or 0
        last_value = system_state.get("last_adc_value") or 0
        voltage = (float(last_value) / ((1 << 21) - 1)) * Config.VREF_ADC * 2
        return voltage, last_value


# 💡 Hardened: DAC output with retry and validation
@with_retry(retries=Config.MAX_DAC_RETRIES, delay=0.05, operation_name="set_dac")
def set_dac(value: int) -> None:
    """
    Set DAC value using bit-banged SPI with timing from dac_gui.py
    """
    with dac_lock:
        try:
            # 💡 Hardened: Validate input value
            if not isinstance(value, int):
                raise ValueError(f"DAC value must be integer, got {type(value)}")
                
            # Ensure value is 16-bit
            original_value = value
            value &= 0xFFFF
            
            if original_value != value:
                logger.warning(f"⚠️ DAC value truncated from {original_value} to {value}")

            # Start transmission
            GPIO.output(Config.PIN_CS_DAC, GPIO.LOW)
            time.sleep(Config.DAC_CLK_DELAY)  # Use 10μs like in dac_gui.py

            # Send 16 bits MSB first
            bit_errors = 0
            for i in range(15, -1, -1):
                try:
                    bit = (value >> i) & 1
                    GPIO.output(Config.PIN_DAT_DAC, bit)
                    time.sleep(Config.DAC_CLK_DELAY)  # Wait before clock high
                    GPIO.output(Config.PIN_CLK_DAC, GPIO.HIGH)
                    time.sleep(Config.DAC_CLK_DELAY)  # Hold clock high
                    GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
                    time.sleep(Config.DAC_CLK_DELAY)  # Hold clock low
                except Exception as e:
                    bit_errors += 1
                    logger.error(f"❌ Error setting bit {i} of DAC: {e}")
                    # Try to continue transmission
                    GPIO.output(Config.PIN_CLK_DAC, GPIO.LOW)
                    time.sleep(Config.DAC_CLK_DELAY)
            
            # End transmission
            GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)
            
            # 💡 Hardened: Check for bit errors
            if bit_errors > 0:
                logger.warning(f"⚠️ DAC set completed with {bit_errors} bit errors")
                if bit_errors > 8:  # Half or more bits had errors
                    raise RuntimeError(f"Too many bit errors ({bit_errors}) during DAC write")
            
            # 💡 Hardened: Update last successful DAC operation timestamp
            system_state.set("last_successful_dac", time.time())
            global last_successful_operation
            last_successful_operation = time.time()
            
        except Exception as e:
            # 💡 Hardened: Record DAC error
            system_state.record_hardware_error("dac", f"DAC write error: {e}")
            logger.error("❌ Error setting DAC: %s", e)
            GPIO.output(Config.PIN_CS_DAC, GPIO.HIGH)  # Ensure CS is high on error
            raise  # Re-raise to trigger retry


# 💡 Hardened: Set DAC voltage with validation
def set_dac_voltage(voltage: float) -> None:
    """
    Set DAC output voltage with gain compensation like in dac_gui.py
    """
    try:
        # 💡 Hardened: Validate voltage is in reasonable range
        if voltage < 0 or voltage > 5.0:
            logger.warning(f"⚠️ DAC voltage {voltage}V outside recommended range (0-5V)")
            # Clamp to safe range
            voltage = max(0, min(5.0, voltage))
        
        # Apply gain compensation (divide by GAIN like in dac_gui.py)
        v_dac = voltage / Config.DAC_GAIN
        
        # Calculate DAC value using the same method as dac_gui.py
        dac_value = int((v_dac / Config.VREF_DAC) * 65535)
        dac_value = max(0, min(65535, dac_value))  # Limit to valid range
        
        # Send to DAC
        set_dac(dac_value)
        logger.debug(f"DAC set to {voltage:.3f}V (DAC input: {v_dac:.3f}V, value: {dac_value} (0x{dac_value:04X}))")
    except Exception as e:
        logger.error(f"❌ Error setting DAC voltage: {e}")
        # 💡 Hardened: Try to set DAC to safe value on error
        try:
            set_dac(0)  # Try to set to 0V as safety measure
            logger.warning("⚠️ Set DAC to 0V after error")
        except:
            pass


# 💡 Hardened: Set HV with validation and safety checks
@critical_operation
def set_dac_voltage_for_hv_dc(hv_dc_value: int) -> None:
    """
    Sets the DAC output voltage to achieve the desired HV DC value.
    
    Follows the 1:2000 ratio where 3.25V DAC output = 6500V HV DC.
    
    Args:
        hv_dc_value: Desired high voltage DC value in volts
    """
    try:
        # 💡 Hardened: Validate HV value is in safe range
        if hv_dc_value < 0 or hv_dc_value > 7000:
            logger.warning(f"⚠️ HV DC value {hv_dc_value}V outside safe range (0-7000V)")
            # Clamp to safe range
            hv_dc_value = max(0, min(7000, hv_dc_value))
        
        # Calculate required DAC voltage using the 1:2000 ratio (6500V HV = 3.25V DAC)
        dac_voltage = hv_dc_value / 2000.0  # This is the exact 1:2000 ratio
        
        # 💡 Hardened: Double-check calculated voltage is in safe range
        if dac_voltage < 0 or dac_voltage > 3.5:
            logger.warning(f"⚠️ Calculated DAC voltage {dac_voltage}V outside safe range (0-3.5V)")
            dac_voltage = max(0, min(3.5, dac_voltage))
        
        # Log the conversion
        logger.info(f"⚡ Setting HV DC to {hv_dc_value}V (DAC output: {dac_voltage:.4f}V)")
        
        # Apply the voltage to the DAC
        set_dac_voltage(dac_voltage)
        
        # Store the set value in system state
        system_state.set("hv_dc_value", hv_dc_value)
    except Exception as e:
        logger.error(f"❌ Error setting HV DC voltage: {e}")
        # 💡 Hardened: Safety measure - try to set voltage to 0
        try:
            set_dac_voltage(0.0)
            logger.warning("⚠️ Emergency shutdown - set DAC to 0V after HV error")
        except:
            pass


def motor_status_monitor() -> None:
    """
    Continuously monitor motor status in a separate thread
    """
    logger.info("🔄 Motor status monitoring started")
    thread_heartbeat("MotorStatusMonitor")  # 💡 Hardened: Register thread heartbeat
    
    error_logged = False  # Track if we've already logged an error
    last_status = None
    consecutive_errors = 0
    last_successful_read = time.time()

    while not shutdown_requested.is_set():
        try:
            # 💡 Hardened: Periodically report thread is alive
            thread_heartbeat("MotorStatusMonitor")
            
            status = read_motor_status()
            if status:
                # 💡 Hardened: Reset error counters on success
                consecutive_errors = 0
                last_successful_read = time.time()
                
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
            else:
                # 💡 Hardened: Handle failed reads
                consecutive_errors += 1
                
                # After 3 consecutive failures, log more severe warning
                if consecutive_errors == 3:
                    time_since_success = time.time() - last_successful_read
                    logger.warning(f"⚠️ Motor status read failed {consecutive_errors} times in a row ({time_since_success:.1f}s since last success)")
                    
                # After 10 consecutive failures, try to recover I2C bus
                if consecutive_errors == 10:
                    logger.error(f"❌ Motor status read failed {consecutive_errors} times - attempting I2C recovery")
                    try:
                        with i2c_lock:
                            i2c_bus.close()
                            time.sleep(0.5)
                            i2c_bus = smbus2.SMBus(1)
                            logger.info("✅ I2C bus reinitialized for motor communication")
                    except Exception as e:
                        logger.error(f"❌ I2C recovery failed: {e}")
                
        except Exception as e:
            consecutive_errors += 1
            logger.error("❌ Error in motor status monitor: %s", e)

        # 💡 Hardened: Dynamic polling interval - slow down after errors
        poll_interval = min(Config.MOTOR_STATUS_POLL_INTERVAL * (1 + consecutive_errors * 0.1), 2.0)
        
        # Sleep before next poll, checking shutdown frequently
        end_time = time.time() + poll_interval
        while time.time() < end_time:
            if shutdown_requested.is_set():
                break
            time.sleep(0.1)

    logger.info("🔄 Motor status monitor terminated")


# 💡 Hardened: Motor movement with proper verification
@critical_operation
def move_motor(position: MotorPosition) -> bool:
    """
    Move motor to specified position using i2c_rdwr

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

        # 💡 Hardened: Check if motor has error before sending command
        if status and (status & MotorStatus.ERROR):
            logger.warning("⚠️ Motor has error flag set, attempting command anyway")
            
        # 💡 Hardened: Retry I2C command if needed
        success = False
        for retry in range(Config.MAX_I2C_RETRIES):
            try:
                # Send appropriate command to motor controller
                cmd = Config.CMD_UP if position == MotorPosition.UP else Config.CMD_DOWN
                with i2c_lock:
                    with SMBus(1) as bus:
                        write_cmd = i2c_msg.write(Config.I2C_ADDR_MOTOR, [cmd])
                        bus.i2c_rdwr(write_cmd)
                success = True
                break
            except Exception as e:
                logger.error(f"❌ Error sending motor command (attempt {retry+1}): {e}")
                time.sleep(Config.I2C_RECOVERY_DELAY)
        
        if not success:
            logger.error(f"❌ Failed to send motor command after {Config.MAX_I2C_RETRIES} attempts")
            system_state.set("last_error", f"Motor command failed after retries")
            return False

        # Wait briefly and check if command was accepted
        time.sleep(0.1)
        status = read_motor_status()
        
        # 💡 Hardened: Verify command was accepted
        if status and (status & MotorStatus.BUSY):
            logger.info("✅ Motor command accepted, motor is now busy")
            
            # 💡 Hardened: Start a verification thread to ensure motor starts moving
            def verify_motor_movement():
                try:
                    time.sleep(1.0)  # Wait for movement to start
                    
                    # Check if motor is still busy 
                    verify_status = read_motor_status()
                    if verify_status and not (verify_status & MotorStatus.BUSY):
                        logger.warning("⚠️ Motor no longer busy after 1 second - movement may have failed")
                        
                                        # For UP command, check if we reach home position
                    if position == MotorPosition.UP:
                        # Wait up to 10 seconds for home position
                        home_reached = False
                        for _ in range(20):  # 20 * 0.5s = 10s
                            time.sleep(0.5)
                            verify_status = read_motor_status()
                            if verify_status and (verify_status & MotorStatus.HOME):
                                logger.info("✅ Motor reached home position verification")
                                home_reached = True
                                break
                        
                        if not home_reached:
                            logger.warning("⚠️ Motor failed to reach home position within timeout")
                except Exception as e:
                    logger.error(f"❌ Error in motor movement verification: {e}")
            
            verification_thread = threading.Thread(
                target=verify_motor_movement,
                daemon=True,
                name="MotorMoveVerification"
            )
            verification_thread.start()
            
            return True
        else:
            # 💡 Hardened: Log warning but still return True since command was sent
            logger.warning("⚠️ Motor did not report busy after command, but continuing")
            return True
    except Exception as e:
        logger.error("❌ Error moving motor: %s", e)
        system_state.set("last_error", f"Motor error: {str(e)}")
        return False


# 💡 Hardened: HV control with safety checks and verification
@critical_operation
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
            # 💡 Hardened: Safety check - make sure we're not in a state where HV shouldn't be on
            current_state = system_state.get_q628_state()
            if current_state not in [Q628State.WAIT_HV, Q628State.START, Q628State.PRE_ACTIV]:
                logger.warning(f"⚠️ Enabling HV in unexpected state: {current_state.name}")
            
            # Get configured HV DC value
            hv_dc = system_state.get("hv_dc_value")
            
            # 💡 Hardened: Validate HV DC value
            if hv_dc < 0 or hv_dc > 7000:
                logger.warning(f"⚠️ HV DC value outside safe range: {hv_dc}V, clamping to safe value")
                hv_dc = max(0, min(7000, hv_dc))
                system_state.set("hv_dc_value", hv_dc)
            
            # Calculate DAC voltage using the 1:2000 ratio (6500V = 3.25V DAC)
            dac_voltage = hv_dc / 2000.0
            
            # 💡 Hardened: Sequential activation for safety
            # First set DAC to proper voltage
            set_dac_voltage(dac_voltage)
            time.sleep(0.1)  # Short delay between DAC and HV activation
            
            # Then activate the HV AC/DC GPIO
            GPIO.output(Config.HVAC_GPIO, GPIO.HIGH)
            
            # 💡 Hardened: Verify HV GPIO is actually HIGH
            if GPIO.input(Config.HVAC_GPIO) != GPIO.HIGH:
                logger.error("❌ HV GPIO failed to set HIGH - hardware issue detected")
                
            # Log the values being applied
            hv_ac = system_state.get("hv_ac_value")
            logger.info(f"⚡ Applied voltage values - AC:{hv_ac}V, DC:{hv_dc}V (DAC={dac_voltage:.4f}V)")
            
            # 💡 Hardened: Start a verification thread for HV activation
            def verify_hv_activation():
                try:
                    time.sleep(0.5)  # Wait for HV to stabilize
                    # Verify HV GPIO is still HIGH
                    if GPIO.input(Config.HVAC_GPIO) != GPIO.HIGH:
                        logger.error("❌ HV GPIO dropped to LOW unexpectedly after activation")
                        # Don't auto-correct - this requires operator intervention
                except Exception as e:
                    logger.error(f"❌ Error in HV activation verification: {e}")
            
            # Start verification in background
            threading.Thread(target=verify_hv_activation, daemon=True, name="HVVerification").start()
            
        else:
            # 💡 Hardened: Sequential deactivation for safety
            # First turn off the main HV AC/DC GPIO
            GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
            
            # Verify HV GPIO is LOW
            if GPIO.input(Config.HVAC_GPIO) != GPIO.LOW:
                logger.error("❌ HV GPIO failed to set LOW - hardware issue detected")
            
            # Short delay
            time.sleep(0.1)
            
            # Then set DAC to zero
            set_dac_voltage(0.0)
            logger.info("⚡ High voltage turned OFF, DAC set to 0V")
            
            # 💡 Hardened: Additional verification for complete HV shutdown
            def verify_hv_shutdown():
                try:
                    time.sleep(0.5)  # Wait to ensure shutdown is complete
                    # Verify HV GPIO is still LOW
                    if GPIO.input(Config.HVAC_GPIO) != GPIO.LOW:
                        logger.error("❌ HV GPIO unexpectedly HIGH after shutdown")
                except Exception as e:
                    logger.error(f"❌ Error in HV shutdown verification: {e}")
            
            # Start verification in background
            threading.Thread(target=verify_hv_shutdown, daemon=True, name="HVShutdownVerification").start()
        
    except Exception as e:
        logger.error("❌ Error setting high voltage: %s", e)
        system_state.set("last_error", f"HV error: {str(e)}")
        
        # 💡 Hardened: Emergency shutdown on error
        try:
            # Ensure HV is off
            GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
            set_dac_voltage(0.0)
            logger.warning("⚠️ Emergency HV shutdown after error")
        except Exception as shutdown_error:
            logger.critical(f"❌ CRITICAL: Emergency HV shutdown failed: {shutdown_error}")


# 💡 Hardened: Valve control with verification
@critical_operation
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
        
        # 💡 Hardened: Set both magnets with verification
        GPIO.output(Config.MAGNET1_GPIO, gpio_state)
        GPIO.output(Config.MAGNET2_GPIO, gpio_state)
        
        # 💡 Hardened: Verify GPIO states
        magnet1_actual = GPIO.input(Config.MAGNET1_GPIO)
        magnet2_actual = GPIO.input(Config.MAGNET2_GPIO)
        
        if magnet1_actual != gpio_state or magnet2_actual != gpio_state:
            logger.error(f"❌ Valve GPIO verification failed: " +
                         f"M1={magnet1_actual}!={gpio_state}, " +
                         f"M2={magnet2_actual}!={gpio_state}")
            
            # Try once more
            GPIO.output(Config.MAGNET1_GPIO, gpio_state)
            GPIO.output(Config.MAGNET2_GPIO, gpio_state)
            
            # Second verification
            magnet1_actual = GPIO.input(Config.MAGNET1_GPIO)
            magnet2_actual = GPIO.input(Config.MAGNET2_GPIO)
            
            if magnet1_actual != gpio_state or magnet2_actual != gpio_state:
                logger.error("❌ Valve GPIO verification failed after retry - possible hardware issue")
            else:
                logger.info("✅ Valve GPIO set correctly after retry")
        
    except Exception as e:
        logger.error("❌ Error setting valve: %s", e)
        system_state.set("last_error", f"Valve error: {str(e)}")


def led_control_thread() -> None:
    """
    Control LED status indication based on system state.

    LED patterns:
    - Slow blink: System initializing or in IDLE
    - Solid on: Ready to start (after 'S' command, waiting for button)
    - Fast blink: Measurement in progress (after button press)
    """
    logger.info("💡 LED control thread started")
    thread_heartbeat("LEDControl")  # 💡 Hardened: Register thread heartbeat

    led_state = False
    shutdown_blink_count = 0  # Limited number of blinks during shutdown
    error_blink_start = 0  # For error pattern timing

    try:  # 💡 Hardened: Added global exception handler for thread
        while not shutdown_requested.is_set():
            try:
                # 💡 Hardened: Periodically report thread is alive
                thread_heartbeat("LEDControl")
                
                current_state = system_state.get_q628_state()
                measurement_active = system_state.is_measurement_active()
                
                # Check if waiting for button press (after 'S' command)
                waiting_for_button = (current_state == Q628State.WAIT_START_SW)

                # 💡 Hardened: Check system health for error pattern
                system_healthy = system_state.get("healthy")
                error_count = system_state.get("error_count")
                
                # 💡 Hardened: Special error pattern if system unhealthy
                if not system_healthy and error_count > 0:
                    # Rapid blink pattern (3 quick flashes, pause, repeat)
                    cycle_time = time.time() % 2.0  # 2-second cycle
                    
                    if cycle_time < 0.2:
                        led_state = True
                    elif cycle_time < 0.4:
                        led_state = False
                    elif cycle_time < 0.6:
                        led_state = True
                    elif cycle_time < 0.8:
                        led_state = False
                    elif cycle_time < 1.0:
                        led_state = True
                    else:
                        led_state = False
                        
                    GPIO.output(Config.START_LED_GPIO, GPIO.HIGH if led_state else GPIO.LOW)
                    time.sleep(0.05)  # Short sleep for responsive error pattern
                    
                elif current_state in [Q628State.POWER_UP, Q628State.STARTUP, Q628State.PRE_IDLE]:
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
                    # 💡 Hardened: Flash SOS pattern on error
                    for _ in range(3):  # S: 3 short flashes
                        GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
                        time.sleep(0.2)
                        GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
                        time.sleep(0.2)
                    time.sleep(0.4)  # Pause between letters
                    for _ in range(3):  # O: 3 long flashes
                        GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
                        time.sleep(0.6)
                        GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
                        time.sleep(0.2)
                    time.sleep(0.4)  # Pause between letters
                    for _ in range(3):  # S: 3 short flashes
                        GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
                        time.sleep(0.2)
                        GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
                        time.sleep(0.2)
                    time.sleep(1.0)  # Longer pause after SOS
                except:
                    # If SOS pattern fails, try simple fallback
                    try:
                        GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)  # Error fallback - constant on
                        led_state = True
                    except:
                        pass
                time.sleep(1.0)
    except Exception as e:
        logger.critical(f"❌ CRITICAL: LED control thread crashed: {e}")
        # Try to recover by restarting the thread
        try:
            new_thread = threading.Thread(
                target=led_control_thread,
                daemon=True,
                name="LEDControl-Recovery"
            )
            new_thread.start()
            register_thread("LEDControl-Recovery", new_thread)
            logger.info("✅ Restarted LED control thread after crash")
        except:
            pass

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
    
    try:  # 💡 Hardened: Added exception handling
        # Clear buffer for a fresh start
        adc_buffer.clear()
        virtual_buffer.clear()
        system_state.clear_adc_data()
        
        # Reset transmission tracking
        last_sent_index = 0
        
        # Reset the virtual sample count
        system_state.set("virtual_sample_count", 0)
        
        # Mark the sampling as active for the state machine
        system_state.start_sampling()
        sampling_active.set()
        
        # 💡 Hardened: Set sampling timestamp
        global last_successful_sampling
        last_successful_sampling = time.time()
        
    except Exception as e:
        logger.error(f"❌ Error starting sampling: {e}")
        # Try emergency recovery
        try:
            sampling_active.set()
            system_state.set("sampling_active", True)
            logger.warning("⚠️ Emergency sampling activation after error")
        except:
            pass


def stop_sampling() -> None:
    """
    Stop ADC sampling with coordinated shutdown.
    Finalizes CSV files with the correct end timestamp.
    """
    logger.info("📊 Stopping sampling") 
    
    try:  # 💡 Hardened: Added exception handling
        sampling_active.clear()
        system_state.stop_sampling()
        
                # Log buffer statistics
        defined_time = system_state.get("defined_measurement_time")
        expected_virtual_samples = int(defined_time * 100)  # 100Hz expected by Delphi
        current_virtual_samples = system_state.get("virtual_sample_count")
        
        logger.info(f"📊 Sampling stopped with {current_virtual_samples}/{expected_virtual_samples} virtual samples")
        logger.info(f"📊 {last_sent_index} samples sent to client so far")

        # 💡 Hardened: Ensure both flags are reset
        if system_state.is_sampling_active():
            logger.warning("⚠️ Sampling flag inconsistency detected, forcing reset")
            system_state.set("sampling_active", False)
            
        if sampling_active.is_set():
            logger.warning("⚠️ Sampling event inconsistency detected, forcing reset")
            sampling_active.clear()

        # Flush CSV file
        if Config.CSV_ENABLED and csv_file_handle is not None:
            try:
                safe_flush_csv()
                logger.info("📊 CSV file flushed")
            except Exception as e:
                logger.error(f"❌ Error flushing CSV file: {e}")

        # 💡 Hardened: Verify buffers are in expected state
        adc_health = adc_buffer.health_check()
        virtual_health = virtual_buffer.health_check()
        logger.debug(f"📊 Buffer health after stop - ADC: {adc_health['size']}/{adc_health['capacity']}, " +
                   f"Virtual: {virtual_health['size']}/{virtual_health['capacity']}")
                   
    except Exception as e:
        logger.error(f"❌ Error stopping sampling: {e}")
        # Emergency sampling termination
        sampling_active.clear()
        system_state.set("sampling_active", False)


# 💡 Hardened: Wait for motor with progress feedback, retry, and tolerance
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

    # 💡 Hardened: Start with a brief sleep to allow motor controller to update
    time.sleep(0.2)
    
    # 💡 Hardened: Track progress and report
    last_report_time = start_time
    errors = 0
    max_errors = 5  # Maximum allowed consecutive errors
    was_busy = False
    
    while time.time() - start_time < timeout:
        if shutdown_requested.is_set():
            return False

        try:
            status = read_motor_status()
            if status:
                errors = 0  # Reset error counter on successful read
                
                # Track motor busy state
                if status & MotorStatus.BUSY:
                    was_busy = True
                
                # If motor is at home and not busy, we're done
                if (status & MotorStatus.HOME) and not (status & MotorStatus.BUSY):
                    elapsed = time.time() - start_time
                    logger.info(f"✅ Motor reached home position after {elapsed:.1f}s")
                    return True

                # If motor has error, log and return
                if status & MotorStatus.ERROR:
                    logger.error("❌ Motor error while waiting for home position")
                    return False
                
                # Progress updates every second
                current_time = time.time()
                if current_time - last_report_time >= 1.0:
                    elapsed = current_time - start_time
                    remaining = timeout - elapsed
                    
                    motor_state = []
                    if status & MotorStatus.HOME:
                        motor_state.append("at home")
                    if status & MotorStatus.BUSY:
                        motor_state.append("busy")
                    if status & MotorStatus.ACTIVE:
                        motor_state.append("active")
                    
                    state_str = ", ".join(motor_state) if motor_state else "unknown state"
                    logger.info(f"⏳ Waiting for motor home: {elapsed:.1f}s elapsed, {remaining:.1f}s remaining, motor {state_str}")
                    last_report_time = current_time
            else:
                # Motor status read failed
                errors += 1
                if errors > max_errors:
                    logger.error(f"❌ Motor status read failed {errors} times while waiting for home")
                    return False
        except Exception as e:
            errors += 1
            logger.error(f"❌ Error checking motor status: {e}")
            if errors > max_errors:
                return False

        time.sleep(0.5)  # Check every 500ms

    # 💡 Hardened: Tolerant completion condition - if was busy and now at home
    # Try one last time with a more lenient condition
    try:
        final_status = read_motor_status()
        if final_status and (final_status & MotorStatus.HOME) and was_busy:
            logger.info("✅ Motor reached home position at timeout boundary (tolerant check)")
            return True
    except:
        pass
        
    logger.warning("⏰ Timeout waiting for motor to reach home position")
    return False


def q628_state_machine() -> None:
    """
    Main Q628 state machine - Implements measurement with exact sample count control.
    - Uses 50Hz sampling mapped to 100Hz grid
    - Measurement ends exactly at the specified sample count
    - Manages valve and motor operation with precise timing
    """
    logger.info("🔄 Q628 state machine started")
    thread_heartbeat("Q628StateMachine")  # 💡 Hardened: Register thread heartbeat
    
    # Initial state
    system_state.set_q628_state(Q628State.POWER_UP)
    
    # Track motor movement events
    motor_down_time = None
    
    # 💡 Hardened: Track state transitions for watchdog
    last_state_change_time = time.time()
    stuck_in_state_count = 0
    state_timeout_factors = {
        Q628State.POWER_UP: 5.0,      # 5x normal timeout for POWER_UP
        Q628State.STARTUP: 3.0,       # 3x normal timeout for STARTUP
        Q628State.PRE_IDLE: 2.0,      # 2x normal timeout for PRE_IDLE
        Q628State.IDLE: float('inf'),  # No timeout for IDLE
        Q628State.WAIT_START_SW: 5.0,  # 5x normal timeout for WAIT_START_SW
        Q628State.WAIT_HV: 2.0,        # 2x normal timeout for WAIT_HV
        Q628State.START: 3.0,          # 3x normal timeout for START
        Q628State.PRE_ACTIV: 3.0,      # 3x normal timeout for PRE_ACTIV
        Q628State.ACTIV: 2.0,          # 2x normal timeout for ACTIV
        Q628State.FINISH: 3.0,         # 3x normal timeout for FINISH
        Q628State.RECOVERY: 5.0        # 5x normal timeout for RECOVERY
    }
    
    # Main state machine loop
    while not shutdown_requested.is_set():
        try:
            # 💡 Hardened: Periodically report thread is alive
            thread_heartbeat("Q628StateMachine")
            
            # Get current state and elapsed time
            current_state = system_state.get_q628_state()
            elapsed = system_state.get_q628_elapsed()
            
            # 💡 Hardened: Check for stuck states (except IDLE and ACTIV)
            timeout_base = 10.0  # Base timeout value in seconds
            timeout_factor = state_timeout_factors.get(current_state, 1.0)
            state_timeout = timeout_base * timeout_factor
            
            if (current_state not in [Q628State.IDLE, Q628State.ACTIV] and 
                elapsed > state_timeout and 
                current_state != Q628State.RECOVERY):
                # Only increment stuck counter if we're not in recovery mode
                stuck_in_state_count += 1
                logger.warning(f"⚠️ State machine stuck in {current_state.name} for {elapsed:.1f}s " +
                             f"(timeout: {state_timeout:.1f}s, occurrence #{stuck_in_state_count})")
                
                if stuck_in_state_count >= 3:
                    logger.error(f"❌ State machine stuck in {current_state.name} {stuck_in_state_count} times - entering recovery")
                    # Enter recovery state
                    system_state.set_q628_state(Q628State.RECOVERY)
                    continue  # Skip to next iteration to handle recovery state
            
            # Reset stuck counter if state changes
            if time.time() - last_state_change_time < 1.0:  # State changed recently
                stuck_in_state_count = 0
                
            # Record current state change time for next comparison
            last_state_change_time = time.time()

            # State machine transitions
            if current_state == Q628State.POWER_UP:
                # Initial power-up state
                if elapsed > 2.0:
                    # After 2 seconds, transition to STARTUP
                    system_state.set_q628_state(Q628State.STARTUP)

            elif current_state == Q628State.STARTUP:
                # System startup - initialize hardware
                try:  # 💡 Hardened: Exception handling for hardware operations
                    # Disable HV
                    set_high_voltage(False)

                    # Close valve
                    set_valve(ValveStatus.CLOSED)

                    # Before moving motor, ensure valves are open
                    set_valve(ValveStatus.OPEN)
                    logger.info("🔧 Opening valves for motor movement")
                    
                    # Move motor to home position
                    if not move_motor(MotorPosition.UP):
                        logger.error("❌ Failed to send motor up command during startup")
                        # Continue anyway - will retry in next state
                    
                    # Transition to PRE_IDLE
                    system_state.set_q628_state(Q628State.PRE_IDLE)
                except Exception as e:
                    logger.error(f"❌ Error during STARTUP state: {e}")
                    time.sleep(1.0)  # Brief delay before retrying

            elif current_state == Q628State.PRE_IDLE:
                # Preparing for idle state
                try:  # 💡 Hardened: Exception handling for hardware operations
                    if elapsed > 3.0:
                        # 3 seconds have passed, now wait 3.5 more seconds with valves open
                        if elapsed > 6.5:  # 3.0 + 3.5 seconds
                            # Only close valves 3.5 seconds after motor movement complete
                            set_valve(ValveStatus.CLOSED)
                            logger.info("🔧 Closed valves 3.5 seconds after motor reached position")

                            # Transition to IDLE
                            system_state.set_q628_state(Q628State.IDLE)
                except Exception as e:
                    logger.error(f"❌ Error during PRE_IDLE state: {e}")
                    time.sleep(1.0)  # Brief delay before retrying

            elif current_state == Q628State.IDLE:
                # System idle, waiting for commands
                try:  # 💡 Hardened: Exception handling for command processing
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
                            system_state.start_measurement()
                            start_sampling()

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
                except Exception as e:
                    logger.error(f"❌ Error during IDLE state: {e}")
                    time.sleep(1.0)  # Brief delay before retrying

            elif current_state == Q628State.WAIT_START_SW:
                try:  # 💡 Hardened: Exception handling for button detection
                    # When button pressed
                    button_state = GPIO.input(Config.START_TASTE_GPIO)
                    if button_state == GPIO.LOW:
                        # Button pressed, debounce
                        time.sleep(Config.BUTTON_DEBOUNCE_TIME)
                        if GPIO.input(Config.START_TASTE_GPIO) == GPIO.LOW:
                            logger.info("👇 Button pressed, starting measurement")

                            # Store button press time
                            system_state.set_button_press_time()
                            
                            # Turn on High Voltage immediately upon button press
                            logger.info("⚡ Turning on High Voltage immediately upon button press")
                            set_high_voltage(True)
                            
                            # START MEASUREMENT AND SAMPLING
                            system_state.start_measurement()
                            start_sampling()

                            # Reset the last_sent_index for new measurement
                            last_sent_index = 0

                            # Transition to fixed initialization waiting phase
                            system_state.set_q628_state(Q628State.WAIT_HV)

                    # Check for timeout (2 minutes)
                    if elapsed > 120.0:
                        logger.warning("⏰ Timeout waiting for button press")
                        GPIO.output(Config.START_LED_GPIO, GPIO.LOW)

                        # Return to IDLE
                        system_state.set_q628_state(Q628State.IDLE)
                except Exception as e:
                    logger.error(f"❌ Error during WAIT_START_SW state: {e}")
                    # If GPIO reading fails, be conservative and stay in this state
                    time.sleep(0.5)  # Brief delay before retrying

            elif current_state == Q628State.WAIT_HV:
                try:  # 💡 Hardened: Exception handling
                    # Fixed initialization time for HV stabilization
                    if elapsed > Config.FIXED_INITIALIZATION_TIME:
                        logger.info(f"⏱️ Fixed initialization time of {Config.FIXED_INITIALIZATION_TIME}s completed")
                        
                        # 💡 Hardened: Verify HV is actually on
                        if GPIO.input(Config.HVAC_GPIO) != GPIO.HIGH:
                            logger.error("❌ HV not enabled as expected - forcing HV on")
                            set_high_voltage(True)
                            time.sleep(0.2)  # Brief delay before continuing
                            
                        # After fixed time, transition to START state
                        system_state.set_q628_state(Q628State.START)
                except Exception as e:
                    logger.error(f"❌ Error during WAIT_HV state: {e}")
                    time.sleep(0.5)  # Brief delay before retrying

            elif current_state == Q628State.START:
                try:  # 💡 Hardened: Exception handling
                    # Ensure valves are open before moving motor
                    logger.info("🔧 Opening valves and moving motor down")
                    
                    # Open valves (activate both magnets)
                    set_valve(ValveStatus.OPEN)

                                        # Move motor down
                    if not move_motor(MotorPosition.DOWN):
                        logger.error("❌ Motor down command failed - retrying once")
                        time.sleep(1.0)
                        if not move_motor(MotorPosition.DOWN):
                            logger.error("❌ Motor down command failed after retry - continuing anyway")
                    
                    # Store the exact time when motor down command was issued
                    motor_down_time = time.time()
                    logger.info(f"⏱️ Motor down command sent at {motor_down_time:.3f}")

                    # Transition to PRE_ACTIV
                    system_state.set_q628_state(Q628State.PRE_ACTIV)
                except Exception as e:
                    logger.error(f"❌ Error during START state: {e}")
                    # Critical error - attempt recovery
                    time.sleep(1.0)
                    # Try operation again - don't abort measurement
                    logger.warning("⚠️ Retrying valve open and motor down after error")
                    try:
                        set_valve(ValveStatus.OPEN)
                        move_motor(MotorPosition.DOWN)
                        motor_down_time = time.time()
                        system_state.set_q628_state(Q628State.PRE_ACTIV)
                    except Exception as retry_error:
                        logger.error(f"❌ Retry failed: {retry_error}")

            elif current_state == Q628State.PRE_ACTIV:
                try:  # 💡 Hardened: Exception handling
                    # Wait for motor movement to complete (bottom position reached)
                    motor_status = system_state.get("motor_status")
                    
                    # 💡 Hardened: Add timeout check for motor waiting
                    if elapsed > 10.0 and (motor_status & MotorStatus.BUSY):
                        logger.warning("⚠️ Motor still busy after 10s - forcing continuation")
                        motor_status &= ~MotorStatus.BUSY  # Clear busy flag to allow continuation
                    
                    # Check if motor has finished moving (not busy)
                    if not (motor_status & MotorStatus.BUSY) or elapsed > 5.0:
                        # Turn HV off after configured delay since motor reached bottom
                        if motor_down_time is not None:
                            time_since_motor_down = time.time() - motor_down_time
                            
                            # Using the configured delay from Config
                            hv_off_delay = Config.HV_OFF_DELAY_AFTER_DOWN
                            
                            if time_since_motor_down >= hv_off_delay:
                                logger.info(f"⚡ Exactly {hv_off_delay}s after motor down - turning OFF High Voltage")
                                set_high_voltage(False)
                                
                                # 💡 Hardened: Verify HV is actually off
                                time.sleep(0.1)  # Brief delay for GPIO to settle
                                if GPIO.input(Config.HVAC_GPIO) != GPIO.LOW:
                                    logger.error("❌ HV still enabled after turn-off command - forcing off")
                                    # Try again with direct GPIO control as last resort
                                    GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
                                    set_dac_voltage(0.0)
                                
                                # Transition to ACTIV state
                                system_state.set_q628_state(Q628State.ACTIV)
                                logger.info("📊 Active measurement phase started")
                            else:
                                # Not yet reached the configured delay
                                remaining = hv_off_delay - time_since_motor_down
                                logger.debug(f"⏱️ Waiting {remaining:.3f}s more before turning off HV")
                        else:
                            logger.warning("⚠️ Motor down time not recorded, using elapsed time")
                            if elapsed > Config.HV_OFF_DELAY_AFTER_DOWN:
                                logger.info(f"⚡ Turning OFF High Voltage after {Config.HV_OFF_DELAY_AFTER_DOWN}s (using elapsed time fallback)")
                                set_high_voltage(False)
                                
                                # Transition to ACTIV
                                system_state.set_q628_state(Q628State.ACTIV)
                                logger.info("📊 Active measurement phase started")
                except Exception as e:
                    logger.error(f"❌ Error during PRE_ACTIV state: {e}")
                    # Critical phase - don't abort measurement, try to recover
                    time.sleep(1.0)
                    
                    # If we've been in this state too long, force continuation
                    if elapsed > 10.0:
                        logger.warning("⚠️ Forcing transition to ACTIV state after error")
                        # Ensure HV is off before continuing
                        try:
                            set_high_voltage(False)
                        except:
                            # Last resort direct GPIO control
                            GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
                            
                        system_state.set_q628_state(Q628State.ACTIV)

            elif current_state == Q628State.ACTIV:
                try:  # 💡 Hardened: Exception handling
                    # Get precise timing and sample information
                    button_press_time = system_state.get("button_press_time")
                    defined_time = system_state.get("defined_measurement_time")
                    current_virtual_samples = system_state.get("virtual_sample_count")
                    expected_virtual_samples = int(defined_time * 100)
                    
                    # 💡 Hardened: Ensure HV stays off during active measurement
                    if system_state.get("hv_status") or GPIO.input(Config.HVAC_GPIO) == GPIO.HIGH:
                        logger.error("❌ HV unexpectedly ON during ACTIV state - forcing off")
                        set_high_voltage(False)
                    
                    # 💡 Hardened: Ensure we're still sampling
                    if not system_state.is_sampling_active() or not sampling_active.is_set():
                        logger.error("❌ Sampling inactive during ACTIV state - reactivating")
                        system_state.set("sampling_active", True)
                        sampling_active.set()
                    
                    # CRITICAL: Only check for exact sample count, not time
                    if current_virtual_samples >= expected_virtual_samples:
                        logger.info(f"⏱️ Exact sample count reached: {current_virtual_samples}/{expected_virtual_samples}")
                        
                        # Stop sampling
                        stop_sampling()
                        # Move to FINISH state
                        system_state.set_q628_state(Q628State.FINISH)
                    
                    # Log warning if we're running over time but let it continue until sample count is reached
                    elif button_press_time is not None and (time.time() - button_press_time > defined_time + 20):
                        # If we're WAY over time (20+ seconds), log warning but keep going
                        logger.warning(f"⚠️ Measurement running significantly over time but hasn't reached target sample count")
                        logger.warning(f"⚠️ Current: {current_virtual_samples}/{expected_virtual_samples} samples, " +
                                    f"Time: {time.time() - button_press_time:.1f}/{defined_time}s")
                        
                        # 💡 Hardened: Check if we're making progress with sample collection
                        if current_virtual_samples == 0 or (time.time() - button_press_time > defined_time + 60):
                            # If no samples collected or over 60 seconds past the deadline, emergency finish
                            logger.error("❌ Emergency termination of measurement - severe timing problems")
                            stop_sampling()
                            system_state.set_q628_state(Q628State.FINISH)
                    
                    # 💡 Hardened: Periodically log progress
                    if int(elapsed) % 5 == 0 and int(elapsed) != 0:  # Every 5 seconds
                        progress_pct = (current_virtual_samples / expected_virtual_samples) * 100 if expected_virtual_samples > 0 else 0
                        if 0 <= int(elapsed) % 15 < 1:  # Less frequent detailed log every 15 seconds
                            logger.info(f"📊 Measurement progress: {current_virtual_samples}/{expected_virtual_samples} " +
                                      f"virtual samples ({progress_pct:.1f}%), runtime: {runtime}/{defined_time}s")
                    
                    # Short sleep for responsive state changes
                    time.sleep(0.001)
                except Exception as e:
                    logger.error(f"❌ Error during ACTIV state: {e}")
                    # Critical phase - ensure we don't get stuck
                    time.sleep(0.5)
                    
                    # Check if we should force termination
                    if elapsed > defined_time * 1.5:  # 50% over expected duration
                        logger.error("❌ Forcing measurement termination after extended runtime")
                        # Force measurement end
                        stop_sampling()
                        system_state.set_q628_state(Q628State.FINISH)

            elif current_state == Q628State.FINISH:
                try:  # 💡 Hardened: Exception handling
                    # Ensure sampling is stopped
                    if sampling_active.is_set() or system_state.is_sampling_active():
                        stop_sampling()
                        logger.info("📊 Ensuring sampling is stopped")
                    
                    # Open valves before motor movement
                    logger.info("🔧 Opening valves for motor movement")
                    set_valve(ValveStatus.OPEN)
                    
                    # Move motor up
                    logger.info("🔧 Moving motor back up to home position")
                    if not move_motor(MotorPosition.UP):
                        logger.error("❌ Motor up command failed - retrying")
                        time.sleep(1.0)
                        move_motor(MotorPosition.UP)  # Retry once
                    
                    # Wait for motor to reach home position
                    motor_reached_home = wait_for_motor_home(timeout=10)
                    
                    if not motor_reached_home:
                        logger.warning("⚠️ Motor may not have reached home position - continuing anyway")
                    
                    # Wait exactly 3.5 seconds after motor reached position
                    logger.info("⏱️ Waiting exactly 3.5 seconds after motor reached home position")
                    time.sleep(3.5)
                    
                    # Close valves (deactivate magnets)
                    logger.info("🔧 Closing valves 3.5s after motor reached home")
                    set_valve(ValveStatus.CLOSED)

                    # 💡 Hardened: Ensure HV is definitely off
                    if system_state.get("hv_status") or GPIO.input(Config.HVAC_GPIO) == GPIO.HIGH:
                        logger.error("❌ HV unexpectedly ON during FINISH state - forcing off")
                        set_high_voltage(False)

                    # Report sample statistics at end of measurement
                    virtual_samples = system_state.get("virtual_sample_count")
                    real_samples = len(adc_buffer.get_all())
                    defined_time = system_state.get("defined_measurement_time")
                    expected_virtual_samples = int(defined_time * 100)
                    
                    # 💡 Hardened: Calculate sampling quality metrics
                    completion_pct = (virtual_samples / expected_virtual_samples) * 100 if expected_virtual_samples > 0 else 0
                    sampling_quality = "Excellent" if completion_pct >= 99.9 else "Good" if completion_pct >= 95 else "Fair" if completion_pct >= 90 else "Poor"
                    
                    # Provide comprehensive measurement summary
                    logger.info("=" * 80)
                    logger.info(f"📊 MEASUREMENT COMPLETE - Sample Statistics:")
                    logger.info(f"📊 Real samples collected: {real_samples}")
                    logger.info(f"📊 Virtual samples in 100Hz grid: {virtual_samples}")
                    logger.info(f"📊 Expected virtual samples (100Hz): {expected_virtual_samples}")
                    logger.info(f"📊 Real sample rate: {real_samples/defined_time:.2f} Hz")
                    logger.info(f"📊 Virtual sample rate: {virtual_samples/defined_time:.2f} Hz")
                    logger.info(f"📊 Samples sent to client: {last_sent_index}")
                    logger.info(f"📊 Measurement duration: {defined_time}s")
                    logger.info(f"📊 Completion: {completion_pct:.1f}% ({sampling_quality} quality)")
                    logger.info("=" * 80)

                    # 💡 Hardened: Ensure CSV file is properly flushed
                    safe_flush_csv()

                    # End measurement but don't clear buffers
                    system_state.end_measurement()
                    
                    # Return to IDLE
                    system_state.set_q628_state(Q628State.IDLE)
                except Exception as e:
                    logger.error(f"❌ Error during FINISH state: {e}")
                    # Critical cleanup phase - retry essential operations
                    time.sleep(1.0)
                    
                    # Ensure minimal cleanup happens even after error
                    try:
                        stop_sampling()
                        set_high_voltage(False)
                        set_valve(ValveStatus.CLOSED)
                        system_state.end_measurement()
                        system_state.set_q628_state(Q628State.IDLE)
                    except Exception as cleanup_error:
                        logger.error(f"❌ Minimal cleanup also failed: {cleanup_error}")
                        # Last resort - force back to IDLE
                        system_state.set_q628_state(Q628State.IDLE)
            
            # 💡 Hardened: Added RECOVERY state to handle fault conditions
            elif current_state == Q628State.RECOVERY:
                try:
                    logger.warning(f"⚠️ In RECOVERY state, attempting to restore system ({elapsed:.1f}s elapsed)")
                    
                    # If measurement was active, end it properly
                    if system_state.is_measurement_active():
                        logger.warning("⚠️ Ending active measurement during recovery")
                        stop_sampling()
                        system_state.end_measurement()
                    
                    # Ensure HV is off
                    if system_state.get("hv_status") or GPIO.input(Config.HVAC_GPIO) == GPIO.HIGH:
                        logger.warning("⚠️ Turning off HV during recovery")
                        set_high_voltage(False)
                    
                    # Try to return motor to safe position
                    if system_state.get("motor_position") != MotorPosition.UP:
                        logger.warning("⚠️ Moving motor to home position during recovery")
                        set_valve(ValveStatus.OPEN)  # Ensure valves open before motor movement
                        time.sleep(0.5)
                        move_motor(MotorPosition.UP)
                        # Don't wait for completion
                    
                    # Reset all important flags and counters
                    sampling_active.clear()
                    enable_from_pc.clear()
                    simulate_start.clear()
                    
                    # After basic recovery steps, return to IDLE state
                    if elapsed > 5.0:
                        logger.info("✅ Recovery complete, returning to IDLE state")
                        system_state.set_q628_state(Q628State.IDLE)
                        
                except Exception as e:
                    logger.error(f"❌ Error during RECOVERY state: {e}")
                    time.sleep(2.0)  # Longer delay to avoid rapid failures
                    # Force return to IDLE if recovery takes too long
                    if elapsed > 15.0:
                        logger.error("❌ Recovery timed out, forcing return to IDLE")
                        system_state.set_q628_state(Q628State.IDLE)

            # 💡 Hardened: Add default case to handle unknown states
            else:
                logger.error(f"❌ Invalid state: {current_state}")
                # Return to safe IDLE state
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

                # Increment recovery counter
                system_state.set("recovery_attempts", system_state.get("recovery_attempts") + 1)
                
                # Return to IDLE after error if not already in recovery state
                if current_state != Q628State.RECOVERY:
                    system_state.set_q628_state(Q628State.RECOVERY)
            except Exception as cleanup_error:
                logger.critical(f"❌ CRITICAL: Error during emergency cleanup: {cleanup_error}")
                # Last resort attempt to return to IDLE
                system_state.set_q628_state(Q628State.IDLE)

            time.sleep(1.0)  # Longer sleep after error


# 💡 Hardened: Building TTCP data with validation and checksums
def build_ttcp_data() -> bytes:
    """
    Generates a formatted TTCP_DATA response for Delphi using 50Hz samples
    mapped to 100Hz grid. Each real sample is sent twice to fill the 100Hz grid.
    Values are scaled by SCALE_FACTOR to improve visual deflection in Delphi.
    """
    global last_sent_index, sent_packet_counter
    
    try:
        # Increment packet counter
        sent_packet_counter += 1
        
        # Get system status
        state_dict = system_state.get_status_dict()
        defined_time = state_dict["defined_measurement_time"]
        
        # Ensure runtime is an integer
        runtime = int(state_dict["runtime"])  # Convert to integer
        
        current_state = state_dict["q628_state"]
        
        # Initialize buffer for 256 bytes
        data = bytearray(256)

        # 💡 Hardened: Add checksums and packet counter for transport reliability
        packet_id = sent_packet_counter & 0xFFFFFFFF  # 32-bit packet ID

        # Standard fields (1-42)
        struct.pack_into('<i', data, 0, 256)  # Size: 256 bytes
        struct.pack_into('<i', data, 4, runtime)  # Runtime as integer
        
        # Temperature, etc.
        # 💡 Hardened: Validate temperature and humidity ranges
        temp = state_dict["temperature"]
        if temp < -40 or temp > 125:
            temp = max(-40, min(125, temp))  # Clamp to valid range
            
        humidity = state_dict["humidity"]
        if humidity < 0 or humidity > 100:
            humidity = max(0, min(100, humidity))  # Clamp to valid range
            
        temp_int = int(temp * 100)
        struct.pack_into('<i', data, 8, temp_int)
        humidity_int = int(humidity * 100)
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
            
            # 💡 Hardened: Validate data count doesn't exceed available data
            if data_count > len(latest_samples):
                data_count = len(latest_samples)
                logger.warning(f"⚠️ Requested more samples ({data_count}) than available ({len(latest_samples)})")
            
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
            
            # 💡 Hardened: Add packet counter and checksum
            data[54] = (packet_id >> 8) & 0xFF  # High byte of packet ID 
            data[55] = packet_id & 0xFF  # Low byte of packet ID
            
            # Pack the actual data values (latest samples)
            sent_values = []
            
            for i in range(50):
                if i < data_count:
                    # 💡 Hardened: Safely access samples with bounds checking
                    sample_index = len(latest_samples) - data_count + i
                    if 0 <= sample_index < len(latest_samples):
                        raw_value = latest_samples[sample_index]
                    else:
                        raw_value = 0
                        logger.warning(f"⚠️ Sample index out of range: {sample_index} (len={len(latest_samples)})")
                    
                    # Apply scaling factor to improve visualization
                    scaled_value = raw_value * SCALE_FACTOR
                    
                    # Ensure we don't exceed 32-bit integer limits
                    delphi_value = min(2147483647, max(-2147483648, scaled_value))
                    
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
            try:  # 💡 Hardened: Add exception handling
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
                
                # 💡 Hardened: Add packet counter for reliability
                data[54] = (packet_id >> 8) & 0xFF  # High byte of packet ID 
                data[55] = packet_id & 0xFF  # Low byte of packet ID
        
                # Pack the actual data values
                sent_values = []
                
                for i in range(50):
                    if i < data_count:
                        # 💡 Hardened: Safely access sample with bounds checking
                        if i < len(samples_to_send):
                            # Each virtual sample is (value, is_virtual, virtual_idx)
                            value, is_virtual, virtual_idx = samples_to_send[i]
                            
                            # Apply scaling factor to improve visualization
                            scaled_value = value * SCALE_FACTOR
                            
                            # Ensure we don't exceed 32-bit integer limits
                            delphi_value = min(2147483647, max(-2147483648, scaled_value))
                            
                            # Add to our tracking for CSV logging
                            sent_values.append((value, delphi_value, is_virtual, virtual_idx))
                        else:
                            logger.warning(f"⚠️ Sample index {i} out of range (len={len(samples_to_send)})")
                            delphi_value = 0
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
                    if last_sent_index % 500 == 0 or last_sent_index >= total_virtual_samples:
                        progress_pct = (last_sent_index / max(1, expected_virtual_total)) * 100
                        logger.info(f"📊 Progress: {last_sent_index}/{expected_virtual_total} virtual samples sent ({progress_pct:.1f}%)")
                    
                    # Flag when we've sent all available data
                    if last_sent_index >= total_virtual_samples:
                        logger.info(f"📊 Completed sending all {total_virtual_samples} virtual samples to Delphi client")
            
            except Exception as e:
                logger.error(f"❌ Error processing virtual samples: {e}")
                # 💡 Hardened: Send blank packet with error flag if processing fails
                data[42] = 0  # No samples
                data[43] = 1  # Error flag in dummy1 field

        # 💡 Hardened: Add packet checksum
        checksum = 0
        for i in range(255):  # Sum all bytes except the last one (which will be checksum)
            checksum = (checksum + data[i]) & 0xFF
        data[255] = checksum

        return bytes(data)

    except Exception as e:
        logger.error(f"❌ Error creating TTCP_DATA: {e}", exc_info=True)
        
        # 💡 Hardened: Return a properly formatted minimal valid packet on error
        try:
            minimal_data = bytearray(256)
            struct.pack_into('<i', minimal_data, 0, 256)  # Size
            struct.pack_into('<i', minimal_data, 4, 0)    # Runtime
            # Add packet ID and checksum
            minimal_data[54] = (sent_packet_counter >> 8) & 0xFF
            minimal_data[55] = sent_packet_counter & 0xFF
            # Calculate checksum
            checksum = sum(minimal_data[:255]) & 0xFF
            minimal_data[255] = checksum
            return bytes(minimal_data)
        except:
            # Absolute last resort - fixed error packet
            return bytes([0, 1, 0, 0] + [0] * 252)

# 💡 Hardened: TTCP command parsing with extensive validation
def parse_ttcp_cmd(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse Delphi-compatible TTCP_CMD packet with improved debugging
    """
    try:
        # 💡 Hardened: Input validation
        if not data or not isinstance(data, bytes):
            logger.error(f"Invalid TTCP_CMD input: {type(data)}")
            return None
            
        # Show raw command bytes for debugging (first 32 bytes max)
        display_len = min(32, len(data))
        logger.info(f"Raw command data: {data[:display_len].hex()}")
        
        if len(data) < 32:  # Delphi sends 8 x 4-byte ints = 32 bytes
            logger.error(f"Invalid TTCP_CMD size: {len(data)} bytes, expected 32+")
            return None

        # Unpack 8 integers from the data
        try:
            values = struct.unpack("<8i", data[:32])
            size, cmd_int, runtime, hvac, hvdc, profile1, profile2, wait_acdc = values
        except struct.error as e:
            logger.error(f"❌ Error unpacking TTCP_CMD: {e}")
            return None
        
        # Debug log the integer values
        logger.info(f"Unpacked command: size={size}, cmd_int={cmd_int}, runtime={runtime}, " +
                   f"hvac={hvac}, hvdc={hvdc}, profile1={profile1}, profile2={profile2}, wait_acdc={wait_acdc}")

        # 💡 Hardened: Validate field ranges
        if size != 32 and size != 256:  # Common expected sizes
            logger.warning(f"⚠️ Unusual TTCP_CMD size: {size}")
            
        if runtime < 0 or runtime > 3600:  # 0 to 1 hour
            logger.warning(f"⚠️ Runtime out of normal range: {runtime}s")
            runtime = max(0, min(3600, runtime))  # Clamp to valid range
            
        if hvac < 0 or hvac > 500:  # 0 to 500V
            logger.warning(f"⚠️ HVAC out of normal range: {hvac}V")
            hvac = max(0, min(500, hvac))  # Clamp to valid range
            
        if hvdc < 0 or hvdc > 7000:  # 0 to 7000V
            logger.warning(f"⚠️ HVDC out of normal range: {hvdc}V")
            hvdc = max(0, min(7000, hvdc))  # Clamp to valid range

        # Convert command integer to character
        # This is critical - cmd_int is the ASCII value of the command character
        if not (32 <= cmd_int <= 126):
            logger.warning(f"⚠️ Command code {cmd_int} outside printable ASCII range")
            cmd_char = '?'
        else:
            cmd_char = chr(cmd_int)
            
        logger.info(f"Command character from ASCII {cmd_int}: '{cmd_char}'")
        
        # Map to command type
        cmd_type = next((c for c in CommandType if c.value == cmd_char), CommandType.UNKNOWN)
        logger.info(f"Mapped to command type: {cmd_type.name}")

        # Calculate DAC voltage for HV DC value using 1:2000 ratio
        dac_voltage = hvdc / 2000.0
        logger.info("Parsed TTCP_CMD: %s (runtime=%ds, HVAC=%d, HVDC=%d → DAC=%fV)",
                    cmd_char, runtime, hvac, hvdc, dac_voltage)

        # Add 0.4 seconds compensation to runtime for all commands
        # Store as a float internally, but we'll convert to int when needed
        adjusted_runtime = runtime + 0.25  # Add 250ms compensation

        # Update system state with received parameters and adjusted runtime
        system_state.update(
            profile_hin=profile1,
            profile_zurueck=profile2,
            wait_acdc=wait_acdc,
            hv_ac_value=hvac,
            hv_dc_value=hvdc,
            defined_measurement_time=adjusted_runtime
        )

        # Log the adjustment when it's a START command
        if cmd_type == CommandType.START:
            logger.info(f"🔄 START COMMAND RECOGNIZED!")
            logger.info(f"📏 Set defined measurement time to {runtime} seconds (adjusted to {adjusted_runtime} seconds)")
            logger.info(f"⚡ Set HV DC value to {hvdc}V (DAC voltage will be {dac_voltage:.4f}V)")

        # 💡 Hardened: Create command history for debugging
        timestamp = time.time()
        cmd_history = {
            'timestamp': timestamp,
            'cmd_type': cmd_type.name,
            'runtime': runtime,
            'hvac': hvac, 
            'hvdc': hvdc,
            'adjusted_runtime': adjusted_runtime
        }
        
        return {
            'size': size,
            'cmd': cmd_char,
            'cmd_type': cmd_type,
            'runtime': runtime,  # Keep the original integer runtime here
            'hvac': hvac,
            'hvdc': hvdc,
            'profile1': profile1,
            'profile2': profile2,
            'wait_acdc': wait_acdc,
            'timestamp': timestamp,  # 💡 Hardened: Add timestamp for command tracking
            'history': cmd_history   # 💡 Hardened: Add command history
        }
    except Exception as e:
        logger.error(f"Error parsing TTCP_CMD: {e}", exc_info=True)
        return None


# 💡 Hardened: Client connection handler with error rate limiting and recovery
def handle_client(conn: socket.socket, addr: Tuple[str, int]) -> None:
    """
    Handle client connection with proper command handling
    """
    global last_sent_index
    
    client_id = f"{addr[0]}:{addr[1]}"
    logger.info("🔌 Connected to %s", client_id)
    system_state.increment_clients()
    system_state.set("last_ip", addr[0])

    # 💡 Hardened: Track per-client statistics
    client_stats = {
        "connect_time": time.time(),
        "packets_sent": 0,
        "packets_received": 0,
        "last_command": None,
        "last_activity": time.time(),
        "errors": 0
    }

    # Set TCP_NODELAY for this connection (critical for Delphi)
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.settimeout(1.0)
        
        # 💡 Hardened: Set additional socket options for stability
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Set TCP keepalive settings if OS supports them
        try:
            # TCP Keepalive after 60 seconds of inactivity
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            # Send keepalive every 10 seconds
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            # Try 5 times before giving up
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
        except (AttributeError, OSError):
            # Not all systems support these options
            pass
    except Exception as e:
        logger.warning("⚠️ Could not set TCP socket options for %s: %s", client_id, e)

    # Track consecutive errors
    error_count = 0
    MAX_ERRORS = 10
    
    # 💡 Hardened: Track error rate limiting
    error_times = deque(maxlen=10)  # Keep last 10 error timestamps
    last_error_reset = time.time()

    # Send immediate response on connection
    response = build_ttcp_data()
    try:
        conn.sendall(response)
        client_stats["packets_sent"] += 1
        logger.info("📤 Sent initial data packet to %s (%d bytes)", client_id, len(response))
    except Exception as e:
        error_count += 1
        error_times.append(time.time())
        logger.error("❌ Error sending initial packet to %s: %s", client_id, e)
        conn.close()
        system_state.decrement_clients()
        return

    # Track last data send time for periodic updates
    last_data_send_time = time.time()
    
    try:
        while not shutdown_requested.is_set():
            current_time = time.time()
            client_stats["last_activity"] = current_time
            
            # 💡 Hardened: Reset error count periodically if no recent errors
            if error_count > 0 and current_time - last_error_reset > 60:
                # If no errors in the last minute, gradually reduce error count
                errors_in_last_minute = sum(1 for t in error_times if current_time - t < 60)
                if errors_in_last_minute == 0:
                    error_count = max(0, error_count - 1)
                    last_error_reset = current_time
                    logger.debug(f"🔄 Reduced error count to {error_count} after 1 minute with no errors")
            
            # Send periodic status updates in IDLE state
            if (system_state.get_q628_state() == Q628State.IDLE and 
                current_time - last_data_send_time >= 0.5):
                try:
                    response = build_ttcp_data()
                    conn.sendall(response)
                    last_data_send_time = current_time
                    client_stats["packets_sent"] += 1
                    logger.debug("📤 Sent periodic status update in IDLE state")
                except Exception as e:
                    error_count += 1
                    error_times.append(current_time)
                    logger.error(f"❌ Error sending periodic status: {e} (error #{error_count})")
                    if error_count >= MAX_ERRORS:
                        break

            # Receive command with timeout
            try:
                # 💡 Hardened: Use select to detect if data is available before recv
                ready_to_read, _, _ = select.select([conn], [], [], 0.1)
                if ready_to_read:
                    try:
                        cmd_data = conn.recv(Config.BUFFER_SIZE)
                        if not cmd_data:
                            logger.info("👋 Client %s disconnected", client_id)
                            break
                            
                        client_stats["packets_received"] += 1
                        client_stats["last_activity"] = time.time()

                        # Log received command in detail
                        logger.info("Received from %s (%d bytes): %s", 
                                   client_id, len(cmd_data), cmd_data.hex())

                        # Reset error count on successful receive
                        error_count = 0

                        # Parse the command if it looks like a valid TTCP_CMD
                        cmd_info = None
                        if len(cmd_data) >= 32:
                            cmd_info = parse_ttcp_cmd(cmd_data)

                        if cmd_info:
                            # 💡 Hardened: Update client history
                            client_stats["last_command"] = cmd_info["cmd_type"].name
                            
                            logger.info(f"✅ Command recognized: {cmd_info['cmd']} - Current state: {system_state.get_q628_state().name}")
                            # Process command based on type
                            if cmd_info['cmd_type'] == CommandType.START:
                                logger.info(f"⚙️ START COMMAND received: Measurement time={cmd_info['runtime']}s, HVAC={cmd_info['hvac']}V, HVDC={cmd_info['hvdc']}V")
                                system_state.set("defined_measurement_time", cmd_info['runtime'])
                                
                                # Set enable_from_pc flag for manual start mode
                                enable_from_pc.set()
                                logger.info("✅ Enable flag set - waiting for button press")
                                
                                # Reset last_sent_index for new measurement
                                last_sent_index = 0

                            elif cmd_info['cmd_type'] == CommandType.TRIGGER:  # Simulate button press
                                logger.info("🔄 TRIGGER command received (simulate button press)")
                                simulate_start.set()

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

                            else:
                                logger.warning(f"⚠️ Unknown command: {cmd_info['cmd']}")
                        else:
                            # Could not parse as a valid command
                            logger.warning(f"⚠️ Received invalid command data: {cmd_data.hex()}")
                    except ConnectionResetError:
                        logger.warning("🔌 Connection reset by %s", client_id)
                        break
                    except socket.timeout:
                        # Timeout during recv after select said data was available
                        logger.warning(f"⚠️ Socket timeout after select indicated data for {client_id}")
                        continue
                    except Exception as e:
                        error_count += 1
                        error_times.append(current_time)
                        logger.error("❌ Error receiving data from %s: %s (error #%d)",
                                     client_id, e, error_count)
                        
                        if error_count >= MAX_ERRORS:
                            logger.error("⛔ Too many consecutive errors, closing connection to %s", client_id)
                            break
                        
                        # Short pause before retry
                        time.sleep(0.1)
                        continue

            except select.error as e:
                error_count += 1
                error_times.append(current_time)
                logger.error(f"❌ Select error with {client_id}: {e} (error #{error_count})")
                time.sleep(0.1)
                continue
            except socket.timeout:
                # Just continue the loop on timeout
                continue
            except Exception as e:
                error_count += 1
                error_times.append(current_time)
                logger.error("❌ Unhandled error with %s: %s (error #%d)",
                             client_id, e, error_count)
                
                if error_count >= MAX_ERRORS:
                    logger.error("⛔ Too many errors, closing connection to %s", client_id)
                    break
                    
                time.sleep(0.1)
                continue

            # Always send a response packet with current status after command processing
            # if we've received a command or it's been more than 0.5s since the last update
            if ready_to_read or current_time - last_data_send_time >= 0.5:
                try:
                    response = build_ttcp_data()
                    last_data_send_time = current_time
                    conn.sendall(response)
                    client_stats["packets_sent"] += 1
                    logger.debug("📤 Sent response to %s (%d bytes)", client_id, len(response))

                    # Reset error count on successful send
                    error_count = 0

                except socket.timeout:
                    error_count += 1
                    error_times.append(current_time)
                    logger.warning("⏱️ Timeout sending response to %s (error #%d)",
                                   client_id, error_count)
                except Exception as e:
                    error_count += 1
                    error_times.append(current_time)
                    logger.error("❌ Error sending response to %s: %s (error #%d)",
                                 client_id, e, error_count)

                    if error_count >= MAX_ERRORS:
                        logger.error("⛔ Too many consecutive errors, closing connection to %s", client_id)
                        break
            
            # Update previous state for state transition detection
            system_state.set("prev_q628_state", system_state.get_q628_state())

    except Exception as e:
        logger.error("❌ Unhandled error with client %s: %s", client_id, e, exc_info=True)
    finally:
        # 💡 Hardened: Log client statistics at disconnect
        connection_duration = time.time() - client_stats["connect_time"]
        logger.info(f"📊 Client {client_id} statistics: Connected for {connection_duration:.1f}s, " +
                  f"Sent {client_stats['packets_sent']} packets, Received {client_stats['packets_received']} packets, " +
                  f"Errors: {client_stats['errors']}")
        
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
        # 💡 Hardened: Check if there's already a lock file for this process
        lock_file = f"/tmp/qumat628_port_{port}.lock"
        if os.path.exists(lock_file):
            try:
                with open(lock_file, 'r') as f:
                    pid = int(f.read().strip())
                    if psutil.pid_exists(pid):
                        logger.warning(f"⚠️ Port {port} is claimed by PID {pid} according to lock file")
                        # Check if this is our own process
                        if pid == os.getpid():
                            logger.info(f"✅ Port {port} is already owned by this process")
                            return
            except:
                # Lock file invalid or can't be read
                pass
            
        # Try to detect and kill processes using the port
        success = False
        for attempt in range(3):  # Try up to 3 times
            try:
                output = subprocess.check_output(f"lsof -t -i:{port}", shell=True).decode()
                found_pids = False
                
                for pid in output.strip().split('\n'):
                    if pid:  # Check if pid is not empty
                        found_pids = True
                        try:
                            pid_int = int(pid)
                            # Don't kill ourselves
                            if pid_int != os.getpid():
                                logger.info(f"🔪 Terminating process {pid} using port {port}")
                                os.kill(pid_int, signal.SIGTERM)
                        except Exception as e:
                            logger.error(f"❌ Failed to kill process {pid}: {e}")
                
                if not found_pids:
                    logger.info(f"✅ No processes found using port {port}")
                    success = True
                    break
                    
                # Wait a bit and check if port is truly free
                time.sleep(1.0)
                try:
                    # Try to bind to the port to see if it's free
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    test_socket.bind(('0.0.0.0', port))
                    test_socket.close()
                    logger.info(f"✅ Successfully bound to port {port} - it's free")
                    success = True
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Port {port} still in use after killing processes: {e}")
                    # Try more aggressive SIGKILL on next attempt
                    if attempt > 0:
                        try:
                            output = subprocess.check_output(f"lsof -t -i:{port}", shell=True).decode()
                            for pid in output.strip().split('\n'):
                                if pid and int(pid) != os.getpid():
                                    logger.warning(f"⚠️ Sending SIGKILL to {pid}")
                                    os.kill(int(pid), signal.SIGKILL)
                        except:
                            pass
            except subprocess.CalledProcessError:
                # This is expected if no process is using the port
                logger.info(f"✅ Port {port} is free (no processes found)")
                success = True
                break
            except Exception as e:
                logger.warning(f"⚠️ Error checking port {port}: {e}")
            
            time.sleep(1.0)  # Wait before retry
        
        if success:
            # Create a lock file to indicate we're using this port
            try:
                with open(lock_file, 'w') as f:
                    f.write(str(os.getpid()))
                logger.debug(f"💡 Created port lock file: {lock_file}")
            except Exception as e:
                logger.warning(f"⚠️ Could not create port lock file: {e}")
        else:
            logger.error(f"❌ Failed to free port {port} after multiple attempts")
    except Exception as e:
        # 💡 Hardened: Catch any unhandled exceptions in the port freeing process
        logger.error(f"❌ Error freeing port {port}: {e}")
    


# 💡 Hardened: Robust server with auto-recovery and connection tracking
def ttcp_server(port: int) -> None:
    """
    TCP server with proper socket options and error handling
    
    Args:
        port: TCP port to listen on
    """
    logger.info("Starting TTCP server on port %d...", port)

    server = None
    connections = {}  # Track active connections
    bind_failures = 0
    max_bind_failures = 5
    last_socket_check = time.time()
    
    # 💡 Hardened: Kill any existing processes using this port
    free_port(port)

    while not shutdown_requested.is_set():
        try:
            # Check if need to create a new socket
            if server is None:
                # Create socket with SO_REUSEADDR
                server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                
                # 💡 Hardened: Set additional socket options for stability
                try:
                    # Disable Nagle's algorithm for responsiveness
                    server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    # Allow socket reuse - helps with restarts
                    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    # Set timeout for accept
                    server.settimeout(1.0)
                except:
                    # Not all systems support these options
                    pass
                
                # Try to bind, might fail if port is in use
                try:
                    server.bind(('0.0.0.0', port))
                    bind_failures = 0  # Reset counter on success
                except OSError as e:
                    bind_failures += 1
                    logger.error(f"⚠️ Could not bind to port {port}: {e} (attempt {bind_failures}/{max_bind_failures})")
                    
                    if bind_failures >= max_bind_failures:
                        logger.error(f"❌ Could not bind to port {port} after {max_bind_failures} attempts")
                        # Try more aggressively freeing the port
                        free_port(port)
                        bind_failures = 0  # Reset for next attempt
                        
                    time.sleep(5)
                    # Close socket and retry
                    try:
                        server.close()
                    except:
                        pass
                    server = None
                    continue
                    
                server.listen(5)
                logger.info(f"✅ TTCP server running on port {port}")

            # 💡 Hardened: Periodically check socket health
            current_time = time.time()
            if current_time - last_socket_check > 60:  # Every minute
                last_socket_check = current_time
                
                # Check active connections and clean up dead ones
                dead_connections = []
                for client_id, thread in list(connections.items()):
                    if not thread.is_alive():
                        dead_connections.append(client_id)
                
                for client_id in dead_connections:
                    logger.warning(f"⚠️ Removing dead connection thread for {client_id}")
                    connections.pop(client_id, None)
                
                # Log active connection count
                connection_count = len(connections)
                logger.info(f"📊 Active connections: {connection_count}")

            # Accept connections in a loop
            try:
                # Use a timeout to allow checking shutdown_requested
                conn, addr = server.accept()
                client_id = f"{addr[0]}:{addr[1]}"
                
                # Start a new thread to handle this client
                client_thread = threading.Thread(
                    target=handle_client,
                    args=(conn, addr),
                    daemon=True,
                    name=f"ClientHandler-{client_id}"
                )
                client_thread.start()
                
                # Track the connection
                connections[client_id] = client_thread
                
                # Register thread for health monitoring
                register_thread(f"ClientHandler-{client_id}", client_thread)
                
                logger.info(f"✅ Started thread for client {client_id}, total connections: {len(connections)}")

            except socket.timeout:
                # This is expected due to the timeout we set
                continue
            except ConnectionResetError:
                logger.warning("⚠️ Connection reset during accept")
                # Recreate socket on next iteration
                try:
                    server.close()
                except:
                    pass
                server = None
                time.sleep(1)
            except Exception as e:
                if not shutdown_requested.is_set():
                    logger.error(f"❌ Error accepting connection: {e}")
                    # Try to recreate the socket
                    try:
                        server.close()
                    except:
                        pass
                    server = None
                    time.sleep(1)

        except Exception as e:
            logger.error(f"❌ Critical error in TTCP server: {e}", exc_info=True)

            # Close the server socket if it exists
            if server:
                try:
                    server.close()
                except:
                    pass
                server = None

            # Wait before retrying
            logger.info(f"⏳ Waiting 5 seconds before restarting server on port {port}...")
            time.sleep(5)

    # Shutdown handling
    logger.info(f"🛑 TTCP server on port {port} shutting down")
    
    # Close server socket
    if server:
        try:
            server.close()
        except:
            pass
    
    # Clean up lock file
    try:
        lock_file = f"/tmp/qumat628_port_{port}.lock"
        if os.path.exists(lock_file):
            os.remove(lock_file)
            logger.debug(f"🧹 Removed port lock file: {lock_file}")
    except:
        pass
    
    logger.info(f"✅ TTCP server on port {port} stopped")


# 💡 Hardened: Enhanced status monitor with resource checking
def status_monitor() -> None:
    """Monitor and log system status periodically"""
    logger.info("📊 Status monitor started")
    thread_heartbeat("StatusMonitor")  # 💡 Hardened: Register thread heartbeat

    disk_warning_sent = False
    cpu_warning_sent = False
    uptime_str = "0m"
    
    while not shutdown_requested.is_set():
        try:
            # 💡 Hardened: Periodically report thread is alive
            thread_heartbeat("StatusMonitor")
            
            # Get system state
            state_dict = system_state.get_status_dict()
            motor_status = state_dict["motor_status"]
            q628_state = state_dict["q628_state"]

            # Calculate uptime
            uptime_seconds = time.time() - system_startup_time
            if uptime_seconds < 3600:
                uptime_str = f"{int(uptime_seconds / 60)}m"
            else:
                uptime_str = f"{uptime_seconds / 3600:.1f}h"

            # 💡 Hardened: Check system resources
            try:
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=0.1)
                
                # Memory usage
                memory = psutil.virtual_memory()
                memory_percent = memory.percent
                
                # Disk space
                disk = psutil.disk_usage('/')
                disk_free_mb = disk.free / (1024 * 1024)
                disk_percent = disk.percent
                
                # Thread count
                thread_count = threading.active_count()
                
                # Include resource info in status
                resource_status = (
                    f"CPU={cpu_percent:.1f}%, "
                    f"Mem={memory_percent:.1f}%, "
                    f"Disk={disk_free_mb:.1f}MB free ({disk_percent}% used), "
                    f"Threads={thread_count}, "
                    f"Uptime={uptime_str}"
                )
                
                # Check resource thresholds and warn if needed
                if disk_free_mb < Config.MIN_FREE_DISK_SPACE_MB and not disk_warning_sent:
                    logger.warning(f"⚠️ Low disk space: {disk_free_mb:.1f}MB free (minimum: {Config.MIN_FREE_DISK_SPACE_MB}MB)")
                    disk_warning_sent = True
                elif disk_free_mb >= Config.MIN_FREE_DISK_SPACE_MB * 1.5:
                    disk_warning_sent = False  # Reset warning flag if back to normal
                    
                if cpu_percent > Config.MAX_CPU_USAGE_PCT and not cpu_warning_sent:
                    logger.warning(f"⚠️ High CPU usage: {cpu_percent:.1f}% (threshold: {Config.MAX_CPU_USAGE_PCT}%)")
                    cpu_warning_sent = True
                elif cpu_percent <= Config.MAX_CPU_USAGE_PCT * 0.8:
                    cpu_warning_sent = False  # Reset warning flag
                
                # Update system state with resource metrics
                system_state.update(
                    cpu_usage=cpu_percent,
                    mem_usage=memory_percent,
                    disk_free_mb=disk_free_mb,
                    threads_active=thread_count
                )
                
            except Exception as resource_error:
                logger.error(f"❌ Error checking system resources: {resource_error}")
                resource_status = "Resource monitoring error"

            # Construct full status message
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
                f"Runtime={state_dict['runtime']}s, "
                f"{resource_status}"
            )
            logger.info("📊 %s", status)

            # Add sampling status
            if state_dict['measurement_active']:
                virtual_samples = state_dict['virtual_sample_count']
                real_samples = len(adc_buffer.get_all())
                expected = int(state_dict['defined_measurement_time'] * 100)
                logger.info(f"📊 Sample status: {virtual_samples}/{expected} virtual samples ({virtual_samples/expected*100:.1f}%)")
                logger.info(f"📊 Real samples: {real_samples}, Virtual samples: {virtual_samples}")

            # 💡 Hardened: Check thread health
            thread_health = check_thread_health()
            if not thread_health["healthy"]:
                logger.warning(f"⚠️ Unhealthy threads detected: {', '.join(thread_health['unhealthy_threads'])}")

            # Check IP address periodically
            if state_dict["cycle_count"] % 5 == 0:
                get_current_ip()

        except Exception as e:
            logger.error(f"❌ Error in status monitor: {e}")
            time.sleep(5)  # Longer sleep on error

        # Sleep for the configured interval, but check shutdown_requested more frequently
        for _ in range(Config.STATUS_UPDATE_INTERVAL):
            if shutdown_requested.is_set():
                break
            time.sleep(1)

    logger.info("📊 Status monitor terminated")


# 💡 Hardened: Improved IP address detection with multiple interfaces
def get_current_ip() -> str:
    """Get current IP address with error handling and fallback"""
    try:
        # Try multiple methods to find the IP
        ip_methods = []
        
        # Method 1: Try to get IP from hostname command
        try:
            output = subprocess.check_output("hostname -I", shell=True)
            ip_addresses = output.decode().strip().split()

            if ip_addresses:
                # Filter out localhost and IPv6 addresses
                valid_ips = [ip for ip in ip_addresses
                             if ip != "127.0.0.1" and ":" not in ip]

                if valid_ips:
                    ip = valid_ips[0]  # Take first valid IP
                    ip_methods.append(("hostname -I", ip))
        except Exception as e:
            logger.debug(f"💡 hostname -I method failed: {e}")

        # Method 2: Try socket method
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                # This doesn't actually establish a connection
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
                ip_methods.append(("socket method", ip))
        except Exception as e:
            logger.debug(f"💡 Socket method failed: {e}")
            
        # Method 3: Try using 'ip' command
        try:
            output = subprocess.check_output("ip -4 addr show | grep -oP '(?<=inet\\s)\\d+(\\.\\d+){3}'", shell=True)
            ip_addresses = output.decode().strip().split('\n')
            valid_ips = [ip for ip in ip_addresses if ip != "127.0.0.1"]
            if valid_ips:
                ip = valid_ips[0]
                ip_methods.append(("ip command", ip))
        except Exception as e:
            logger.debug(f"💡 ip command method failed: {e}")
        
        # Method 4: Try 'ifconfig' command
        try:
            output = subprocess.check_output("ifconfig | grep -oP '(?<=inet\\s)\\d+(\\.\\d+){3}'", shell=True)
            ip_addresses = output.decode().strip().split('\n')
            valid_ips = [ip for ip in ip_addresses if ip != "127.0.0.1"]
            if valid_ips:
                ip = valid_ips[0]
                ip_methods.append(("ifconfig", ip))
        except Exception as e:
            logger.debug(f"💡 ifconfig method failed: {e}")
        
        # If we found any IPs, use the first one that isn't localhost
        if ip_methods:
            method, ip = ip_methods[0]
            logger.info(f"🌐 Current IP ({method}): {ip}")
            system_state.set("last_ip", ip)
            return ip
        
        # If all methods fail, use default
        logger.warning("⚠️ All IP detection methods failed")
        return "0.0.0.0"

    except Exception as e:
        logger.error(f"❌ Could not determine IP address: {e}")
        return "0.0.0.0"


# 💡 Hardened: LED blinking with pattern validation and error recovery
def blink_led(mode: str = "slow", duration: float = 3.0) -> None:
    """
    Blink LED with specified pattern

    Args:
        mode: "slow" or "fast" blinking
        duration: How long to blink in seconds
    """
    # 💡 Hardened: Validate inputs
    if mode not in ["slow", "fast", "sos"]:
        logger.warning(f"⚠️ Invalid LED blink mode: {mode}, using slow")
        mode = "slow"
        
    if duration <= 0 or duration > 60:
        logger.warning(f"⚠️ Invalid LED blink duration: {duration}, using 3.0s")
        duration = 3.0

    interval = (Config.LED_BLINK_FAST if mode == "fast"
                else Config.LED_BLINK_SLOW)

    logger.debug(f"💡 LED blinking {mode} for {duration:.1f}s...")

    try:
        end_time = time.time() + duration
        error_count = 0

        while time.time() < end_time and not shutdown_requested.is_set():
            try:
                GPIO.output(Config.START_LED_GPIO, GPIO.HIGH)
                time.sleep(interval)
                if shutdown_requested.is_set():
                    break
                GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
                time.sleep(interval)
                
                # Reset error count on successful blink
                error_count = 0
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Error during LED blink cycle: {e}")
                
                # If too many errors, try to reset GPIO mode
                if error_count > 3:
                    try:
                        GPIO.setmode(GPIO.BCM)
                        GPIO.setup(Config.START_LED_GPIO, GPIO.OUT)
                        logger.info("✅ Reset GPIO mode for LED after errors")
                    except:
                        pass
                
                time.sleep(0.2)  # Brief delay on error
    except Exception as e:
        logger.error(f"❌ Error during LED blinking: {e}")
    finally:
        # Ensure LED is off when done
        try:
            GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
        except Exception as final_error:
            logger.error(f"❌ Error turning off LED at end of blink: {final_error}")


# 💡 Hardened: Robust button wait with noise filtering
def wait_for_button(msg: str, timeout: Optional[float] = None) -> bool:
    """
    Wait for button press with timeout and debouncing

    Args:
        msg: Message to log while waiting
        timeout: Optional timeout in seconds

    Returns:
        True if button was pressed, False if timeout or shutdown requested
    """
    logger.info(f"⏳ {msg} (waiting for button press)...")

    start_time = time.time()
    debounce_time = Config.BUTTON_DEBOUNCE_TIME
    
    # 💡 Hardened: Track button state for noise detection
    consecutive_low_samples = 0
    required_consecutive_low = 3  # Require 3 consecutive LOW samples for better noise immunity
    last_report_time = start_time

    # Wait for button press (HIGH to LOW transition)
    while True:
        try:
            # Check for shutdown or timeout
            if shutdown_requested.is_set():
                return False
                
            current_time = time.time()
            if timeout and (current_time - start_time > timeout):
                logger.info(f"⏰ Timeout waiting for button press ({timeout}s)")
                return False
                
            # Periodically report we're still waiting
            if current_time - last_report_time >= 30.0:  # Every 30 seconds
                elapsed = current_time - start_time
                logger.info(f"⏳ Still waiting for button press (elapsed: {elapsed:.1f}s)")
                last_report_time = current_time
            
            # Sample button state
            button_state = GPIO.input(Config.START_TASTE_GPIO)
            
            # Track consecutive LOW samples for noise immunity
            if button_state == GPIO.LOW:
                consecutive_low_samples += 1
                if consecutive_low_samples >= required_consecutive_low:
                    # Enough consecutive LOW samples - consider button pressed
                    logger.info(f"👇 Button press detected ({consecutive_low_samples} consecutive LOW samples)")
                    break
            else:
                consecutive_low_samples = 0  # Reset counter on HIGH
            
            time.sleep(Config.BUTTON_POLL_INTERVAL)
        except Exception as e:
            logger.error(f"❌ Error checking button state: {e}")
            time.sleep(0.5)  # Longer delay on error

    # Button pressed - wait for debounce
    time.sleep(debounce_time)
    
    # Verify button is still pressed after debounce
    try:
        if GPIO.input(Config.START_TASTE_GPIO) == GPIO.HIGH:
            # False trigger
            logger.warning("⚠️ Button press was noise (HIGH after debounce) - continuing to wait")
            return wait_for_button(msg, timeout)
    except Exception as e:
        logger.error(f"❌ Error during button debounce: {e}")

    logger.info("👇 Button pressed confirmed")

    # Set button press time for timer calculation
    system_state.set_button_press_time()

    # Wait for button release (LOW to HIGH transition)
    release_start_time = time.time()
    release_timeout = 10.0  # Maximum time to wait for release
    
    while True:
        try:
            if shutdown_requested.is_set():
                return False
                
            # Check for release timeout
            if time.time() - release_start_time > release_timeout:
                logger.warning(f"⚠️ Button release timeout ({release_timeout}s) - assuming released")
                break
                
            if GPIO.input(Config.START_TASTE_GPIO) == GPIO.HIGH:
                # Button released
                break
                
            time.sleep(Config.BUTTON_POLL_INTERVAL)
        except Exception as e:
            logger.error(f"❌ Error waiting for button release: {e}")
            time.sleep(0.5)

    # Button released - debounce
    time.sleep(debounce_time)
    logger.info("👆 Button released")
    return True


# 💡 Hardened: Enhanced cleanup with priority order and retry
def cleanup() -> None:
    """Clean up hardware resources"""
    logger.info("🧹 Starting hardware cleanup...")
    
    # 💡 Hardened: Track completion status
    cleanup_status = {
        "hv_disabled": False,
        "led_off": False,
        "valves_closed": False,
        "motor_safe": False,
        "gpio_cleanup": False,
        "csv_closed": False
    }
    
    # Track errors
    cleanup_errors = []

    try:
        # 💡 Hardened: Safety first - ensure HV is off with multiple retries
        for retry in range(3):
            try:
                set_high_voltage(False)
                # Verify HV is actually off
                if GPIO.input(Config.HVAC_GPIO) == GPIO.LOW:
                    cleanup_status["hv_disabled"] = True
                    logger.info("✅ HV turned OFF during cleanup")
                    break
                else:
                    logger.warning(f"⚠️ HV GPIO still HIGH after turning off (retry {retry+1}/3)")
                    # Try direct GPIO control as last resort
                    GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"❌ Error turning off HV during cleanup (attempt {retry+1}): {e}")
                # Last resort direct GPIO control
                try:
                    GPIO.output(Config.HVAC_GPIO, GPIO.LOW)
                except:
                    pass
                time.sleep(0.1)
        
        # Turn off LED
        try:
            GPIO.output(Config.START_LED_GPIO, GPIO.LOW)
            cleanup_status["led_off"] = True
            logger.info("✅ LED turned OFF during cleanup")
        except Exception as e:
            cleanup_errors.append(f"LED off error: {e}")
            logger.error(f"❌ Error turning off LED: {e}")

        # Close any open CSV file
        try:
            global csv_file_handle
            if csv_file_handle is not None:
                with csv_file_lock:
                    csv_file_handle.flush()
                    csv_file_handle.close()
                    csv_file_handle = None
                cleanup_status["csv_closed"] = True
                logger.info("✅ CSV file closed during cleanup")
        except Exception as e:
            cleanup_errors.append(f"CSV close error: {e}")
            logger.error(f"❌ Error closing CSV file during cleanup: {e}")

        # Close valves and reset outputs
        try:
            GPIO.output(Config.MAGNET1_GPIO, GPIO.LOW)
            GPIO.output(Config.MAGNET2_GPIO, GPIO.LOW)
            cleanup_status["valves_closed"] = True
            logger.info("✅ Valves closed during cleanup")
        except Exception as e:
            cleanup_errors.append(f"Valve error: {e}")
            logger.error(f"❌ Error closing valves: {e}")
        
        # 💡 Hardened: Try to ensure motor is in safe position
        try:
            # Check if motor is already in home (up) position
            motor_status = read_motor_status()
            if motor_status and not (motor_status & MotorStatus.HOME):
                logger.info("🏠 Moving motor to home position during cleanup")
                # Ensure valves are open before moving motor
                try:
                    GPIO.output(Config.MAGNET1_GPIO, GPIO.HIGH)
                    GPIO.output(Config.MAGNET2_GPIO, GPIO.HIGH)
                    time.sleep(0.5)  # Brief delay
                except:
                    pass
                
                move_motor(MotorPosition.UP)
                # Don't wait, just initiate move
            
            cleanup_status["motor_safe"] = True
        except Exception as e:
            cleanup_errors.append(f"Motor safe position error: {e}")
            logger.error(f"❌ Error moving motor to safe position: {e}")

        # Cleanup GPIO
        try:
            GPIO.cleanup()
            cleanup_status["gpio_cleanup"] = True
            logger.info("✅ GPIO cleanup completed")
        except Exception as e:
            cleanup_errors.append(f"GPIO cleanup error: {e}")
            logger.error(f"❌ Error during GPIO cleanup: {e}")

        # 💡 Hardened: Final cleanup summary
        success_items = sum(1 for status in cleanup_status.values() if status)
        total_items = len(cleanup_status)
        if success_items == total_items:
            logger.info("✅ Hardware cleanup completed successfully")
        else:
            failed_items = [item for item, status in cleanup_status.items() if not status]
            logger.warning(f"⚠️ Hardware cleanup incomplete ({success_items}/{total_items} items succeeded)")
            logger.warning(f"⚠️ Failed cleanup items: {', '.join(failed_items)}")
            
        if cleanup_errors:
            logger.warning(f"⚠️ Cleanup errors: {'; '.join(cleanup_errors)}")
    except Exception as e:
        logger.critical(f"❌ CRITICAL: Unhandled error during cleanup: {e}", exc_info=True)


# 💡 Hardened: Improved main function with signal handling and resource tracking
def main() -> None:
    """Main application entry point"""
    # Setup logging
    global logger, system_startup_time
    logger = logging.getLogger("qumat628")
    system_startup_time = time.time()
    
    # 💡 Hardened: Register cleanup handler to run on normal exit
    atexit.register(cleanup)
    
    # 💡 Hardened: Set up signal handlers
    def signal_handler(sig, frame):
        if sig == signal.SIGINT:
            logger.info("Program interrupted by user (Ctrl+C)")
        elif sig == signal.SIGTERM:
            logger.info("Program terminated by system")
        else:
            logger.info(f"Program received signal {sig}")
            
        shutdown_requested.set()
        
        # Allow up to 5 seconds for threads to clean up
        logger.info("Allowing up to 5 seconds for threads to terminate...")
        for _ in range(50):  # 50 * 0.1s = 5s
            if sum(1 for t in threading.enumerate() if t != threading.current_thread()) == 0:
                break
            time.sleep(0.1)
            
        # Call cleanup directly to ensure it happens
        cleanup()
        logger.info("Program terminated.")
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Prevent duplicate logger initialization
    if not logger.handlers:
        logger = setup_logging()

    logger.info("Starting QUMAT628 control system with 50Hz sampling mapped to 100Hz grid...")
    logger.info("📊 Target: sample at 50Hz and map to 100Hz by duplicating each sample")
    
    # 💡 Hardened: Log system info
    try:
        import platform
        system_info = f"OS: {platform.system()} {platform.release()}, Python: {platform.python_version()}"
        logger.info(f"🖥️ System information: {system_info}")
        
        memory = psutil.virtual_memory()
        memory_gb = memory.total / (1024**3)
        logger.info(f"🖥️ System memory: {memory_gb:.1f} GB total, {memory.available/(1024**3):.1f} GB available")
        
        disk = psutil.disk_usage('/')
        disk_gb = disk.total / (1024**3)
        logger.info(f"💾 Disk space: {disk_gb:.1f} GB total, {disk.free/(1024**3):.1f} GB free ({disk.percent}% used)")
    except Exception as e:
        logger.warning(f"⚠️ Could not get full system information: {e}")

    # Initialize hardware
    if not initialize_hardware():
        logger.warning("⚠️ Hardware initialization failed, continuing anyway...")

    # Get and log IP address
    get_current_ip()

        # 💡 Hardened: Create directories for data files if they don't exist
    try:
        # Ensure CSV backup directory exists
        if not os.path.exists(Config.CSV_BACKUP_DIR):
            os.makedirs(Config.CSV_BACKUP_DIR)
            logger.info(f"✅ Created CSV backup directory: {Config.CSV_BACKUP_DIR}")
    except Exception as e:
        logger.warning(f"⚠️ Could not create data directories: {e}")

    # Start main server on primary port
    main_server_thread = threading.Thread(
        target=ttcp_server,
        args=(Config.PRIMARY_PORT,),
        daemon=True,
        name="TTCPServer-Primary"
    )
    main_server_thread.start()
    register_thread("TTCPServer-Primary", main_server_thread)
    logger.info("Main TCP server started on port %d", Config.PRIMARY_PORT)

    # Start compatibility server on secondary port
    compat_server_thread = threading.Thread(
        target=ttcp_server,
        args=(Config.COMPAT_PORT,),
        daemon=True,
        name="TTCPServer-Compat"
    )
    compat_server_thread.start()
    register_thread("TTCPServer-Compat", compat_server_thread)
    logger.info("Compatibility TCP server started on port %d", Config.COMPAT_PORT)

    # Start status monitor
    status_thread = threading.Thread(
        target=status_monitor, 
        daemon=True,
        name="StatusMonitor"
    )
    status_thread.start()
    register_thread("StatusMonitor", status_thread)
    logger.info("Status monitor started")

    try:
        # 💡 Hardened: Monitor and keep critical threads alive
        while True:
            if shutdown_requested.is_set():
                break
                
            # Check if main threads are alive
            thread_health = check_thread_health()
            
            if not thread_health["healthy"]:
                logger.warning(f"⚠️ Unhealthy threads detected: {', '.join(thread_health['unhealthy_threads'])}")
                
                # Attempt to restart critical threads if needed
                for thread_name in thread_health["unhealthy_threads"]:
                    # Only restart if not in recovery and not intentionally terminating
                    if not recovery_in_progress.is_set() and not shutdown_requested.is_set():
                        logger.error(f"❌ Thread {thread_name} is unhealthy, attempting restart")
                        
                        # Based on thread name, start the appropriate new thread
                        if "TTCPServer-Primary" in thread_name:
                            port_thread = threading.Thread(
                                target=ttcp_server,
                                args=(Config.PRIMARY_PORT,),
                                daemon=True,
                                name="TTCPServer-Primary-Recovery"
                            )
                            port_thread.start()
                            register_thread("TTCPServer-Primary-Recovery", port_thread)
                            
                        elif "TTCPServer-Compat" in thread_name:
                            compat_thread = threading.Thread(
                                target=ttcp_server,
                                args=(Config.COMPAT_PORT,),
                                daemon=True,
                                name="TTCPServer-Compat-Recovery"
                            )
                            compat_thread.start()
                            register_thread("TTCPServer-Compat-Recovery", compat_thread)
                            
                        elif "StatusMonitor" in thread_name:
                            status_thread = threading.Thread(
                                target=status_monitor,
                                daemon=True,
                                name="StatusMonitor-Recovery"
                            )
                            status_thread.start()
                            register_thread("StatusMonitor-Recovery", status_thread)
            
            # 💡 Hardened: Periodically check system resources
            resources = check_system_resources()
            if not resources.get("healthy", False):
                logger.warning(f"⚠️ System resources unhealthy: CPU={resources.get('cpu_percent', 0):.1f}%, " +
                               f"Memory={resources.get('memory_percent', 0):.1f}%, " +
                               f"Disk={resources.get('disk_free_mb', 0):.1f}MB")
            
            # 💡 Hardened: Ensure CSV is flushed periodically
            if 'csv_file_handle' in globals() and csv_file_handle is not None:
                try:
                    safe_flush_csv()
                except Exception as e:
                    logger.error(f"❌ Error flushing CSV in main loop: {e}")
            
            # Periodically log uptime
            uptime = time.time() - system_startup_time
            if int(uptime) % 3600 == 0 and uptime > 0:  # Every hour
                hours = uptime / 3600
                logger.info(f"⏱️ System uptime: {hours:.1f} hours")
                
                # Log thread information
                thread_names = [t.name for t in threading.enumerate()]
                thread_count = len(thread_names)
                logger.info(f"🧵 Active threads ({thread_count}): {', '.join(thread_names)}")
            
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user (Ctrl+C)")
    except Exception as e:
        logger.critical(f"❌ CRITICAL ERROR in main loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        shutdown_requested.set()
        time.sleep(2)  # Give threads time to clean up
        cleanup()
        logger.info("Program terminated.")


if __name__ == "__main__":
    try:
        # Simple output at startup
        print("QUMAT628 Control System with 50Hz to 100Hz mapping starting...")
        
        # 💡 Hardened: Track startup time for uptime calculation
        system_startup_time = time.time()
        
        # Set global flags for diagnostics
        global_system_healthy = True
        last_successful_operation = time.time()
        last_successful_sampling = time.time()
        
        # Run the main function
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
