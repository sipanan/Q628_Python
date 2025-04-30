import sys
import time
import threading
import smbus2
import RPi.GPIO as GPIO
from PyQt6.QtWidgets import QApplication, QWidget, QLabel, QPushButton, QVBoxLayout, QHBoxLayout
from PyQt6.QtCore import Qt, QTimer

# === GPIO/ADC-Konfiguration (wie in deinem adc.py) ===
VREF = 2.048
PIN_CS = 10
PIN_CLK = 11
PIN_SDO = 9
CLK_DELAY = 0.00001  # 10 µs

def init_adc():
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(PIN_CS, GPIO.OUT)
    GPIO.setup(PIN_CLK, GPIO.OUT)
    GPIO.setup(PIN_SDO, GPIO.IN, pull_up_down=GPIO.PUD_UP)
    GPIO.output(PIN_CS, GPIO.HIGH)
    GPIO.output(PIN_CLK, GPIO.LOW)

def wait_ready(timeout=1.0):
    deadline = time.time() + timeout
    while GPIO.input(PIN_SDO):
        if time.time() > deadline:
            raise TimeoutError("❌ Timeout: RDY pin stayed HIGH (data not ready)")
        time.sleep(0.001)

def read_voltage():
    GPIO.output(PIN_CS, GPIO.LOW)
    wait_ready()

    result = 0
    for _ in range(24):
        GPIO.output(PIN_CLK, GPIO.HIGH)
        time.sleep(CLK_DELAY)
        GPIO.output(PIN_CLK, GPIO.LOW)
        time.sleep(CLK_DELAY)
        bit = GPIO.input(PIN_SDO)
        result = (result << 1) | bit

    GPIO.output(PIN_CS, GPIO.HIGH)

    ovl = (result >> 23) & 1
    ovh = (result >> 22) & 1

    value = result & 0x3FFFFF
    if value & (1 << 21):
        value -= (1 << 22)

    voltage = (value / (1 << 21)) * VREF
    return voltage, ovl, ovh

# === I2C-Konfiguration ===
I2C_BUS = 1
ADDR_MOTOR = 0x1B
ADDR_SENSOR = 0x28
CMD_DOWN = 0x0C
CMD_UP = 0x0E
CMD_PASSIV = 0x0D
CMD_STATUS = 0x40

MASK_BUSY = 0b10000000
MASK_HOME = 0b01000000
MASK_ERR  = 0b00100000
MASK_ACT  = 0b00010000

# === GUI ===
class QumatGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("QUMAT628 Steuerung")
        self.setMinimumSize(500, 420)
        self.bus = smbus2.SMBus(I2C_BUS)
        self.get_sensor_next = False
        init_adc()
        self.setup_ui()
        self.start_threads()

    def setup_ui(self):
        layout = QVBoxLayout()

        # Buttons
        btns = QHBoxLayout()
        self.btn_up = QPushButton("⬆️ Hoch")
        self.btn_down = QPushButton("⬇️ Runter")
        self.btn_up.clicked.connect(lambda: self.bus.write_byte(ADDR_MOTOR, CMD_UP))
        self.btn_down.clicked.connect(lambda: self.bus.write_byte(ADDR_MOTOR, CMD_DOWN))
        btns.addWidget(self.btn_up)
        btns.addWidget(self.btn_down)
        layout.addLayout(btns)

        # Statusanzeige
        self.status_label = QLabel("Status: --")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_label)

        # Temperatur / Feuchte
        self.temp_label = QLabel("Temperatur: -- °C")
        self.hum_label = QLabel("Feuchte: -- %")
        self.temp_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.hum_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.temp_label)
        layout.addWidget(self.hum_label)

        # ADC-Spannung
        self.adc_label = QLabel("ADC-Spannung: -- V")
        self.adc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.adc_label)

        self.setLayout(layout)

    def start_threads(self):
        threading.Thread(target=self.status_loop, daemon=True).start()

        self.sensor_timer = QTimer()
        self.sensor_timer.timeout.connect(self.update_sensor)
        self.sensor_timer.start(200)

        self.adc_timer = QTimer()
        self.adc_timer.timeout.connect(self.update_adc)
        self.adc_timer.start(17)

    def status_loop(self):
        while True:
            try:
                self.bus.write_byte(ADDR_MOTOR, CMD_PASSIV)
                time.sleep(0.01)
                self.bus.write_byte(ADDR_MOTOR, CMD_STATUS)
                time.sleep(0.01)
                status = self.bus.read_byte(ADDR_MOTOR)

                flags = []
                if status & MASK_BUSY: flags.append("🟡 Busy")
                else: flags.append("✅ Bereit")
                if status & MASK_HOME: flags.append("🏠 Home")
                if status & MASK_ERR:  flags.append("❌ Fehler")
                if status & MASK_ACT:  flags.append("⚙️ Aktiv")

                self.status_label.setText(" | ".join(flags))
            except Exception as e:
                self.status_label.setText(f"❌ Statusfehler: {e}")
            time.sleep(1)

    def update_sensor(self):
        try:
            if self.get_sensor_next:
                read = smbus2.i2c_msg.read(ADDR_SENSOR, 4)
                self.bus.i2c_rdwr(read)
                data = list(read)
                raw_hum = ((data[0] & 0x3F) << 8) | data[1]
                raw_temp = (data[2] << 6) | (data[3] >> 2)
                hum = raw_hum / 16383.0 * 100.0
                temp = raw_temp / 16383.0 * 165.0 - 40.0
                self.temp_label.setText(f"Temperatur: {temp:.2f} °C")
                self.hum_label.setText(f"Feuchte: {hum:.2f} %")
                self.get_sensor_next = False
            else:
                self.bus.i2c_rdwr(smbus2.i2c_msg.write(ADDR_SENSOR, []))
                self.get_sensor_next = True
        except Exception as e:
            print(f"[Sensorfehler] {e}")

    def update_adc(self):
        try:
            voltage, ovl, ovh = read_voltage()
            if ovl or ovh:
                self.adc_label.setText(f"⚠️ Overflow! ({voltage:.6f} V)")
            else:
                self.adc_label.setText(f"ADC-Spannung: {voltage:.6f} V")
        except TimeoutError:
            self.adc_label.setText("❌ Timeout beim ADC")
        except Exception as e:
            print(f"[ADC Fehler] {e}")
            self.adc_label.setText("❌ ADC Fehler")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    gui = QumatGUI()
    gui.show()
    sys.exit(app.exec())
