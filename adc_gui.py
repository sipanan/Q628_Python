import sys
import time
import threading
import RPi.GPIO as GPIO
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QLineEdit, QPushButton
)
from PyQt6.QtCore import QTimer

# === MCP3553 GPIO Pins ===
SCK = 11
SDO = 9
CS  = 10

# === GPIO Setup ===
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.setup(SCK, GPIO.OUT)
GPIO.setup(SDO, GPIO.IN)
GPIO.setup(CS, GPIO.OUT)
GPIO.output(SCK, GPIO.LOW)
GPIO.output(CS, GPIO.LOW)

# === Defaults ===
DEFAULT_VREF = 2.048
SAMPLE_INTERVAL = 16.67 / 1000  # seconds (60Hz)

class MCP3553Reader:
    def __init__(self, vref=DEFAULT_VREF):
        self.vref = vref
        self.lock = threading.Lock()
        self.last_result = {
            "raw": 0,
            "signed22": 0,
            "signed32": 0,
            "voltage": 0.0,
            "ovl": 0,
            "ovh": 0,
        }

    def adc_soft_reset(self):
        GPIO.output(CS, GPIO.HIGH)
        time.sleep(0.01)
        GPIO.output(CS, GPIO.LOW)

    def wait_for_ready(self, timeout=1.0):
        start = time.time()
        while GPIO.input(SDO):
            if time.time() - start > timeout:
                raise TimeoutError("SDO stayed HIGH")
            time.sleep(0.0005)

    def read_adc(self):
        try:
            self.wait_for_ready()
            value = 0
            for _ in range(24):
                GPIO.output(SCK, GPIO.HIGH)
                value = (value << 1) | GPIO.input(SDO)
                GPIO.output(SCK, GPIO.LOW)
                time.sleep(1e-6)

            ovh = (value >> 23) & 0x01
            ovl = (value >> 22) & 0x01
            signed = value & 0x3FFFFF
            if signed & (1 << 21):
                signed -= (1 << 22)
            signed_32bit = signed << 10
            voltage = (signed / (1 << 21)) * self.vref

            with self.lock:
                self.last_result = {
                    "raw": value,
                    "signed22": signed,
                    "signed32": signed_32bit,
                    "voltage": voltage,
                    "ovl": ovl,
                    "ovh": ovh,
                }
        except TimeoutError:
            self.adc_soft_reset()

class ADCGui(QWidget):
    def __init__(self, adc_reader):
        super().__init__()
        self.reader = adc_reader
        self.init_ui()
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_display)
        self.timer.start(int(SAMPLE_INTERVAL * 1000))

        self.adc_thread = threading.Thread(target=self.read_loop, daemon=True)
        self.adc_thread.start()

    def init_ui(self):
        layout = QVBoxLayout()
        self.vref_input = QLineEdit(str(DEFAULT_VREF))
        self.vref_input.setPlaceholderText("Enter VREF (e.g. 2.048)")
        self.vref_input.returnPressed.connect(self.update_vref)

        self.labels = {
            "raw": QLabel("Raw Binary: "),
            "hex": QLabel("Raw Hex: "),
            "signed22": QLabel("Signed Value (22-bit): "),
            "signed32": QLabel("Signed Value (32-bit): "),
            "voltage": QLabel("Voltage: "),
            "ov": QLabel("Overflow L/H: "),
        }

        layout.addWidget(QLabel("VREF Input:"))
        layout.addWidget(self.vref_input)
        for label in self.labels.values():
            layout.addWidget(label)

        self.setLayout(layout)
        self.setWindowTitle("MCP3553 ADC Monitor")
        self.resize(300, 250)

    def update_vref(self):
        try:
            new_vref = float(self.vref_input.text())
            self.reader.vref = new_vref
        except ValueError:
            pass

    def update_display(self):
        with self.reader.lock:
            r = self.reader.last_result
        self.labels["raw"].setText(f"Raw Binary: {format(r['raw'], '024b')}")
        self.labels["hex"].setText(f"Raw Hex: 0x{r['raw']:06X}")
        self.labels["signed22"].setText(f"Signed Value (22-bit): {r['signed22']}")
        self.labels["signed32"].setText(f"Signed Value (32-bit): {r['signed32']}")
        self.labels["voltage"].setText(f"Voltage: {r['voltage']:.6f} V")
        self.labels["ov"].setText(f"Overflow L/H: OVL={r['ovl']} OVH={r['ovh']}")

    def read_loop(self):
        while True:
            self.reader.read_adc()
            time.sleep(SAMPLE_INTERVAL)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    adc_reader = MCP3553Reader()
    gui = ADCGui(adc_reader)
    gui.show()
    try:
        sys.exit(app.exec())
    finally:
        GPIO.cleanup()
