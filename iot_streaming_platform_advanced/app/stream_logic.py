"""Pure streaming logic; no Kafka or database required for unit testing."""
from collections import deque
from statistics import mean, pstdev
from datetime import datetime, timezone
from pydantic import ValidationError
from app.schemas import SensorEvent

def validate_event(payload):
    return SensorEvent.model_validate(payload).model_dump(mode="json")

def threshold_alerts(event, temp_limit=45.0, humidity_limit=85.0, vibration_limit=8.0):
    alerts = []
    if event["temperature_c"] >= temp_limit:
        alerts.append({"type": "high_temperature", "severity": "critical",
                       "message": f"Temperature {event['temperature_c']:.2f} °C >= {temp_limit:.2f} °C"})
    if event["humidity_pct"] >= humidity_limit:
        alerts.append({"type": "high_humidity", "severity": "warning",
                       "message": f"Humidity {event['humidity_pct']:.2f}% >= {humidity_limit:.2f}%"})
    if event["vibration"] >= vibration_limit:
        alerts.append({"type": "high_vibration", "severity": "critical",
                       "message": f"Vibration {event['vibration']:.2f} >= {vibration_limit:.2f}"})
    return alerts

class RollingAnomalyDetector:
    """Per-sensor, per-metric rolling z-score detector. Scores against prior observations."""
    def __init__(self, window=25, z_threshold=3.0, min_samples=8):
        self.window = int(window)
        self.z_threshold = float(z_threshold)
        self.min_samples = int(min_samples)
        self.history = {}

    def score(self, sensor_id, metric, value):
        key = (sensor_id, metric)
        values = self.history.setdefault(key, deque(maxlen=self.window))
        result = None
        if len(values) >= self.min_samples:
            avg = mean(values)
            std = pstdev(values)
            if std > 1e-9:
                z = (float(value) - avg) / std
                if abs(z) >= self.z_threshold:
                    result = {"type": "statistical_anomaly", "severity": "warning",
                              "message": f"{metric} anomaly: value={value:.2f}, rolling z-score={z:.2f}"}
        values.append(float(value))
        return result
