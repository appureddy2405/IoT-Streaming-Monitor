import unittest
from datetime import datetime, timezone
from app.stream_logic import validate_event, threshold_alerts, RollingAnomalyDetector

def event(**updates):
    base = {
        "event_id": "evt-1", "sensor_id": "sensor-01",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": 30.0, "humidity_pct": 50.0,
        "vibration": 1.0, "pressure_kpa": 101.0, "metadata": {}
    }
    base.update(updates)
    return base

class StreamLogicTests(unittest.TestCase):
    def test_valid_event(self):
        self.assertEqual(validate_event(event())["sensor_id"], "sensor-01")

    def test_out_of_range_rejected(self):
        with self.assertRaises(Exception):
            validate_event(event(humidity_pct=150))

    def test_threshold_alerts(self):
        alerts = threshold_alerts(event(temperature_c=50, humidity_pct=90, vibration=9))
        self.assertEqual({a["type"] for a in alerts},
                         {"high_temperature", "high_humidity", "high_vibration"})

    def test_anomaly_after_warmup(self):
        detector = RollingAnomalyDetector(window=20, z_threshold=3, min_samples=8)
        for value in [10, 10.1, 9.9, 10.0, 10.2, 9.8, 10.1, 9.9]:
            self.assertIsNone(detector.score("s1", "temperature_c", value))
        result = detector.score("s1", "temperature_c", 25)
        self.assertIsNotNone(result)
        self.assertEqual(result["type"], "statistical_anomaly")

if __name__ == "__main__":
    unittest.main()
