"""Generate simulated IoT events and publish them to Kafka."""
import json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SIMULATOR_INTERVAL_SECONDS

def make_event(sensor_id):
    spike = random.random() < 0.035
    return {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(random.uniform(25, 42) + (random.uniform(10, 22) if spike else 0), 2),
        "humidity_pct": round(min(100, random.uniform(30, 78) + (random.uniform(15, 25) if spike else 0)), 2),
        "vibration": round(random.uniform(0.1, 5.5) + (random.uniform(5, 12) if spike else 0), 2),
        "pressure_kpa": round(random.uniform(98, 108) + (random.uniform(15, 25) if spike else 0), 2),
        "metadata": {"site": "demo-factory", "firmware": "1.2.0"}
    }

def main():
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode(),
                key_serializer=lambda x: x.encode(),
                retries=5, acks="all",
            )
        except NoBrokersAvailable:
            print("Kafka unavailable; retrying in 3 seconds", flush=True)
            time.sleep(3)
    print(f"Publishing simulated events to {KAFKA_TOPIC}", flush=True)
    while True:
        for number in range(1, 6):
            event = make_event(f"sensor-{number:02d}")
            producer.send(KAFKA_TOPIC, value=event, key=event["sensor_id"])
            print(f"produced {event['sensor_id']} temp={event['temperature_c']}C", flush=True)
        producer.flush()
        time.sleep(SIMULATOR_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
