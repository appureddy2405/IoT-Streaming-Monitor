"""Kafka stream processor: validate, score, alert, and persist."""
import json, time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from app.config import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_DLQ_TOPIC,
    TEMP_LIMIT_C, HUMIDITY_LIMIT_PCT, VIBRATION_LIMIT, ANOMALY_WINDOW, ANOMALY_ZSCORE)
from app.db import wait_for_db, save_event
from app.stream_logic import validate_event, threshold_alerts, RollingAnomalyDetector
from prometheus_client import Counter, start_http_server

PROCESSED = Counter("iot_events_processed_total", "Events successfully processed")
INVALID = Counter("iot_events_invalid_total", "Invalid events sent to dead-letter topic")
ALERTS = Counter("iot_alerts_total", "Alerts produced", ["alert_type", "severity"])

def main():
    start_http_server(9100)
    wait_for_db()
    producer = None
    consumer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode(), retries=5, acks="all")
        except NoBrokersAvailable:
            time.sleep(3)
    while consumer is None:
        try:
            consumer = KafkaConsumer(KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="iot-stream-processor-v1",
                auto_offset_reset="earliest", enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode()))
        except NoBrokersAvailable:
            time.sleep(3)
    detector = RollingAnomalyDetector(ANOMALY_WINDOW, ANOMALY_ZSCORE)
    print(f"Processor consuming {KAFKA_TOPIC}", flush=True)
    for message in consumer:
        try:
            event = validate_event(message.value)
            alerts = threshold_alerts(event, TEMP_LIMIT_C, HUMIDITY_LIMIT_PCT, VIBRATION_LIMIT)
            for metric in ("temperature_c", "humidity_pct", "vibration", "pressure_kpa"):
                anomaly = detector.score(event["sensor_id"], metric, float(event[metric]))
                if anomaly:
                    alerts.append(anomaly)
            save_event(event, alerts)
            for alert in alerts:
                ALERTS.labels(alert["type"], alert["severity"]).inc()
                print(f"ALERT {event['sensor_id']}: {alert['message']}", flush=True)
            PROCESSED.inc()
            consumer.commit()
        except Exception as exc:
            print(f"Invalid/failed event offset={message.offset}: {exc}", flush=True)
            try:
                producer.send(KAFKA_DLQ_TOPIC, {"source_topic": message.topic,
                    "partition": message.partition, "offset": message.offset,
                    "error": str(exc), "payload": message.value})
                producer.flush()
                INVALID.inc()
                consumer.commit()
            except Exception as dlq_exc:
                print(f"DLQ write failed; leaving offset uncommitted: {dlq_exc}", flush=True)

if __name__ == "__main__":
    main()
