"""PostgreSQL/TimescaleDB access helpers."""
import os
import time
import psycopg
from psycopg.rows import dict_row
from app.config import DATABASE_URL

def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=5)

def wait_for_db(retries=30, delay=2):
    last_error = None
    for _ in range(retries):
        try:
            with connect() as conn:
                conn.execute("SELECT 1")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"Database did not become ready: {last_error}")

def save_event(event, alerts):
    with connect() as conn:
        row = conn.execute("""
            INSERT INTO sensor_readings
              (event_id, sensor_id, event_timestamp, temperature_c, humidity_pct,
               vibration, pressure_kpa, anomaly_flags, metadata)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
            ON CONFLICT (event_id, event_timestamp) DO NOTHING
            RETURNING id
        """, (
            event["event_id"], event["sensor_id"], event["timestamp"],
            event["temperature_c"], event["humidity_pct"], event["vibration"],
            event["pressure_kpa"], psycopg.types.json.Jsonb(alerts),
            psycopg.types.json.Jsonb(event.get("metadata", {}))
        )).fetchone()
        if row:
            for alert in alerts:
                conn.execute("""
                    INSERT INTO sensor_alerts
                      (event_id, sensor_id, event_timestamp, alert_type, severity, message)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (event["event_id"], event["sensor_id"], event["timestamp"],
                      alert["type"], alert["severity"], alert["message"]))
