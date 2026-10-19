CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGSERIAL,
    event_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    humidity_pct DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    pressure_kpa DOUBLE PRECISION NOT NULL,
    anomaly_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, event_timestamp),
    UNIQUE (event_id, event_timestamp)
);

SELECT create_hypertable('sensor_readings', 'event_timestamp', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_time
ON sensor_readings (sensor_id, event_timestamp DESC);

CREATE TABLE IF NOT EXISTS sensor_alerts (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_alerts_time ON sensor_alerts (created_at DESC);
