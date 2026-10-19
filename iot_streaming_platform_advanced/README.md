# Advanced IoT Streaming Platform

An end-to-end, locally runnable IoT streaming platform built for learning streaming systems and portfolio demonstration.

## Features

- **Sensor simulator** publishes temperature, humidity, vibration, and pressure events to Kafka.
- **Kafka** runs in KRaft mode (no ZooKeeper) with persistent volume.
- **Stream processor** validates events, calculates threshold alerts, detects statistical anomalies using rolling z-scores, and persists records.
- **PostgreSQL + TimescaleDB** stores sensor readings and alert history.
- **FastAPI** provides REST endpoints for health, latest readings, sensor history, aggregate metrics, and alerts.
- **Streamlit + Plotly** dashboard visualizes live readings, trends, threshold lines, and alert history.
- **Prometheus** scrapes API metrics; optional Grafana is included for dashboards.
- **Dead-letter topic** receives invalid messages so they can be inspected.
- **Unit tests** cover validation, threshold rules, and anomaly logic.
- **Docker Compose** starts the whole stack.

This is an advanced learning/demo system, not a hardened production deployment. Production deployments should add authentication/authorization, TLS, secrets management, schema registry, robust idempotency, backups, and a managed observability/security plan.

## Architecture

```text
Sensor simulator
      |
      v
 Kafka topic: iot-readings
      |
      v
 Stream processor ---- invalid events ----> Kafka topic: iot-readings-dlq
      |
      +---- threshold alerts / anomaly scoring
      |
      v
 TimescaleDB (PostgreSQL)
      |
      +------ FastAPI REST API ------> Streamlit dashboard
      |
      +------ Prometheus ------------> Grafana (optional)
```

## Prerequisites

- Docker Desktop (Windows/macOS) or Docker Engine + Compose (Linux)
- Python 3.11+ (only needed to run tests or run Python components outside Docker)
- 4 GB RAM recommended for the full stack

## Quick start

1. Extract the ZIP and open a terminal in `iot_streaming_platform_advanced`.
2. Copy `.env.example` to `.env` and change passwords if desired.
3. Start the stack:

```bash
docker compose up --build -d
docker compose ps
```

4. Follow logs:

```bash
docker compose logs -f processor
```

5. Open:
   - Dashboard: http://localhost:8501
   - API docs: http://localhost:8000/docs
   - API health: http://localhost:8000/health
   - Prometheus: http://localhost:9090
   - Grafana (optional profile): http://localhost:3000

The simulator starts automatically as a Compose service. The processor waits for Kafka and the database to become available.

## Useful commands

```bash
# View all service logs
docker compose logs -f

# Stop services (keeps database/Kafka volumes)
docker compose down

# Stop and remove stored demo data
docker compose down -v

# Run unit tests locally
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
python -m unittest discover -s tests -v

# Scale simulator instances (each generates unique sensor IDs)
docker compose up --build --scale simulator=2 -d
```

## API examples

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/readings/latest?limit=20"
curl "http://localhost:8000/readings/history?sensor_id=sensor-01&limit=100"
curl "http://localhost:8000/metrics/summary?window_minutes=5"
curl "http://localhost:8000/alerts?limit=50"
```

PowerShell users can use `Invoke-RestMethod http://localhost:8000/health`.

## Configuration

Settings are read from `.env` / environment variables. See `.env.example`.

- `TEMP_LIMIT_C`: threshold for high temperature
- `HUMIDITY_LIMIT_PCT`: threshold for high humidity
- `VIBRATION_LIMIT`: threshold for high vibration
- `ANOMALY_ZSCORE`: rolling z-score alert threshold
- `ANOMALY_WINDOW`: number of previous values to use for scoring
- `SIMULATOR_INTERVAL_SECONDS`: pause between rounds
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`: database connection

The processor calculates threshold alerts on every event. Statistical anomaly scoring uses recent in-memory observations per sensor/metric; restart clears that short-term scoring history, while stored readings and alerts remain in PostgreSQL.

## Data model

- `sensor_readings`: timestamp, sensor ID, measurement values, and JSON metadata.
- `sensor_alerts`: timestamp, sensor ID, alert type, severity, message, and related reading timestamp.

The database initialization creates a TimescaleDB hypertable when supported by the configured image. It falls back to a normal PostgreSQL table if hypertable creation is unavailable.

## Project layout

```text
iot_streaming_platform_advanced/
├── app/
│   ├── api.py
│   ├── config.py
│   ├── db.py
│   ├── processor.py
│   ├── schemas.py
│   ├── simulator.py
│   └── stream_logic.py
├── dashboard/
│   └── app.py
├── tests/
│   └── test_stream_logic.py
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       └── provisioning/datasources/datasource.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
├── .env.example
└── README.md
```

## Troubleshooting

- **Dashboard shows no data:** check `docker compose ps`, then `docker compose logs -f simulator processor api`.
- **Kafka startup takes time:** wait 30–60 seconds and inspect `docker compose logs kafka`.
- **Port conflict:** change the host-side port mapping in Compose.
- **Database password changed after first boot:** an existing named volume retains its original database credentials. Use `docker compose down -v` to reset local demo data (this deletes it).
- **Docker build fails:** confirm Docker is running and retry `docker compose build --no-cache`.

## Learning roadmap

1. Run the whole stack and inspect the event JSON.
2. Follow one event from simulator -> Kafka -> processor -> database -> API.
3. Change a threshold and observe alerts.
4. Add a new sensor field and update validation, persistence, API, and dashboard.
5. Add a Kafka consumer group and study partitioning/offsets.
6. Add schema versioning and idempotent event writes.
7. Add authentication and deploy to a cloud environment.
