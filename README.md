Advanced IoT Streaming Platform
A complete local learning project for streaming IoT sensor data using Python, Apache Kafka, TimescaleDB/PostgreSQL, FastAPI, Streamlit, Prometheus, and optional Grafana.

The project simulates temperature, humidity, vibration, and pressure readings; streams events through Kafka; validates and analyzes them; stores readings and alerts; and presents results in a dashboard and REST API.
Important: This is a portfolio/learning project, not a production-hardened deployment. Before deploying publicly, add authentication, authorization, TLS, secure secret management, database backups, and appropriate Kafka security.

1. Features
- Sensor simulator generates events for five sensors.
- Kafka runs in KRaft mode; ZooKeeper is not needed.
- Processor validates incoming events and sends problematic messages to a dead-letter topic.
- Threshold alerts for temperature, humidity, and vibration.
- Rolling z-score anomaly detection for sensor measurements.
- PostgreSQL/TimescaleDB stores readings and alert history.
- FastAPI exposes endpoints for health, readings, history, aggregates, and alerts.
- Streamlit/Plotly dashboard shows trends and recent alerts.
- Prometheus scrapes API and processor metrics.
- Optional Grafana service with a Prometheus data source.
- Unit tests for event validation, threshold alerts, and anomaly detection.
- Docker Compose starts the complete local stack.
2. Architecture
                     +----------------------+
                     | Python sensor        |
                     | simulator             |
                     +-----------+----------+
                                 |
                                 v
                     +----------------------+
                     | Kafka: iot-readings  |
                     +-----------+----------+
                                 |
                                 v
                     +----------------------+
                     | Python stream        |
                     | processor            |
                     | - validation         |
                     | - threshold alerts   |
                     | - anomaly detection  |
                     +----+-------------+---+
                          |             |
               invalid    |             | valid readings + alerts
                          v             v
                 +----------------+  +----------------------+
                 | Kafka DLQ      |  | TimescaleDB          |
                 | iot-readings-  |  | readings + alerts    |
                 | dlq            |  +----------+-----------+
                 +----------------+             |
                                                 v
                                      +----------------------+
                                      | FastAPI REST API     |
                                      +----------+-----------+
                                                 |
                                                 v
                                      +----------------------+
                                      | Streamlit dashboard  |
                                      +----------------------+

                         Prometheus -> API/processor metrics
                         Grafana (optional) -> Prometheus
3. Requirements
Install or have access to:
- Docker Desktop on Windows/macOS, or Docker Engine + Docker Compose on Linux.
- At least 4 GB RAM available to Docker is recommended.
- A browser.
- Python 3.11+ only if you want to run tests or Python components outside Docker.
The full application runs inside containers, so you do not need to install Kafka or PostgreSQL directly on your computer.
4. Quick start — Windows, macOS, or Linux
Step 1 — Extract the ZIP
Extract iot_streaming_platform_advanced.zip. Open a terminal in the extracted iot_streaming_platform_advanced directory.

You should see docker-compose.yml, Dockerfile, .env.example, app/, dashboard/, db/, and tests/.
Step 2 — Create the environment file
Windows PowerShell:
Copy-Item .env.example .env
macOS/Linux:
cp .env.example .env
The supplied credentials are for local development only. Change the database password in .env before sharing or deploying this project. If you change POSTGRES_PASSWORD, update the password inside DATABASE_URL too so both settings match.
Step 3 — Start Docker Desktop
On Windows/macOS, open Docker Desktop and wait until it reports that the engine is running. On Linux, ensure Docker and the Compose plugin are available.

Check:
docker --version
docker compose version
Step 4 — Build and start all core services
From the project root:
docker compose up --build -d
The first build/download may take a few minutes. It starts Kafka, TimescaleDB, the simulator, the stream processor, the API, and the dashboard.

Check service status:
docker compose ps
Some services may take a short time to initialize. Follow the processor logs:
docker compose logs -f processor
You should eventually see a message that the processor is consuming iot-readings. Press Ctrl+C to stop following logs; this does not stop the containers.
Step 5 — Open the dashboard
Open:
- Dashboard: http://localhost:8501
- Interactive API docs: http://localhost:8000/docs
- Health endpoint: http://localhost:8000/health
The simulator publishes readings automatically. Give the stack 30–60 seconds on first startup if the dashboard is initially empty.
Step 6 — Confirm the data flow
Check the service logs:
docker compose logs --tail=100 simulator
docker compose logs --tail=100 processor
docker compose logs --tail=100 api
The simulator should log produced events. The processor should log alerts when readings exceed thresholds or are flagged as anomalies.
5. Optional monitoring with Prometheus and Grafana
Start the optional monitoring profile:
docker compose --profile monitoring up --build -d
Open:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
Local demo Grafana login:
- Username: admin
- Password: change_me_local_only
Change this password before any non-local use. The Prometheus data source is provisioned automatically. The project includes metrics collection, but does not include a prebuilt custom Grafana dashboard; you can create one from the Prometheus metrics.

Useful metrics include:
- iot_api_requests_total
- iot_api_request_duration_seconds
- iot_events_processed_total
- iot_events_invalid_total
- iot_alerts_total
6. API usage examples
Health check
Browser: http://localhost:8000/health

Expected response shape:
{
  "status": "ok",
  "database": "connected"
}
Get recent readings
curl "http://localhost:8000/readings/latest?limit=20"
Filter by sensor:
curl "http://localhost:8000/readings/latest?limit=20&sensor_id=sensor-01"
Get one sensor's history
curl "http://localhost:8000/readings/history?sensor_id=sensor-01&limit=100"
Get per-sensor aggregates from the last five minutes
curl "http://localhost:8000/metrics/summary?window_minutes=5"
Get recent alerts
curl "http://localhost:8000/alerts?limit=50"
Filter by severity:
curl "http://localhost:8000/alerts?limit=50&severity=critical"
Windows PowerShell alternative: use Invoke-RestMethod instead of curl, for example:
Invoke-RestMethod "http://localhost:8000/health"
Invoke-RestMethod "http://localhost:8000/readings/latest?limit=20"
7. Configure thresholds and simulator
Open .env and change values such as:
TEMP_LIMIT_C=45
HUMIDITY_LIMIT_PCT=85
VIBRATION_LIMIT=8
ANOMALY_ZSCORE=3.0
ANOMALY_WINDOW=25
SIMULATOR_INTERVAL_SECONDS=1.0
Then recreate the services to apply the configuration:
docker compose up --build -d
- TEMP_LIMIT_C: alert threshold for temperature in °C.
- HUMIDITY_LIMIT_PCT: alert threshold for humidity in percent.
- VIBRATION_LIMIT: alert threshold for vibration.
- ANOMALY_ZSCORE: absolute z-score required to flag a statistical anomaly.
- ANOMALY_WINDOW: number of recent values used per sensor and measurement.
- SIMULATOR_INTERVAL_SECONDS: delay between batches of generated readings.
The simulator intentionally produces occasional spikes so you can observe alert behavior. Statistical detection needs a warm-up period before it starts scoring anomalies. The anomaly detector's in-memory history resets when the processor restarts; readings and alert history already written to the database remain stored.
8. Run tests locally
You need Python 3.11+ installed.

Create a virtual environment:

Windows PowerShell:
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
If PowerShell blocks environment activation, run this for the current terminal only, then activate again:
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
macOS/Linux:
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
The pure logic tests do not require Kafka or TimescaleDB to be running.
9. Stop, restart, and reset
Stop containers but preserve stored data:
docker compose down
Start them again:
docker compose up -d
View logs:
docker compose logs -f
docker compose logs -f kafka
docker compose logs -f timescaledb
docker compose logs -f processor
Delete all local Kafka and database data (irreversible):
docker compose down -v
Use down -v only when you intentionally want a fresh demo database and Kafka state.
10. Troubleshooting
env file .env not found
Create it using Step 2 (Copy-Item .env.example .env on PowerShell or cp .env.example .env on macOS/Linux).
Docker command not found or daemon unavailable
Install/start Docker Desktop or Docker Engine, then retry docker compose up --build -d.
Dashboard says API is unavailable
docker compose ps
docker compose logs --tail=200 api
docker compose logs --tail=200 timescaledb
Wait for the database health check to pass. If the API repeatedly fails, verify the database credentials in .env.
Dashboard is empty
docker compose logs --tail=100 simulator
docker compose logs --tail=100 processor
Confirm the simulator is publishing and the processor is consuming. Allow time for the initial service startup.
Port already in use
Default ports are 9092 (Kafka), 5432 (database), 8000 (API), 8501 (dashboard), 9090 (Prometheus), and 3000 (Grafana). If a port is occupied, change the host-side port mapping before starting the stack.
Changed database password but connection still fails
A named Docker volume retains the original database credentials. For a disposable local demo, stop the stack and remove volumes with docker compose down -v, then start again. This deletes the database and Kafka data.
11. Project structure
iot_streaming_platform_advanced/
├── app/
│   ├── api.py              # FastAPI endpoints and API metrics
│   ├── config.py           # environment configuration
│   ├── db.py               # PostgreSQL persistence helpers
│   ├── processor.py        # Kafka consumer and event processing
│   ├── schemas.py          # Pydantic event schema
│   ├── simulator.py        # synthetic IoT event producer
│   └── stream_logic.py     # validation, threshold and anomaly logic
├── dashboard/
│   └── app.py              # Streamlit + Plotly dashboard
├── db/
│   └── init.sql            # readings/alerts schema and hypertable
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/provisioning/datasources/datasource.yml
├── tests/
│   └── test_stream_logic.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
├── .env.example
└── README.md
