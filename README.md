# Advanced IoT Streaming Platform

A real-time Internet of Things (IoT) data streaming platform built with Python, Apache Kafka, FastAPI, PostgreSQL/TimescaleDB, and Streamlit.

## 1. Project Overview

This project simulates IoT sensors that continuously generate data, streams the data through Apache Kafka, processes incoming events, detects abnormal sensor readings, stores time-series data in a database, and displays real-time monitoring information on a dashboard.

### Key Features

- Real-time IoT sensor data simulation
- Apache Kafka-based event streaming
- Stream processing and data validation
- Threshold-based anomaly detection
- Time-series data storage
- REST API built with FastAPI
- Interactive dashboard using Streamlit
- Docker-based deployment
- Prometheus metrics for monitoring
- Automated tests using pytest

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core programming language |
| Apache Kafka | Real-time event streaming |
| PostgreSQL | Data storage |
| TimescaleDB | Time-series data management |
| FastAPI | REST API development |
| Streamlit | Interactive dashboard |
| Docker | Containerization |
| Docker Compose | Multi-service orchestration |
| Prometheus | Metrics monitoring |
| Grafana | Monitoring dashboards |
| Pytest | Automated testing |

## 3. Architecture

The platform follows this data flow:

1. IoT sensors generate temperature, humidity, and other sensor readings.
2. The sensor simulator publishes events to Apache Kafka.
3. A stream-processing service consumes the events.
4. The processor validates readings and identifies threshold violations.
5. Processed events are stored in PostgreSQL/TimescaleDB.
6. FastAPI exposes endpoints for retrieving sensor data and system metrics.
7. Streamlit displays sensor readings, trends, and anomalies.
8. Prometheus and Grafana can be used for operational monitoring.

### Architecture Flow

IoT Sensors → Kafka Producer → Apache Kafka → Stream Processor → TimescaleDB → FastAPI → Streamlit Dashboard

## 4. Project Structure

```text
iot-streaming-platform/
├── app/
│   ├── api.py
│   ├── processor.py
│   ├── simulator.py
│   ├── database.py
│   └── config.py
├── dashboard/
│   └── streamlit_app.py
├── database/
│   └── schema.sql
├── tests/
│   └── test_processor.py
├── monitoring/
│   └── prometheus.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

The exact filenames and directories may vary depending on the implementation.

## 5. Prerequisites

Install the following tools before running the project:

- Python 3.11 or a compatible version
- Docker Desktop
- Git
- Visual Studio Code or another code editor

Verify the installations:

```bash
python --version
docker --version
docker compose version
git --version
```

## 6. Installation and Setup

### Step 1: Download the Project

Download the project ZIP and extract it to a local directory.

Open the extracted folder in Visual Studio Code.

### Step 2: Configure Environment Variables

Copy `.env.example` to `.env`.

On Windows Command Prompt:

```cmd
copy .env.example .env
```

On Linux or macOS:

```bash
cp .env.example .env
```

Update the environment variables if necessary. Do not commit passwords, tokens, or other secrets to version control.

### Step 3: Start the Services

Run the following command from the directory containing `docker-compose.yml`:

```bash
docker compose up --build -d
```

This command builds the application images and starts the configured services in the background.

### Step 4: Check the Containers

```bash
docker compose ps
```

View application logs:

```bash
docker compose logs -f
```

View logs for a specific service:

```bash
docker compose logs -f api
```

Replace `api` with the actual service name defined in your Docker Compose configuration.

## 7. Access the Application

Once the services are running, open the corresponding URLs.

| Component | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| FastAPI Application | http://localhost:8000 |
| FastAPI Swagger Documentation | http://localhost:8000/docs |
| FastAPI ReDoc Documentation | http://localhost:8000/redoc |
| Prometheus Metrics | http://localhost:8000/metrics |

These addresses assume the services use the default local port mappings. Optional services may require additional configuration.

## 8. API Endpoints

The API may provide endpoints similar to the following, depending on the implemented routes.

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/` | Check application status |
| GET | `/health` | Health check |
| GET | `/sensors` | Retrieve available sensors |
| GET | `/readings` | Retrieve sensor readings |
| GET | `/anomalies` | Retrieve detected anomalies |
| GET | `/metrics` | Expose Prometheus metrics |

Use the interactive Swagger interface at `http://localhost:8000/docs` to inspect the actual endpoints supported by the application.

## 9. Example IoT Event

A sensor event can use a JSON structure similar to this:

```json
{
  "sensor_id": "sensor-001",
  "timestamp": "2026-10-19T10:30:00Z",
  "temperature": 28.5,
  "humidity": 64.2
}
```

The event is published to Kafka, processed by the streaming service, and stored for analysis.

The actual event schema depends on the implementation.

## 10. Anomaly Detection

The processor can identify abnormal readings using configurable thresholds.

For example:

- Temperature above the configured maximum
- Temperature below the configured minimum
- Humidity outside the permitted range
- Missing or invalid sensor values
- Malformed events

Threshold-based detection is a starting point. The platform can later be extended with statistical anomaly detection and machine learning models.

## 11. Running Tests

If the project includes pytest tests, install the dependencies in your Python environment and run:

```bash
pytest -v
```

To run tests inside a configured application container:

```bash
docker compose exec api pytest -v
```

Replace `api` with the appropriate service name. The container must include the test dependencies and project test files.

## 12. Stopping the Application

Stop the running containers:

```bash
docker compose down
```

To remove the containers and associated volumes:

```bash
docker compose down -v
```

**Warning:** Removing volumes can permanently delete locally stored database data. Use this command only when you intend to reset the environment.

## 13. Troubleshooting

### Docker Is Not Running

Start Docker Desktop and wait until its engine is ready.

### Port Already in Use

Another application may already be using a required port. Stop the conflicting process or change the port mapping in `docker-compose.yml`.

### Services Cannot Connect to Kafka

Check the Kafka container logs and verify that the bootstrap server configuration matches the Docker network configuration.

### Database Connection Failed

Verify the database service status, credentials, database name, and connection settings in `.env`.

### Dashboard Has No Data

Check that the simulator is publishing events, the Kafka processor is consuming them, and the database contains records.

### View Detailed Logs

```bash
docker compose logs --tail=100
```

## 14. Future Enhancements

Potential improvements include:

- MQTT integration for physical IoT devices
- Schema Registry and event schema validation
- Apache Flink or Spark Structured Streaming
- Machine learning-based anomaly detection
- Authentication and role-based access control
- Cloud deployment on AWS, Azure, or GCP
- Kubernetes orchestration
- CI/CD automation
- Data retention policies and automated backups
- Load testing and performance benchmarking

## 15. Learning Outcomes

By completing this project, you can gain practical experience with:

- Event-driven architecture
- Real-time data engineering
- Kafka producers and consumers
- Python backend development
- REST API design
- Time-series databases
- Data validation and anomaly detection
- Containerization and service orchestration
- Monitoring and observability
- Automated testing

## 16. Conclusion

The Advanced IoT Streaming Platform demonstrates how sensor data can be collected, streamed, processed, stored, and visualized using modern data engineering tools.

It provides a foundation for industrial IoT monitoring, smart buildings, predictive maintenance, environmental monitoring, and real-time analytics.

**Important:** Ensure that the documented endpoints, service names, environment variables, and project paths match the actual implementation before deploying the application.
