import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://iot_app:change_me_local_only@localhost:5432/iot_platform")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-readings")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "iot-readings-dlq")
TEMP_LIMIT_C = float(os.getenv("TEMP_LIMIT_C", "45"))
HUMIDITY_LIMIT_PCT = float(os.getenv("HUMIDITY_LIMIT_PCT", "85"))
VIBRATION_LIMIT = float(os.getenv("VIBRATION_LIMIT", "8"))
ANOMALY_ZSCORE = float(os.getenv("ANOMALY_ZSCORE", "3.0"))
ANOMALY_WINDOW = int(os.getenv("ANOMALY_WINDOW", "25"))
SIMULATOR_INTERVAL_SECONDS = float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "1.0"))
