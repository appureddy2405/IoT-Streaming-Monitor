"""FastAPI endpoints for IoT readings and alerts."""
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, Query
from prometheus_client import Counter, Histogram, make_asgi_app
from starlette.middleware.base import BaseHTTPMiddleware
from app.db import connect, wait_for_db

app = FastAPI(title="IoT Streaming Platform API", version="1.0.0",
    description="Readings, aggregation, alert history, and service health.")
REQUESTS = Counter("iot_api_requests_total", "API requests", ["method", "path"])
LATENCY = Histogram("iot_api_request_duration_seconds", "API request duration")
app.mount("/metrics", make_asgi_app())

class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        with LATENCY.time():
            response = await call_next(request)
        REQUESTS.labels(request.method, request.url.path).inc()
        return response
app.add_middleware(MetricsMiddleware)

@app.on_event("startup")
def startup():
    wait_for_db()

@app.get("/health")
def health():
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}")

@app.get("/readings/latest")
def latest_readings(limit: int = Query(100, ge=1, le=1000), sensor_id: str | None = None):
    sql = "SELECT * FROM sensor_readings"
    params = []
    if sensor_id:
        sql += " WHERE sensor_id=%s"
        params.append(sensor_id)
    sql += " ORDER BY event_timestamp DESC LIMIT %s"
    params.append(limit)
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]

@app.get("/readings/history")
def reading_history(sensor_id: str = Query(..., min_length=1),
                    limit: int = Query(500, ge=1, le=5000)):
    with connect() as conn:
        rows = conn.execute("""
            SELECT * FROM sensor_readings WHERE sensor_id=%s
            ORDER BY event_timestamp DESC LIMIT %s
        """, (sensor_id, limit)).fetchall()
    return [dict(row) for row in rows]

@app.get("/metrics/summary")
def metrics_summary(window_minutes: int = Query(5, ge=1, le=1440)):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    with connect() as conn:
        rows = conn.execute("""
            SELECT sensor_id, COUNT(*) AS count,
              AVG(temperature_c) AS avg_temperature_c,
              MIN(temperature_c) AS min_temperature_c,
              MAX(temperature_c) AS max_temperature_c,
              AVG(humidity_pct) AS avg_humidity_pct,
              AVG(vibration) AS avg_vibration,
              AVG(pressure_kpa) AS avg_pressure_kpa
            FROM sensor_readings WHERE event_timestamp >= %s
            GROUP BY sensor_id ORDER BY sensor_id
        """, (cutoff,)).fetchall()
    return {"window_minutes": window_minutes, "sensors": [dict(r) for r in rows]}

@app.get("/alerts")
def alert_history(limit: int = Query(100, ge=1, le=1000),
                  severity: str | None = None):
    sql = "SELECT * FROM sensor_alerts"
    params = []
    if severity:
        sql += " WHERE severity=%s"
        params.append(severity)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
