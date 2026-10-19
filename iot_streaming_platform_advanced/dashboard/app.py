import os, time
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API = os.getenv("API_BASE_URL", "http://localhost:8000")
st.set_page_config(page_title="IoT Streaming Platform", page_icon="📡", layout="wide")
st.title("📡 Advanced IoT Streaming Platform")
st.caption("Kafka event streaming • TimescaleDB • FastAPI • anomaly detection • threshold alerts")

with st.sidebar:
    st.header("Controls")
    refresh = st.slider("Refresh interval (seconds)", 2, 20, 5)
    limit = st.selectbox("Recent readings", [100, 250, 500, 1000], index=1)
    sensor_filter = st.text_input("Sensor ID filter (optional)", placeholder="sensor-01")
    auto = st.checkbox("Auto-refresh", value=True)

try:
    health = requests.get(f"{API}/health", timeout=3)
    health.raise_for_status()
    readings_resp = requests.get(f"{API}/readings/latest", params={"limit": limit}, timeout=10)
    readings_resp.raise_for_status()
    alert_resp = requests.get(f"{API}/alerts", params={"limit": 100}, timeout=10)
    alert_resp.raise_for_status()
    readings = readings_resp.json()
    alerts = alert_resp.json()
except Exception as exc:
    st.error(f"API is not available: {exc}. Start the stack with `docker compose up --build -d`.")
    st.stop()

df = pd.DataFrame(readings)
if df.empty:
    st.info("Waiting for the first events. Check simulator and processor logs.")
    st.stop()

df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
df = df.dropna(subset=["event_timestamp"]).sort_values("event_timestamp")
if sensor_filter.strip():
    df = df[df["sensor_id"].str.contains(sensor_filter.strip(), case=False, na=False)]
if df.empty:
    st.warning("No readings match this sensor filter.")
    st.stop()

alerts_df = pd.DataFrame(alerts)
a, b, c, d = st.columns(4)
a.metric("Readings loaded", len(df))
b.metric("Sensors", df["sensor_id"].nunique())
c.metric("Recent alerts", len(alerts_df))
d.metric("Latest event", df["event_timestamp"].max().strftime("%H:%M:%S UTC"))

st.subheader("Latest reading by sensor")
latest = df.sort_values("event_timestamp").groupby("sensor_id", as_index=False).tail(1)
st.dataframe(latest[["sensor_id", "event_timestamp", "temperature_c", "humidity_pct", "vibration", "pressure_kpa"]].sort_values("event_timestamp", ascending=False),
             use_container_width=True, hide_index=True)

st.subheader("Time-series trends")
options = {"temperature_c": "Temperature (°C)", "humidity_pct": "Humidity (%)",
           "vibration": "Vibration", "pressure_kpa": "Pressure (kPa)"}
metric = st.selectbox("Measurement", list(options), format_func=lambda x: options[x])
fig = px.line(df, x="event_timestamp", y=metric, color="sensor_id", markers=True,
              labels={"event_timestamp": "Event time", metric: options[metric]})
st.plotly_chart(fig, use_container_width=True)

st.subheader("Aggregates by sensor")
try:
    summary = requests.get(f"{API}/metrics/summary", params={"window_minutes": 5}, timeout=10).json()
    summary_df = pd.DataFrame(summary.get("sensors", []))
    if not summary_df.empty:
        st.dataframe(summary_df.round(2), use_container_width=True, hide_index=True)
    else:
        st.write("No aggregate rows yet.")
except Exception as exc:
    st.warning(f"Could not load aggregate metrics: {exc}")

st.subheader("Alert history")
if alerts_df.empty:
    st.success("No alerts recorded yet.")
else:
    st.dataframe(alerts_df, use_container_width=True, hide_index=True)

if auto:
    time.sleep(refresh)
    st.rerun()
