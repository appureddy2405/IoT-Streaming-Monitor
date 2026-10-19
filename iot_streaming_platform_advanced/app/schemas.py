from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

class SensorEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    event_id: str = Field(min_length=1)
    sensor_id: str = Field(min_length=1)
    timestamp: datetime
    temperature_c: float = Field(ge=-50, le=150)
    humidity_pct: float = Field(ge=0, le=100)
    vibration: float = Field(ge=0, le=1000)
    pressure_kpa: float = Field(ge=0, le=2000)
