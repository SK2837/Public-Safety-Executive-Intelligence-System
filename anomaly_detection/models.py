"""Pydantic models for anomaly detection output."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class AnomalySeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AnomalyType(str, Enum):
    VOLUME_SPIKE = "VOLUME_SPIKE"
    RESPONSE_TIME_SPIKE = "RESPONSE_TIME_SPIKE"
    GEOGRAPHIC_HOTSPOT = "GEOGRAPHIC_HOTSPOT"
    RESOURCE_GAP = "RESOURCE_GAP"


class Anomaly(BaseModel):
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    detected_at: datetime
    description: str
    z_score: Optional[float] = Field(default=None, description="Statistical z-score where applicable")
    agency: Optional[str] = None
    district: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    affected_count: Optional[int] = Field(default=None, description="Incident or gap-hour count")
    metadata: dict = Field(default_factory=dict)
