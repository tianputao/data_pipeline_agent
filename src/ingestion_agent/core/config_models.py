from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, root_validator, validator


class SourceType(str, Enum):
    postgres = "postgres"
    mysql = "mysql"
    sqlserver = "sqlserver"
    kafka = "kafka"
    event_hubs = "event_hubs"


class SinkType(str, Enum):
    delta = "delta"
    jdbc = "jdbc"
    table = "table"


class Frequency(str, Enum):
    once = "once"
    hourly = "hourly"
    daily = "daily"
    streaming = "streaming"


class AggregateConfig(BaseModel):
    group_by: List[str] = Field(default_factory=list)
    window: Optional[str] = None
    metrics: Dict[str, str] = Field(default_factory=dict)  # column -> agg func (sum, avg, count)

    @validator("metrics")
    def metrics_not_empty(cls, v: Dict[str, str]) -> Dict[str, str]:
        if v and not any(v.values()):
            raise ValueError("At least one aggregate metric function is required")
        return v


class TransformationConfig(BaseModel):
    select: List[str] = Field(default_factory=list)
    rename: Dict[str, str] = Field(default_factory=dict)
    convert: Dict[str, str] = Field(default_factory=dict)  # column -> target type
    aggregate: Optional[AggregateConfig] = None


class SourceConfig(BaseModel):
    type: SourceType
    jdbc_url: Optional[str] = None
    table: Optional[str] = None
    topic: Optional[str] = None
    increment_field: Optional[str] = None
    frequency: Frequency = Frequency.daily
    options: Dict[str, Any] = Field(default_factory=dict)

    @root_validator(skip_on_failure=True)
    def validate_source(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        source_type = values.get("type")
        if source_type in {SourceType.postgres, SourceType.mysql, SourceType.sqlserver}:
            if not values.get("jdbc_url"):
                raise ValueError("jdbc_url is required for relational sources")
            if not values.get("table"):
                raise ValueError("table is required for relational sources")
        if source_type in {SourceType.kafka, SourceType.event_hubs}:
            if not values.get("topic"):
                raise ValueError("topic is required for streaming sources")
            if values.get("frequency") != Frequency.streaming:
                raise ValueError("streaming sources must use frequency=streaming")
        return values


class SinkConfig(BaseModel):
    type: SinkType
    path: Optional[str] = None
    catalog: Optional[str] = None
    database: Optional[str] = None
    table: Optional[str] = None
    mode: str = "append"
    layer: Optional[str] = None  # bronze/silver/gold
    options: Dict[str, Any] = Field(default_factory=dict)

    @root_validator(skip_on_failure=True)
    def validate_sink(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        sink_type = values.get("type")
        if sink_type == SinkType.delta and not values.get("path") and not values.get("table"):
            raise ValueError("delta sink requires path or table")
        if sink_type == SinkType.jdbc and not values.get("table"):
            raise ValueError("jdbc sink requires table name")
        return values


class MonitoringConfig(BaseModel):
    enable_lineage: bool = True
    enable_metrics: bool = True
    enable_audit: bool = True


class IngestionJobConfig(BaseModel):
    job_name: str
    description: Optional[str] = None
    source: SourceConfig
    transformations: TransformationConfig = Field(default_factory=TransformationConfig)
    sink: SinkConfig
    tags: Dict[str, str] = Field(default_factory=dict)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    @property
    def is_streaming(self) -> bool:
        return self.source.frequency == Frequency.streaming


class JobRequest(BaseModel):
    """Agent entry payload."""

    natural_language: Optional[str] = None
    config: Optional[IngestionJobConfig] = None
    render_only: bool = False
