from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


LogLevel = Literal["debug", "info", "warn", "error", "fatal"]

LEVEL_TO_INT: dict[LogLevel, int] = {
    "debug": 1,
    "info": 2,
    "warn": 3,
    "error": 4,
    "fatal": 5,
}
INT_TO_LEVEL: dict[int, LogLevel] = {v: k for k, v in LEVEL_TO_INT.items()}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def level_to_int(level: LogLevel) -> int:
    return LEVEL_TO_INT[level]


def level_from_int(value: int) -> LogLevel:
    # ClickHouse Enum8 can return int; map back to our string enum.
    return INT_TO_LEVEL.get(int(value), "info")


class LogEntryCreate(BaseModel):
    service: str = Field(..., min_length=1, max_length=256)
    level: LogLevel
    message: str
    trace_id: str | None = None
    span_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    host: str | None = None

    def to_record(self, timestamp: datetime) -> dict[str, Any]:
        return {
            "timestamp": timestamp,
            "service": self.service,
            "level": level_to_int(self.level),
            "message": self.message,
            "trace_id": self.trace_id or "",
            "span_id": self.span_id or "",
            "metadata": json.dumps(self.metadata, ensure_ascii=False),
            "host": self.host or "",
        }


class LogEntryResponse(BaseModel):
    timestamp: datetime
    service: str
    level: LogLevel
    message: str
    trace_id: str = ""
    span_id: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    host: str = ""

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "LogEntryResponse":
        metadata_raw = row.get("metadata") or "{}"
        if isinstance(metadata_raw, str):
            try:
                metadata_parsed = json.loads(metadata_raw)
            except Exception:
                metadata_parsed = {}
        elif isinstance(metadata_raw, dict):
            metadata_parsed = metadata_raw
        else:
            metadata_parsed = {}

        level_raw = row.get("level")
        if isinstance(level_raw, int):
            level_parsed = level_from_int(level_raw)
        else:
            # If clickhouse already returns the string enum, keep it.
            level_parsed = level_raw
        return cls(
            timestamp=row["timestamp"],
            service=row["service"],
            level=level_parsed,  # type: ignore[arg-type]
            message=row["message"],
            trace_id=row.get("trace_id") or "",
            span_id=row.get("span_id") or "",
            metadata=metadata_parsed,
            host=row.get("host") or "",
        )

