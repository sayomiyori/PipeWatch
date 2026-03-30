from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.models.log_entry import LogEntryResponse, LogLevel


router = APIRouter(prefix="/api/v1", tags=["query"])


@router.get("/logs")
async def get_logs(
    request: Request,
    service: str | None = None,
    level: LogLevel | None = None,
    trace_id: str | None = None,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    clickhouse = request.app.state.clickhouse
    rows = clickhouse.query_logs(
        service=service,
        level=level,
        trace_id=trace_id,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
    )
    items = [LogEntryResponse.from_db_row(r) for r in rows]
    return {"count": len(items), "items": items}


class LogsStatsResponse(BaseModel):
    group_by: Literal["service", "level"]
    items: list[dict[str, Any]]


@router.get("/logs/stats")
async def get_logs_stats(
    request: Request,
    group_by: Literal["service", "level"] = "service",
    service: str | None = None,
    level: LogLevel | None = None,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    clickhouse = request.app.state.clickhouse
    rows = clickhouse.query_stats(
        group_by=group_by,
        service=service,
        level=level,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
    )

    # Normalize level key if it's returned as Enum8 int.
    if group_by == "level":
        normalized: list[dict[str, Any]] = []
        for r in rows:
            k = r.get("k")
            if isinstance(k, int):
                # Our ClickHouse layer filters by level int, but when grouping it may still return int.
                from app.models.log_entry import level_from_int

                normalized.append({"k": level_from_int(k), "cnt": r.get("cnt", 0)})
            else:
                normalized.append(r)
        rows = normalized

    return {"group_by": group_by, "items": rows}

