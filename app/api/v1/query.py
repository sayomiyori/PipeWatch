from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from app.models.log_entry import LogEntryResponse, LogLevel
from app.services.identity import read_tenant

router = APIRouter(prefix="/api/v1", tags=["query"])


@router.get("/logs")
def get_logs(
    request: Request,
    service: str | None = None,
    level: LogLevel | None = None,
    trace_id: str | None = None,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    q: str | None = Query(default=None, description="Full-text search in message"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=500),
    tenant_id: str = Depends(read_tenant),
) -> dict[str, Any]:
    offset = (page - 1) * size
    clickhouse = request.app.state.clickhouse

    total = clickhouse.count_logs(
        service=service, level=level, trace_id=trace_id,
        from_ts=from_ts, to_ts=to_ts, q=q, tenant_id=tenant_id,
    )
    rows = clickhouse.query_logs(
        service=service, level=level, trace_id=trace_id,
        from_ts=from_ts, to_ts=to_ts, q=q,
        limit=size, offset=offset, tenant_id=tenant_id,
    )
    items = [LogEntryResponse.from_db_row(r) for r in rows]
    return {"total": total, "page": page, "size": size, "items": items}


@router.get("/logs/stats")
def get_logs_stats(
    request: Request,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    tenant_id: str = Depends(read_tenant),
) -> dict[str, Any]:
    """
    Returns:
      total_count, count_by_level, count_by_service,
      error_rate_per_hour, top_errors
    """
    return request.app.state.clickhouse.query_full_stats(
        from_ts=from_ts, to_ts=to_ts, tenant_id=tenant_id,
    )


@router.get("/logs/services")
def get_services(request: Request, tenant_id: str = Depends(read_tenant)) -> dict[str, Any]:
    """Returns unique services with last_seen timestamp and total log count."""
    services = request.app.state.clickhouse.query_services(tenant_id=tenant_id)
    return {"count": len(services), "services": services}
