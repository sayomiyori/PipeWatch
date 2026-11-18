from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field

from app.config import settings
from app.middleware.metrics import LOGS_ACCEPTED
from app.models.log_entry import LogEntryCreate
from app.services.identity import manage_tenant

router = APIRouter(prefix="/api/v1", tags=["ingest"])


class LogBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    logs: list[LogEntryCreate] = Field(min_length=1, max_length=1000)


async def _enqueue(items: list[LogEntryCreate], request: Request, tenant_id: str, endpoint: str) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc)
    records = [dict(item.to_record(timestamp), tenant_id=tenant_id) for item in items]
    await request.app.state.store.enqueue(records)
    if settings.METRICS_ENABLED and not settings.PLATFORM_AUTH_ENABLED:
        LOGS_ACCEPTED.labels(endpoint=endpoint, service="all").inc(len(records))
    return {"status": "accepted", "count": len(records)}


@router.post("/logs")
async def ingest_log(payload: LogEntryCreate, request: Request, tenant_id: str = Depends(manage_tenant)) -> dict[str, Any]:
    return await _enqueue([payload], request, tenant_id, "logs")


@router.post("/logs/batch")
async def ingest_log_batch(
    payload: LogBatchRequest, request: Request, tenant_id: str = Depends(manage_tenant)
) -> dict[str, Any]:
    return await _enqueue(payload.logs, request, tenant_id, "logs/batch")

