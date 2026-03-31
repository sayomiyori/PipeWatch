from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.middleware.metrics import LOGS_ACCEPTED
from app.models.log_entry import LogEntryCreate

router = APIRouter(prefix="/api/v1", tags=["ingest"])


class LogBatchRequest(BaseModel):
    logs: list[LogEntryCreate]


@router.post("/logs")
async def ingest_log(payload: LogEntryCreate, request: Request) -> dict[str, Any]:
    queue = request.app.state.ingest_queue
    ts = datetime.now(timezone.utc)
    record = payload.to_record(ts)
    await queue.put(record)
    LOGS_ACCEPTED.labels(endpoint="logs", service=record["service"]).inc()
    return {"status": "accepted", "count": 1}


@router.post("/logs/batch")
async def ingest_log_batch(
    payload: LogBatchRequest, request: Request
) -> dict[str, Any]:
    queue = request.app.state.ingest_queue
    ts = datetime.now(timezone.utc)
    for item in payload.logs:
        await queue.put(item.to_record(ts))
        LOGS_ACCEPTED.labels(
            endpoint="logs/batch", service=item.service
        ).inc()
    return {"status": "accepted", "count": len(payload.logs)}

