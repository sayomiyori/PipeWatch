from __future__ import annotations

import asyncio
import json
from typing import Any

from redis.asyncio import Redis

from app.config import settings
from app.models.log_entry import LogLevel, level_from_int


def _log_to_stream_fields(record: dict[str, Any]) -> dict[str, str]:
    level_raw = record.get("level")
    if isinstance(level_raw, int):
        level_str: LogLevel = level_from_int(level_raw)
    else:
        level_str = level_raw  # type: ignore[assignment]

    return {
        "timestamp": str(record["timestamp"].isoformat()),
        "service": str(record["service"]),
        "level": str(level_str),
        "message": str(record["message"]),
        "trace_id": str(record.get("trace_id", "")),
        "span_id": str(record.get("span_id", "")),
        "metadata": str(record.get("metadata", "{}")),
        "host": str(record.get("host", "")),
    }


class RedisStreamPublisher:
    def __init__(self) -> None:
        self._redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )

    async def close(self) -> None:
        try:
            await self._redis.close()
        except Exception:
            pass

    async def publish_logs(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return

        # Best-effort publisher. Ingest API should not fail just because tailing is down.
        if not settings.REDIS_PUBLISH_ENABLED:
            return

        # Use a pipeline for fewer round-trips.
        pipe = self._redis.pipeline()
        for r in records:
            service = str(r.get("service", "unknown"))
            stream = f"logs:{service}"
            fields = _log_to_stream_fields(r)
            pipe.xadd(stream, fields, maxlen=100_000)

        # Pipeline execution can raise connection errors.
        await pipe.execute()

    @property
    def redis(self) -> Redis:
        return self._redis

