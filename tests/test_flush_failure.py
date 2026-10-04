import asyncio
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from app.config import settings
from app.main import _flush_loop
from fastapi import FastAPI


@pytest.mark.asyncio
async def test_flush_recovers_from_temporary_database_failure(monkeypatch):
    monkeypatch.setattr(settings, "APP_BATCH_SIZE", 1)
    monkeypatch.setattr(settings, "APP_BATCH_FLUSH_INTERVAL_SECONDS", 0.01)
    loop = asyncio.get_running_loop()
    delivered = asyncio.Event()
    rows = []
    attempts = 0

    def insert_logs(batch):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary test database outage")
        rows.extend(batch)
        loop.call_soon_threadsafe(delivered.set)

    app = FastAPI()
    app.state.ingest_queue = asyncio.Queue()
    app.state.clickhouse = SimpleNamespace(insert_logs=insert_logs)
    app.state.redis_publisher = SimpleNamespace(publish_logs=AsyncMock())
    record = {"message": "accepted before outage"}
    await app.state.ingest_queue.put(record)
    task = asyncio.create_task(_flush_loop(app))
    try:
        await asyncio.wait_for(delivered.wait(), timeout=2)
        assert rows == [record]
        assert not task.done()
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
