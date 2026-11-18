import asyncio
import threading
from datetime import datetime, timezone

import pytest
from app.config import settings
from app.main import _flush_loop
from fastapi import FastAPI
from redis.exceptions import RedisError


@pytest.mark.asyncio
@pytest.mark.parametrize("during", ["insert", "publish"])
async def test_shutdown_waits_for_insert_and_preserves_backlog_without_duplicates(monkeypatch, during, redis_state):
    monkeypatch.setattr(settings, "APP_BATCH_FLUSH_INTERVAL_SECONDS", 60)
    monkeypatch.setattr(settings, "APP_BATCH_SIZE", 1)
    saved = []
    inserted, release = threading.Event(), threading.Event()
    publishing = asyncio.Event()

    class Database:
        def insert_logs(self, rows):
            if during == "insert" and not inserted.is_set():
                inserted.set()
                assert release.wait(timeout=5)
            saved.extend(rows)

    class Publisher:
        async def publish_logs(self, rows):
            publishing.set()
            if during == "publish":
                await asyncio.Event().wait()

    app = FastAPI()
    app.state.store = redis_state
    app.state.clickhouse = Database()
    app.state.redis_publisher = Publisher()
    await redis_state.enqueue([{"id": i, "timestamp": datetime.now(timezone.utc)} for i in range(100)])
    task = asyncio.create_task(_flush_loop(app))
    try:
        if during == "insert":
            assert await asyncio.to_thread(inserted.wait, 3)
        else:
            await asyncio.wait_for(publishing.wait(), 3)
        task.cancel()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    assert [row["id"] for row in saved] == [0]
    assert await redis_state.redis.llen(redis_state.processing) == 0
    assert await redis_state.redis.llen(redis_state.queue) == 99


@pytest.mark.asyncio
async def test_shutdown_during_redis_release_failure_still_cancels(monkeypatch, redis_state):
    app = FastAPI()
    app.state.store = redis_state
    started = asyncio.Event()
    original = redis_state.claim
    async def claim(token):
        started.set()
        return await original(token)
    async def release(token):
        raise RedisError("Shutdown outage")
    monkeypatch.setattr(redis_state, "claim", claim)
    monkeypatch.setattr(redis_state, "release", release)
    task = asyncio.create_task(_flush_loop(app))
    await asyncio.wait_for(started.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
