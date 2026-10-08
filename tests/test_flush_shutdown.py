import asyncio
import threading

import pytest
from app.config import settings
from app.main import _flush_loop
from fastapi import FastAPI


@pytest.mark.asyncio
@pytest.mark.parametrize("during", ["insert", "publish"])
async def test_shutdown_waits_for_insert_and_drains_without_duplicates(monkeypatch, during):
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
    app.state.ingest_queue = asyncio.Queue()
    app.state.clickhouse = Database()
    app.state.redis_publisher = Publisher()
    for i in range(100):
        app.state.ingest_queue.put_nowait({"id": i})
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
    assert [row["id"] for row in saved] == list(range(100))
    assert app.state.ingest_queue.empty()
