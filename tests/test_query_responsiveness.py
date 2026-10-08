import asyncio
import threading

import httpx
import pytest
from app.api.v1.query import router
from fastapi import FastAPI


@pytest.mark.asyncio
@pytest.mark.parametrize("path,method", [
    ("/api/v1/logs", "count_logs"),
    ("/api/v1/logs/stats", "query_full_stats"),
    ("/api/v1/logs/services", "query_services"),
])
async def test_clickhouse_query_leaves_event_loop_responsive(path, method):
    started = threading.Event()
    release = threading.Event()

    class Database:
        def query_logs(self, **kwargs):
            return []

    def slow_query(**kwargs):
        started.set()
        assert release.wait(timeout=3), "Event loop did not release query"
        return 0 if method == "count_logs" else [] if method == "query_services" else {}

    db = Database()
    setattr(db, method, slow_query)
    app = FastAPI()
    app.include_router(router)
    app.state.clickhouse = db
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        request = asyncio.create_task(client.get(path))
        try:
            assert await asyncio.to_thread(started.wait, 1)
        finally:
            release.set()
        assert (await request).status_code == 200
