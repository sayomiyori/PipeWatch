import asyncio
import os
import sys

import httpx
import pytest

# Ensure `app.*` imports work when pytest is started from repo root.
THIS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def _clickhouse_reachable() -> bool:
    try:
        from app.services.clickhouse import ClickHouseService

        svc = ClickHouseService()
        svc.ensure_tables()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def clickhouse_service():
    if not _clickhouse_reachable():
        pytest.skip("ClickHouse не доступен (пропускаем интеграционные тесты).")

    from app.services.clickhouse import ClickHouseService

    svc = ClickHouseService()
    svc.ensure_tables()
    return svc


@pytest.mark.asyncio
async def test_ingest_then_query_then_verify_in_clickhouse(clickhouse_service):
    from app.main import app as fastapi_app

    transport = httpx.ASGITransport(app=fastapi_app, lifespan="on")
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        trace_ids = [f"tr-{i}-test" for i in range(3)]
        service = "svc-1"

        logs = [
            {
                "service": service,
                "level": "info" if i == 0 else "error",
                "message": f"hello-{i}",
                "trace_id": trace_ids[i],
                "metadata": {"i": i},
                "host": "test-host",
            }
            for i in range(3)
        ]

        r = await client.post("/api/v1/logs/batch", json={"logs": logs})
        assert r.status_code == 200
        assert r.json()["status"] == "accepted"
        assert r.json()["count"] == 3

        # Wait until background flush inserts into ClickHouse.
        async def wait_for_count(expected: int, timeout_s: float = 10.0) -> None:
            deadline = asyncio.get_event_loop().time() + timeout_s
            while True:
                cnt = clickhouse_service.count_by_trace_ids(trace_ids)
                if cnt >= expected:
                    return
                if asyncio.get_event_loop().time() >= deadline:
                    raise AssertionError(f"Timeout waiting for ClickHouse inserts; cnt={cnt}")
                await asyncio.sleep(0.5)

        await wait_for_count(3)

        # Verify query endpoint returns inserted entries.
        r2 = await client.get(
            "/api/v1/logs",
            params={"service": service, "trace_id": trace_ids[0], "limit": 10},
        )
        assert r2.status_code == 200
        payload = r2.json()
        assert payload["count"] >= 1
        assert any(item["trace_id"] == trace_ids[0] for item in payload["items"])

        # Verify stats endpoint returns data.
        r3 = await client.get(
            "/api/v1/logs/stats",
            params={"group_by": "level", "service": service, "limit": 10},
        )
        assert r3.status_code == 200
        stats = r3.json()
        assert stats["group_by"] == "level"
        assert isinstance(stats["items"], list)

