"""Unit tests for Query API endpoints using a mocked ClickHouseService."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI

THIS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from app.api.v1.query import router as query_router  # noqa: E402

# Minimal test app — no lifespan, we inject state manually.
_app = FastAPI()
_app.include_router(query_router)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_row(
    service: str = "svc",
    level: str = "info",
    message: str = "test",
    trace_id: str = "",
) -> dict[str, Any]:
    return {
        "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "service":   service,
        "level":     level,
        "message":   message,
        "trace_id":  trace_id,
        "span_id":   "",
        "metadata":  "{}",
        "host":      "",
    }


FULL_STATS = {
    "total_count":       42,
    "count_by_level":    {"error": 10, "info": 32},
    "count_by_service":  {"svc-a": 20, "svc-b": 22},
    "error_rate_per_hour": [{"hour": "2024-01-01T00:00:00", "count": 5}],
    "top_errors":        [{"message": "oops", "service": "svc-a", "count": 3}],
}


@pytest.fixture
def mock_ch() -> MagicMock:
    ch = MagicMock()
    ch.count_logs.return_value = 1
    ch.query_logs.return_value = [_make_row()]
    ch.query_full_stats.return_value = FULL_STATS
    ch.query_services.return_value = [
        {"service": "svc-a", "last_seen": "2024-01-01T00:00:00", "total_logs": 20}
    ]
    return ch


@pytest.fixture
def transport(mock_ch: MagicMock) -> httpx.ASGITransport:
    _app.state.clickhouse = mock_ch
    return httpx.ASGITransport(app=_app)


# ──────────────────────────────────────────────────────────────────────────────
# GET /api/v1/logs
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_logs_service_filter(transport: httpx.ASGITransport, mock_ch: MagicMock) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs", params={"service": "payment-api"})
    assert r.status_code == 200
    kw = mock_ch.query_logs.call_args.kwargs
    assert kw["service"] == "payment-api"


@pytest.mark.asyncio
async def test_logs_level_filter(transport: httpx.ASGITransport, mock_ch: MagicMock) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs", params={"level": "error"})
    assert r.status_code == 200
    kw = mock_ch.query_logs.call_args.kwargs
    assert kw["level"] == "error"


@pytest.mark.asyncio
async def test_logs_text_search(transport: httpx.ASGITransport, mock_ch: MagicMock) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs", params={"q": "timeout"})
    assert r.status_code == 200
    kw = mock_ch.query_logs.call_args.kwargs
    assert kw["q"] == "timeout"


@pytest.mark.asyncio
async def test_logs_pagination_offset(transport: httpx.ASGITransport, mock_ch: MagicMock) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs", params={"page": 3, "size": 10})
    assert r.status_code == 200
    data = r.json()
    assert data["page"] == 3
    assert data["size"] == 10
    kw = mock_ch.query_logs.call_args.kwargs
    assert kw["offset"] == 20  # (3-1) * 10
    assert kw["limit"]  == 10


@pytest.mark.asyncio
async def test_logs_response_shape(transport: httpx.ASGITransport, mock_ch: MagicMock) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs")
    assert r.status_code == 200
    data = r.json()
    assert "total" in data
    assert "page"  in data
    assert "size"  in data
    assert "items" in data
    assert isinstance(data["items"], list)


# ──────────────────────────────────────────────────────────────────────────────
# GET /api/v1/logs/stats
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stats_structure(transport: httpx.ASGITransport) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs/stats")
    assert r.status_code == 200
    data = r.json()
    for key in ("total_count", "count_by_level", "count_by_service",
                "error_rate_per_hour", "top_errors"):
        assert key in data, f"Missing key: {key}"


@pytest.mark.asyncio
async def test_stats_values(transport: httpx.ASGITransport) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs/stats")
    data = r.json()
    assert data["total_count"] == 42
    assert data["count_by_level"]["error"] == 10
    assert data["count_by_service"]["svc-a"] == 20
    assert data["top_errors"][0]["message"] == "oops"


@pytest.mark.asyncio
async def test_stats_passes_time_range(
    transport: httpx.ASGITransport, mock_ch: MagicMock
) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get(
            "/api/v1/logs/stats",
            params={"from_ts": "2024-01-01T00:00:00", "to_ts": "2024-01-02T00:00:00"},
        )
    assert r.status_code == 200
    kw = mock_ch.query_full_stats.call_args.kwargs
    assert kw["from_ts"] is not None
    assert kw["to_ts"] is not None


# ──────────────────────────────────────────────────────────────────────────────
# GET /api/v1/logs/services
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_services_endpoint(transport: httpx.ASGITransport) -> None:
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/v1/logs/services")
    assert r.status_code == 200
    data = r.json()
    assert "count"    in data
    assert "services" in data
    assert isinstance(data["services"], list)
    assert data["count"] == 1
    assert data["services"][0]["service"] == "svc-a"
