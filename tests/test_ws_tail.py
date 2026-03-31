"""Tests for /ws/tail WebSocket endpoint."""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

THIS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from app.ws.live_tail import router as ws_router  # noqa: E402

# Minimal app — no lifespan, we inject state manually.
_app = FastAPI()
_app.include_router(ws_router)


def _make_entry(
    entry_id: str,
    service: str = "svc",
    level: str = "info",
    message: str = "hello",
) -> tuple[str, dict[str, str]]:
    return (
        entry_id,
        {
            "service":   service,
            "level":     level,
            "message":   message,
            "timestamp": "2024-01-01T00:00:00",
            "trace_id":  "",
            "span_id":   "",
            "metadata":  "{}",
            "host":      "",
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Connection rejection tests (no external deps)
# ──────────────────────────────────────────────────────────────────────────────

def test_ws_tail_rejects_missing_service() -> None:
    """Server closes with code 1008 when `service` query param is absent."""
    _app.state.redis = AsyncMock()
    with TestClient(_app) as client:
        with pytest.raises(Exception):
            with client.websocket_connect("/ws/tail") as ws:
                ws.receive_json()


def test_ws_tail_rejects_null_redis() -> None:
    """Server closes with code 1011 when Redis is unavailable."""
    _app.state.redis = None
    with TestClient(_app) as client:
        with pytest.raises(Exception):
            with client.websocket_connect("/ws/tail?service=svc") as ws:
                ws.receive_json()


# ──────────────────────────────────────────────────────────────────────────────
# Level-filter test
# ──────────────────────────────────────────────────────────────────────────────

def test_ws_tail_level_filter() -> None:
    """Only messages matching the requested level reach the client."""
    call_count = 0

    async def mock_xread(
        streams: dict, count: int | None = None, block: int | None = None
    ) -> list:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First call: return one info and one error entry.
            return [
                (
                    "logs:svc",
                    [
                        _make_entry("1-0", level="info",  message="should-be-filtered"),
                        _make_entry("2-0", level="error", message="should-arrive"),
                    ],
                )
            ]
        # Subsequent calls: keep returning data so the server stays "active"
        # and the WebSocketDisconnect triggered by client close is caught promptly.
        return [
            (
                "logs:svc",
                [_make_entry(f"{call_count}-0", level="error", message=f"loop-{call_count}")],
            )
        ]

    mock_redis      = AsyncMock()
    mock_redis.xread = mock_xread
    _app.state.redis = mock_redis

    with TestClient(_app) as client:
        with client.websocket_connect("/ws/tail?service=svc&level=error") as ws:
            data = ws.receive_json()
            assert data["log"]["level"]   == "error"
            assert data["log"]["message"] == "should-arrive"
            # Close after receiving the first matching message.


# ──────────────────────────────────────────────────────────────────────────────
# No-filter test (all levels pass through)
# ──────────────────────────────────────────────────────────────────────────────

def test_ws_tail_no_filter_passes_all_levels() -> None:
    """Without a level filter every message is forwarded."""
    call_count = 0

    async def mock_xread(
        streams: dict, count: int | None = None, block: int | None = None
    ) -> list:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [
                (
                    "logs:svc",
                    [_make_entry("1-0", level="debug", message="debug-msg")],
                )
            ]
        return [
            (
                "logs:svc",
                [_make_entry(f"{call_count}-0", level="info", message="keep-alive")],
            )
        ]

    mock_redis       = AsyncMock()
    mock_redis.xread = mock_xread
    _app.state.redis = mock_redis

    with TestClient(_app) as client:
        with client.websocket_connect("/ws/tail?service=svc") as ws:
            data = ws.receive_json()
            assert data["log"]["level"]   == "debug"
            assert data["log"]["message"] == "debug-msg"
