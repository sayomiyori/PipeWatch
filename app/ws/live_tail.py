from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from app.config import settings
from app.services.identity import authorize
from app.services.stream import stream_name

router = APIRouter()


async def _stream_logs(
    websocket: WebSocket,
    stream: str,
    last_id: str,
    level_filter: str | None,
) -> None:
    redis = websocket.app.state.redis
    await websocket.accept()
    try:
        while True:
            try:
                if settings.PLATFORM_AUTH_ENABLED:
                    await authorize(websocket, "tenant.read")
                data = await redis.xread({stream: last_id}, count=100, block=5000)
            except asyncio.CancelledError:
                break

            if not data:
                continue

            for stream_name, entries in data:
                for entry_id, fields in entries:
                    last_id = entry_id
                    try:
                        payload: dict[str, Any] = dict(fields)

                        if level_filter and payload.get("level") != level_filter:
                            continue

                        await websocket.send_text(
                            json.dumps(
                                {"id": entry_id, "stream": stream_name, "log": payload},
                                ensure_ascii=False,
                            )
                        )
                    except WebSocketDisconnect:
                        raise
                    except Exception:
                        continue
    except HTTPException as exc:
        await websocket.close(code=1011 if exc.status_code == 503 else 1008)
    except WebSocketDisconnect:
        return
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


@router.websocket("/ws/tail")
async def tail_ws(websocket: WebSocket) -> None:
    """
    Live tail with optional level filter.
    Query params:
      service  – required, stream name suffix
      level    – optional: debug | info | warn | error | fatal
      start    – latest (default) | begin
    """
    params = websocket.query_params
    try:
        tenant_id = await authorize(websocket, "tenant.read")
    except HTTPException as exc:
        await websocket.close(code=1011 if exc.status_code == 503 else 1008)
        return
    service = params.get("service")
    if not service or len(service) > 256:
        await websocket.close(code=1008)
        return

    level_filter = params.get("level") or None
    if level_filter not in (None, "debug", "info", "warn", "error", "fatal"):
        await websocket.close(code=1008)
        return
    start = params.get("start", "latest")
    last_id = "$" if start == "latest" else "0-0"
    stream = stream_name(tenant_id, service)

    if websocket.app.state.redis is None:
        await websocket.close(code=1011)
        return

    await _stream_logs(websocket, stream, last_id, level_filter)


@router.websocket("/ws/live-tail")
async def live_tail_ws(websocket: WebSocket) -> None:
    """Backward-compatible alias for /ws/tail (no level filter)."""
    await tail_ws(websocket)
