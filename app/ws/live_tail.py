from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect


router = APIRouter()


@router.websocket("/ws/live-tail")
async def live_tail_ws(websocket: WebSocket) -> None:
    params = websocket.query_params
    service = params.get("service")
    if not service:
        await websocket.close(code=1008)
        return

    start = params.get("start", "latest")
    stream = f"logs:{service}"
    last_id = "$" if start == "latest" else "0-0"

    redis = websocket.app.state.redis
    if redis is None:
        await websocket.close(code=1011)
        return

    await websocket.accept()
    try:
        while True:
            # XREAD blocks until new entries.
            data = await redis.xread({stream: last_id}, count=100, block=5000)
            if not data:
                continue

            # redis-py returns: [(stream_name, [(id, {field: value})...])]
            for stream_name, entries in data:
                for entry_id, fields in entries:
                    last_id = entry_id
                    try:
                        payload = dict(fields)
                        # Normalize JSON field values as plain strings.
                        message = {
                            "id": entry_id,
                            "stream": stream_name,
                            "log": payload,
                        }
                        await websocket.send_text(json.dumps(message, ensure_ascii=False))
                    except WebSocketDisconnect:
                        raise
                    except Exception:
                        # Drop malformed entries.
                        continue
    except WebSocketDisconnect:
        return
    finally:
        try:
            await websocket.close()
        except Exception:
            pass

