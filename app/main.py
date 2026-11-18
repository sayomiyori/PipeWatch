from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app.api.v1.alerts import router as alerts_router
from app.api.v1.ingest import router as ingest_router
from app.api.v1.query import router as query_router
from app.config import settings
from app.middleware.limits import RequestSizeLimitMiddleware
from app.middleware.metrics import MetricsMiddleware, register_metrics_route
from app.services.alert_engine import AlertEngine
from app.services.clickhouse import ClickHouseService
from app.services.state import RedisState
from app.services.stream import RedisStreamPublisher
from app.ws.live_tail import router as ws_router

logger = logging.getLogger(__name__)


async def _flush_loop(app: FastAPI) -> None:
    store: RedisState = app.state.store
    interval = max(0.01, settings.APP_BATCH_FLUSH_INTERVAL_SECONDS)
    while True:
        token = uuid4().hex
        try:
            if not await store.redis.set(store.lease, token, nx=True, px=30000):
                await asyncio.sleep(interval)
                continue
            lost = asyncio.Event()

            async def renew() -> None:
                while True:
                    await asyncio.sleep(5)
                    try:
                        if not await store.renew(token):
                            lost.set()
                            return
                    except RedisError:
                        lost.set()
                        return

            heartbeat = asyncio.create_task(renew())
            try:
                while not lost.is_set():
                    batch = await store.claim(token)
                    if not batch:
                        await asyncio.sleep(interval)
                        continue
                    # The lease fences acknowledgement; an ambiguous DB write may replay.
                    write = asyncio.create_task(asyncio.to_thread(app.state.clickhouse.insert_logs, batch))
                    cancelled = False
                    try:
                        await asyncio.shield(write)
                    except asyncio.CancelledError:
                        cancelled = True
                        await write
                    except Exception:
                        logger.warning("ClickHouse insert failed; pending records retained")
                        await asyncio.sleep(interval)
                        continue
                    if await store.acknowledge(token):
                        try:
                            await app.state.redis_publisher.publish_logs(batch)
                        except RedisError:
                            logger.warning("Live tail publication failed after confirmed storage")
                    if cancelled:
                        raise asyncio.CancelledError
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                with suppress(RedisError):
                    await store.release(token)
        except RedisError:
            logger.warning("Redis unavailable; durable flushing paused")
            await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.PLATFORM_AUTH_ENABLED and not settings.AUTHFORTRESS_URL:
        raise RuntimeError("AUTHFORTRESS_URL is required in platform mode")
    if settings.APP_BATCH_SIZE < 1 or settings.APP_QUEUE_MAXSIZE < 1 or settings.APP_BATCH_FLUSH_INTERVAL_SECONDS <= 0:
        raise RuntimeError("Ingest batch size, capacity and interval must be positive")
    app.state.clickhouse = await asyncio.to_thread(ClickHouseService)
    await asyncio.to_thread(app.state.clickhouse.ensure_tables)
    publisher = RedisStreamPublisher()
    app.state.redis_publisher = publisher
    app.state.redis = publisher.redis
    app.state.store = RedisState(publisher.redis)
    try:
        config = await publisher.redis.config_get("appendonly", "appendfsync", "maxmemory-policy")
        if (config.get("appendonly") != "yes" or config.get("appendfsync") != "always"
                or config.get("maxmemory-policy") != "noeviction"):
            raise RuntimeError("Redis requires appendonly=yes, appendfsync=always and maxmemory-policy=noeviction")
        flush_task = asyncio.create_task(_flush_loop(app))
        alert_task = asyncio.create_task(AlertEngine(app.state.clickhouse).run_forever(app))
        app.state.flush_task = flush_task
        app.state.alert_task = alert_task
        try:
            yield
        finally:
            for task in (flush_task, alert_task):
                task.cancel()
            await asyncio.gather(flush_task, alert_task, return_exceptions=True)
    finally:
        await publisher.close()
        await asyncio.to_thread(app.state.clickhouse._client.close)


app = FastAPI(title="PipeWatch", version="0.1.0", lifespan=lifespan)
app.add_middleware(RequestSizeLimitMiddleware)
if settings.METRICS_ENABLED:
    app.add_middleware(MetricsMiddleware)


@app.exception_handler(RedisError)
async def redis_unavailable(request: Request, error: RedisError):
    return JSONResponse({"detail": "Durable state unavailable"}, status_code=503)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    try:
        if app.state.flush_task.done() or app.state.alert_task.done():
            raise RuntimeError("Background processing stopped")
        await app.state.redis.ping()
        await asyncio.to_thread(app.state.clickhouse._client.query, "SELECT 1")
    except Exception:
        raise HTTPException(503, "Storage unavailable") from None
    return {"status": "ready"}


app.include_router(ingest_router)
app.include_router(query_router)
app.include_router(alerts_router)
app.include_router(ws_router)
register_metrics_route(app)
