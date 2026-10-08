from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from app.api.v1.alerts import router as alerts_router
from app.api.v1.ingest import router as ingest_router
from app.api.v1.query import router as query_router
from app.config import settings
from app.middleware.metrics import MetricsMiddleware, register_metrics_route
from app.services.alert_engine import AlertEngine
from app.services.clickhouse import ClickHouseService
from app.services.stream import RedisStreamPublisher
from app.ws.live_tail import router as ws_router

logger = logging.getLogger(__name__)


async def _flush_loop(app: FastAPI) -> None:
    """
    Background loop:
    - consumes log records from ingest_queue
    - flushes to ClickHouse every APP_BATCH_FLUSH_INTERVAL_SECONDS or after APP_BATCH_SIZE
    - publishes to Redis Streams for live tail
    """
    queue: asyncio.Queue[dict[str, Any]] = app.state.ingest_queue
    clickhouse: ClickHouseService = app.state.clickhouse
    publisher: RedisStreamPublisher = app.state.redis_publisher

    batch: list[dict[str, Any]] = []
    interval = float(settings.APP_BATCH_FLUSH_INTERVAL_SECONDS)
    batch_size = int(settings.APP_BATCH_SIZE)
    last_flush_monotonic = asyncio.get_event_loop().time()

    async def persist_batch() -> None:
        # Cancellation cannot stop a DB write already running in a thread.
        write = asyncio.create_task(asyncio.to_thread(clickhouse.insert_logs, list(batch)))
        try:
            await asyncio.shield(write)
        except asyncio.CancelledError:
            try:
                await write
            except Exception:
                logger.exception("In-flight log flush failed during shutdown")
            else:
                batch.clear()
            raise
        batch.clear()

    try:
        while True:
            loop = asyncio.get_event_loop()
            time_since_flush = loop.time() - last_flush_monotonic
            timeout = max(0.0, interval - time_since_flush)
            if timeout == 0.0 and not batch:
                # If we're not holding anything to flush, allow blocking for new items.
                timeout = interval
            try:
                item = await asyncio.wait_for(queue.get(), timeout=timeout if timeout > 0 else 0.0)
                batch.append(item)
            except asyncio.TimeoutError:
                pass
            if batch and (len(batch) >= batch_size or loop.time() - last_flush_monotonic >= interval):
                committed = list(batch)
                try:
                    await persist_batch()
                except Exception:
                    # Keep the accepted batch and apply backpressure until storage recovers.
                    logger.exception("Log batch flush failed; retrying")
                    await asyncio.sleep(interval)
                    continue
                try:
                    await publisher.publish_logs(committed)
                except Exception:
                    # Tailing depends on Redis; ingestion must keep working.
                    pass
                last_flush_monotonic = loop.time()
    except asyncio.CancelledError:
        # Drain accepted records while storage is available; live tail is best effort.
        while batch or not queue.empty():
            while len(batch) < batch_size:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                await persist_batch()
            except Exception:
                logger.exception("Final log flush failed; accepted records remain in memory")
                break
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Shared clients and in-memory state.
    app.state.clickhouse = ClickHouseService()
    app.state.clickhouse.ensure_tables()

    publisher = RedisStreamPublisher()
    app.state.redis_publisher = publisher
    # Expose raw redis client for WS layer.
    app.state.redis = publisher.redis

    app.state.ingest_queue = asyncio.Queue(maxsize=settings.APP_QUEUE_MAXSIZE)

    app.state.alert_rules = {}
    app.state.alert_events = []
    app.state.alert_last_fire = {}

    # Background tasks.
    flush_task = asyncio.create_task(_flush_loop(app))
    alert_engine = AlertEngine(clickhouse=app.state.clickhouse)
    alert_task = asyncio.create_task(alert_engine.run_forever(app))

    yield

    # Shutdown.
    for task in [flush_task, alert_task]:
        task.cancel()
    await asyncio.gather(flush_task, alert_task, return_exceptions=True)

    try:
        await publisher.close()
    except Exception:
        pass


app = FastAPI(
    title="PipeWatch",
    version="0.1.0",
    lifespan=lifespan,
)

if settings.METRICS_ENABLED:
    app.add_middleware(MetricsMiddleware)

app.include_router(ingest_router)
app.include_router(query_router)
app.include_router(alerts_router)
app.include_router(ws_router)

register_metrics_route(app)

