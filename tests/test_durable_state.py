import asyncio
import os
import subprocess
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from app.config import settings
from app.main import _flush_loop
from app.services.clickhouse import ClickHouseService
from app.services.state import RedisState
from app.services.stream import RedisStreamPublisher
from fastapi import HTTPException


@pytest.mark.asyncio
async def test_batch_capacity_is_atomic_and_claim_fences_stale_ack(monkeypatch, redis_state):
    monkeypatch.setattr(settings, "APP_QUEUE_MAXSIZE", 2)
    record = {"timestamp": datetime.now(timezone.utc), "message": "durable"}
    await redis_state.enqueue([record])
    with pytest.raises(HTTPException) as error:
        await redis_state.enqueue([record, record])
    assert error.value.status_code == 429
    assert await redis_state.redis.llen(redis_state.queue) == 1
    await redis_state.redis.set(redis_state.lease, "new-worker", px=30000)
    assert await redis_state.claim("stale-worker") == []
    assert await redis_state.claim("new-worker") == [record]
    assert not await redis_state.acknowledge("stale-worker")
    assert await redis_state.redis.llen(redis_state.processing) == 1


@pytest.mark.asyncio
async def test_abrupt_flusher_loss_is_replayed_after_process_restart(redis_state):
    service = f"process-loss-{uuid4().hex}"
    record = {"timestamp": datetime.now(timezone.utc), "service": service, "level": 2, "message": "accepted before kill"}
    await redis_state.enqueue([record])
    code = """
import asyncio, os
from types import SimpleNamespace
from redis.asyncio import Redis
from app.config import settings
from app.main import _flush_loop
from app.services.state import RedisState
async def main():
    redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
    store = RedisState(redis)
    def die(rows):
        os._exit(17)
    app = SimpleNamespace(state=SimpleNamespace(store=store, clickhouse=SimpleNamespace(insert_logs=die)))
    await _flush_loop(app)
asyncio.run(main())
"""
    environment = dict(os.environ, REDIS_KEY_PREFIX=redis_state.prefix)
    result = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", code], env=environment, capture_output=True, timeout=10)
    assert result.returncode == 17, result.stderr.decode()
    assert await redis_state.redis.llen(redis_state.processing) == 1
    # Shorten only the killed test worker's lease instead of deleting state.
    await redis_state.redis.pexpire(redis_state.lease, 1)
    database = ClickHouseService()
    database.ensure_tables()
    publisher = RedisStreamPublisher()
    monkey_app = SimpleNamespace(state=SimpleNamespace(store=redis_state, clickhouse=database, redis_publisher=publisher))
    task = asyncio.create_task(_flush_loop(monkey_app))
    try:
        async with asyncio.timeout(10):
            while database.count_logs(service=service) != 1 or await redis_state.redis.llen(redis_state.processing):
                await asyncio.sleep(0.02)
        assert await redis_state.redis.llen(redis_state.queue) == 0
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await publisher.close()
        database._client.close()


@pytest.mark.asyncio
async def test_rules_history_and_cooldown_survive_state_recreation(redis_state):
    tenant, rule_id = str(uuid4()), str(uuid4())
    rule = {"name": "persistent"}
    await redis_state.save_rule(tenant, rule_id, rule)
    event = {"event_id": str(uuid4()), "rule_id": rule_id, "delivery_status": "pending"}
    assert await redis_state.begin_event(tenant, rule_id, event, 60, rule)
    restarted = RedisState(redis_state.redis)
    assert await restarted.rule(tenant, rule_id) == rule
    assert await restarted.events(tenant) == [event]
    assert not await restarted.begin_event(tenant, rule_id, {**event, "event_id": str(uuid4())}, 60, rule)
    with pytest.raises(HTTPException) as error:
        await restarted.rule(str(uuid4()), rule_id)
    assert error.value.status_code == 404
    with pytest.raises(HTTPException) as error:
        await restarted.save_rule(tenant, rule_id, {"name": "new"}, {"name": "stale"})
    assert error.value.status_code == 409
