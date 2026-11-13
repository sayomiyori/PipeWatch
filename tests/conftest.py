from uuid import uuid4

import pytest
import pytest_asyncio
from app.config import settings
from app.services.state import RedisState
from redis.asyncio import Redis


@pytest.fixture
def isolated_prefix(monkeypatch):
    prefix = f"pipewatch-test:{uuid4().hex}"
    monkeypatch.setattr(settings, "REDIS_KEY_PREFIX", prefix)
    return prefix


@pytest_asyncio.fixture
async def redis_state(isolated_prefix):
    redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB, decode_responses=True)
    await redis.ping()
    state = RedisState(redis)
    yield state
    # Expire only this test's namespace, never flush a shared database.
    async for key in redis.scan_iter(f"{isolated_prefix}:*"):
        await redis.expire(key, 60)
    await redis.aclose()
