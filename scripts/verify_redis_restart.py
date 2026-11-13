"""Prepare/verify a named isolated Redis durability probe across container restart."""

import argparse
import asyncio
from datetime import datetime, timezone

from app.config import settings
from app.services.state import RedisState
from redis.asyncio import Redis


async def run(mode: str, prefix: str):
    if not prefix.startswith("pipewatch-restart-test:"):
        raise ValueError("Use a unique pipewatch-restart-test: prefix")
    settings.REDIS_KEY_PREFIX = prefix
    redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
    state = RedisState(redis)
    try:
        if mode == "prepare":
            await state.enqueue([{"timestamp": datetime.now(timezone.utc), "message": "restart marker"}])
            rule = {"name": "restart marker"}
            await state.save_rule("", "restart-rule", rule)
            event = {"event_id": "restart-event", "rule_id": "restart-rule", "delivery_status": "pending"}
            assert await state.begin_event("", "restart-rule", event, 3600, rule)
        else:
            assert await redis.llen(state.queue) == 1
            assert (await state.rule("", "restart-rule"))["name"] == "restart marker"
            assert (await state.events(""))[0]["event_id"] == "restart-event"
            assert await redis.exists(f"{prefix}:alerts:standalone:cooldown:restart-rule")
            async for key in redis.scan_iter(f"{prefix}:*"):
                await redis.expire(key, 60)
        print(f"PASS: Redis durable probe {mode}")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("prepare", "verify"))
    parser.add_argument("prefix")
    args = parser.parse_args()
    asyncio.run(run(args.mode, args.prefix))
