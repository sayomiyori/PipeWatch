from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from redis.asyncio import Redis

from app.config import settings


class RedisState:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.prefix = settings.REDIS_KEY_PREFIX
        self.queue = f"{self.prefix}:ingest:queued"
        self.processing = f"{self.prefix}:ingest:processing"
        self.lease = f"{self.prefix}:ingest:lease"

    async def enqueue(self, records: list[dict[str, Any]]) -> None:
        encoded = [json.dumps(row, default=lambda value: value.isoformat()) for row in records]
        result = await self.redis.eval(
            """if redis.call('LLEN', KEYS[1]) + redis.call('LLEN', KEYS[2]) + #ARGV - 1 > tonumber(ARGV[1]) then return 0 end
            for i=2,#ARGV do redis.call('RPUSH', KEYS[1], ARGV[i]) end
            return 1""", 2, self.queue, self.processing, settings.APP_QUEUE_MAXSIZE, *encoded,
        )
        if not result:
            raise HTTPException(429, "Ingest backlog is full")

    async def claim(self, token: str) -> list[dict[str, Any]]:
        rows = await self.redis.eval(
            """if redis.call('GET', KEYS[3]) ~= ARGV[1] then return {} end
            if redis.call('LLEN', KEYS[2]) == 0 then
              for i=1,tonumber(ARGV[2]) do
                local row=redis.call('LPOP', KEYS[1]); if not row then break end
                redis.call('RPUSH', KEYS[2], row)
              end
            end
            return redis.call('LRANGE', KEYS[2], 0, -1)""",
            3, self.queue, self.processing, self.lease, token, settings.APP_BATCH_SIZE,
        )
        decoded = [json.loads(row) for row in rows]
        for row in decoded:
            row["timestamp"] = datetime.fromisoformat(row["timestamp"])
        return decoded

    async def acknowledge(self, token: str) -> bool:
        result = await self.redis.eval(
            "if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end; redis.call('DEL', KEYS[1]); return 1",
            2, self.processing, self.lease, token,
        )
        return bool(result)

    async def renew(self, token: str) -> bool:
        return bool(await self.redis.eval(
            "if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end; redis.call('PEXPIRE', KEYS[1], 30000); return 1",
            1, self.lease, token,
        ))

    async def release(self, token: str) -> None:
        await self.redis.eval(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end; return 0",
            1, self.lease, token,
        )

    def rule_key(self, tenant_id: str) -> str:
        return f"{self.prefix}:alerts:{tenant_id or 'standalone'}:rules"

    async def rules(self, tenant_id: str) -> dict[str, dict[str, Any]]:
        return {key: json.loads(value) for key, value in (await self.redis.hgetall(self.rule_key(tenant_id))).items()}

    async def rule(self, tenant_id: str, rule_id: str) -> dict[str, Any]:
        raw = await self.redis.hget(self.rule_key(tenant_id), rule_id)
        if raw is None:
            raise HTTPException(404, "Rule not found")
        return json.loads(raw)

    async def save_rule(self, tenant_id: str, rule_id: str, rule: dict[str, Any], previous: dict | None = None) -> None:
        encoded = json.dumps(rule, sort_keys=True)
        result = await self.redis.eval(
            """local current=redis.call('HGET', KEYS[1], ARGV[1])
            if ARGV[3] == '' then
              if redis.call('HLEN', KEYS[1]) >= 100 then return 0 end
            elseif current ~= ARGV[3] then return -1 end
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]); redis.call('SADD', KEYS[2], ARGV[4]); return 1""",
            2, self.rule_key(tenant_id), f"{self.prefix}:alerts:scopes", rule_id, encoded,
            json.dumps(previous, sort_keys=True) if previous is not None else "", tenant_id or "standalone",
        )
        if result != 1:
            raise HTTPException(409 if result == -1 else 429, "Concurrent rule change" if result == -1 else "Rule limit reached")

    async def delete_rule(self, tenant_id: str, rule_id: str) -> dict:
        raw = await self.redis.eval(
            "local row=redis.call('HGET', KEYS[1], ARGV[1]); if row then redis.call('HDEL', KEYS[1], ARGV[1]) end; return row",
            1, self.rule_key(tenant_id), rule_id,
        )
        if raw is None:
            raise HTTPException(404, "Rule not found")
        return json.loads(raw)

    async def begin_event(self, tenant_id: str, rule_id: str, event: dict, cooldown: int, rule: dict) -> bool:
        base = f"{self.prefix}:alerts:{tenant_id or 'standalone'}"
        return bool(await self.redis.eval(
            """if redis.call('HGET', KEYS[4], ARGV[4]) ~= ARGV[5] then return 0 end
            if not redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 0 end
            redis.call('HSET', KEYS[3], ARGV[1], ARGV[3]); redis.call('LPUSH', KEYS[2], ARGV[1])
            while redis.call('LLEN', KEYS[2]) > 500 do local id=redis.call('RPOP', KEYS[2]); redis.call('HDEL', KEYS[3], id) end
            return 1""", 4, f"{base}:cooldown:{rule_id}", f"{base}:history", f"{base}:events", self.rule_key(tenant_id),
            event["event_id"], max(1, cooldown), json.dumps(event), rule_id, json.dumps(rule, sort_keys=True),
        ))

    async def finish_event(self, tenant_id: str, event: dict) -> None:
        key = f"{self.prefix}:alerts:{tenant_id or 'standalone'}:events"
        await self.redis.eval(
            "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) end; return 0",
            1, key, event["event_id"], json.dumps(event),
        )

    async def events(self, tenant_id: str) -> list[dict]:
        base = f"{self.prefix}:alerts:{tenant_id or 'standalone'}"
        ids = await self.redis.lrange(f"{base}:history", 0, 499)
        if not ids:
            return []
        return [json.loads(raw) for raw in await self.redis.hmget(f"{base}:events", ids) if raw]
