import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from app.api.v1.alerts import router as alerts_router
from app.api.v1.ingest import router as ingest_router
from app.api.v1.query import router as query_router
from app.config import settings
from app.main import redis_unavailable
from app.middleware.limits import RequestSizeLimitMiddleware
from app.middleware.metrics import register_metrics_route
from app.services.alert_engine import AlertEngine
from app.services.clickhouse import ClickHouseService
from app.services.stream import stream_name
from app.ws.live_tail import router as ws_router
from fastapi import FastAPI
from fastapi.testclient import TestClient
from redis import Redis as SyncRedis
from redis.exceptions import RedisError
from starlette.websockets import WebSocketDisconnect


@pytest.fixture
def platform(monkeypatch):
    tenant_a, tenant_b, user = str(uuid4()), str(uuid4()), str(uuid4())
    monkeypatch.setattr(settings, "PLATFORM_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "AUTHFORTRESS_URL", "http://identity.invalid")
    calls = []
    original = httpx.AsyncClient.post

    async def identity_post(client, url, **kwargs):
        if not str(url).startswith("http://identity.invalid/"):
            return await original(client, url, **kwargs)
        calls.append((url, kwargs))
        bearer = kwargs["headers"]["Authorization"]
        tenant = str(url).split("/")[-2]
        permission = kwargs["json"]["permission"]
        status = 200
        if bearer == "Bearer invalid":
            status = 401
        elif bearer == "Bearer unavailable":
            raise httpx.ConnectError("Identity unavailable")
        elif tenant != tenant_a:
            status = 404
        elif bearer == "Bearer member" and permission == "tenant.manage":
            status = 403
        data = {"allowed": True, "user_id": user, "tenant_id": tenant, "permission": permission, "role": "member" if bearer == "Bearer member" else "owner"}
        if bearer == "Bearer malformed":
            data["tenant_id"] = tenant_b
        return httpx.Response(status, json=data, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", identity_post)
    return SimpleNamespace(a=tenant_a, b=tenant_b, calls=calls, headers={"Authorization": "Bearer owner", "X-Tenant-ID": tenant_a})


def make_app(store):
    app = FastAPI()
    for router in (ingest_router, query_router, alerts_router, ws_router):
        app.include_router(router)
    app.add_middleware(RequestSizeLimitMiddleware)
    app.add_exception_handler(RedisError, redis_unavailable)
    register_metrics_route(app)
    app.state.store = store
    return app


@pytest.mark.asyncio
async def test_all_api_routes_fail_closed_and_preserve_identity_status(platform, redis_state):
    app = make_app(redis_state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        for method, path in (("GET", "/api/v1/logs"), ("GET", "/api/v1/logs/stats"), ("GET", "/api/v1/logs/services"),
                             ("POST", "/api/v1/logs"), ("POST", "/api/v1/logs/batch"),
                             ("GET", "/api/v1/alerts"), ("POST", "/api/v1/alerts"),
                             ("GET", "/api/v1/alerts/events"), ("GET", "/api/v1/alerts/id/history"),
                             ("GET", "/api/v1/alerts/id"), ("PATCH", "/api/v1/alerts/id"), ("DELETE", "/api/v1/alerts/id")):
            response = await client.request(method, path, json={} if method in ("POST", "PATCH") else None)
            assert response.status_code == 401, (method, path, response.text)
        for token, tenant, expected in (("invalid", platform.a, 401), ("owner", platform.b, 404), ("unavailable", platform.a, 503), ("malformed", platform.a, 503)):
            response = await client.get("/api/v1/alerts", headers={"Authorization": f"Bearer {token}", "X-Tenant-ID": tenant})
            assert response.status_code == expected
        response = await client.post("/api/v1/alerts", headers={**platform.headers, "Authorization": "Bearer member"}, json={"name": "denied"})
        assert response.status_code == 403
        assert (await client.get("/metrics", headers=platform.headers)).status_code == 404
        assert await redis_state.rules(platform.a) == {}
        assert await redis_state.redis.llen(redis_state.queue) == 0


@pytest.mark.asyncio
async def test_real_clickhouse_queries_ingestion_and_rules_are_tenant_scoped(platform, redis_state):
    from datetime import datetime, timezone

    database = ClickHouseService()
    database.ensure_tables()
    service = f"tenant-boundary-{uuid4().hex}"
    database.insert_logs([
        {"timestamp": datetime.now(timezone.utc), "service": service, "level": 4, "message": message, "tenant_id": tenant}
        for message, tenant in (("own data", platform.a), ("other data", platform.b), ("standalone data", ""))
    ])
    app = make_app(redis_state)
    app.state.clickhouse = database
    foreign_id = str(uuid4())
    await redis_state.save_rule(platform.b, foreign_id, {"name": "foreign"})
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=platform.headers) as client:
            response = await client.get("/api/v1/logs", params={"service": service, "tenant_id": platform.b})
            assert response.status_code == 200
            assert response.json()["total"] == 1
            assert [row["message"] for row in response.json()["items"]] == ["own data"]
            stats = (await client.get("/api/v1/logs/stats")).json()
            assert stats["total_count"] == 1
            assert stats["top_errors"][0]["message"] == "own data"
            services = (await client.get("/api/v1/logs/services")).json()
            assert services["services"][0]["total_logs"] == 1
            for method, suffix in (("GET", ""), ("GET", "/history"), ("PATCH", ""), ("DELETE", "")):
                response = await client.request(method, f"/api/v1/alerts/{foreign_id}{suffix}", json={"name": "stolen"} if method == "PATCH" else None)
                assert response.status_code == 404
            assert (await client.get("/api/v1/alerts")).json()["rules"] == {}
            assert (await client.get("/api/v1/alerts/events")).json()["events"] == []
            created = (await client.post("/api/v1/alerts", json={"name": "own"})).json()
            rule_id = created["rule_id"]
            assert (await client.patch(f"/api/v1/alerts/{rule_id}", json={"threshold": -1})).status_code == 422
            assert (await client.patch(f"/api/v1/alerts/{rule_id}", json={"name": None})).status_code == 422
            assert (await client.patch(f"/api/v1/alerts/{rule_id}", json={"tenant_id": platform.b})).status_code == 422
            assert (await client.get(f"/api/v1/alerts/{rule_id}")).json()["rule"]["name"] == "own"
            payload = {"service": service, "level": "info", "message": "accepted"}
            assert (await client.post("/api/v1/logs", json={**payload, "tenant_id": platform.b})).status_code == 422
            assert (await client.post("/api/v1/logs", json=payload)).json()["status"] == "accepted"
            queued = json.loads((await redis_state.redis.lrange(redis_state.queue, 0, -1))[0])
            assert queued["tenant_id"] == platform.a
            assert (await client.post("/api/v1/logs/batch", json={"logs": []})).status_code == 422
            assert (await client.post("/api/v1/logs", content=b"x" * 1048577)).status_code == 413
    finally:
        database._client.close()
    assert any(call[1]["json"] == {"permission": "tenant.manage"} for call in platform.calls)
    assert any(call[1]["json"] == {"permission": "tenant.read"} for call in platform.calls)


@pytest.mark.asyncio
async def test_identity_is_not_cached_and_redis_outage_is_503(platform, redis_state, monkeypatch):
    app = make_app(redis_state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=platform.headers) as client:
        assert (await client.get("/api/v1/alerts")).status_code == 200
        assert (await client.get("/api/v1/alerts")).status_code == 200
        assert len(platform.calls) == 2
        async def fail(*args, **kwargs):
            raise RedisError("Unavailable")
        monkeypatch.setattr(redis_state.redis, "eval", fail)
        response = await client.post("/api/v1/logs", json={"service": "svc", "level": "info", "message": "outage"})
        assert response.status_code == 503


@pytest.mark.asyncio
async def test_alert_evaluation_counts_only_its_tenant_and_skips_legacy_scope(platform, redis_state):
    from datetime import datetime, timezone

    from app.api.v1.alerts import AlertRuleCreate
    database = ClickHouseService()
    database.ensure_tables()
    service = f"alert-isolation-{uuid4().hex}"
    database.insert_logs([
        {"timestamp": datetime.now(timezone.utc), "service": service, "level": 4, "message": "scoped", "tenant_id": tenant}
        for tenant in (platform.a, platform.b, platform.b, "")
    ])
    rule = AlertRuleCreate(name="isolated", service_filter=service).model_dump()
    for tenant in (platform.a, platform.b, ""):
        await redis_state.save_rule(tenant, str(uuid4()), rule)
    try:
        await AlertEngine(database).evaluate_once(SimpleNamespace(state=SimpleNamespace(store=redis_state)))
        assert (await redis_state.events(platform.a))[0]["count"] == 1
        assert (await redis_state.events(platform.b))[0]["count"] == 2
        assert await redis_state.events("") == []
    finally:
        database._client.close()


@pytest.mark.parametrize("path", ["/ws/tail", "/ws/live-tail"])
def test_websocket_uses_tenant_stream_and_rechecks_revocation(platform, isolated_prefix, monkeypatch, path):
    redis = SyncRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
    redis.xadd(stream_name(platform.a, "same-service"), {"message": "own message"})
    redis.xadd(stream_name(platform.b, "same-service"), {"message": "foreign message"})
    app = make_app(None)
    from redis.asyncio import Redis
    app.state.redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
    original = httpx.AsyncClient.post
    calls = 0
    async def revoke(client, url, **kwargs):
        nonlocal calls
        calls += 1
        if calls >= 3:
            return httpx.Response(404, request=httpx.Request("POST", url))
        return await original(client, url, **kwargs)
    monkeypatch.setattr(httpx.AsyncClient, "post", revoke)
    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(f"{path}?service=same-service&start=begin"):
                pass
        with client.websocket_connect(f"{path}?service=same-service&start=begin", headers=platform.headers) as websocket:
            first = websocket.receive_json()
            assert first["log"]["message"] == "own message"
            assert platform.a in first["stream"]
            with pytest.raises(WebSocketDisconnect) as error:
                websocket.receive_json()
            assert error.value.code == 1008
    redis.close()
