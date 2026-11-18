"""Verify PipeWatch against an isolated real AuthFortress using ephemeral identities."""

import asyncio
import os
import secrets
from uuid import uuid4

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


async def verify():
    auth_url = os.getenv("PIPEWATCH_TEST_AUTH_URL", "http://127.0.0.1:55921")
    api_url = os.getenv("PIPEWATCH_TEST_PLATFORM_URL", "http://127.0.0.1:58385")
    for url in (auth_url, api_url):
        if httpx.URL(url).host not in ("127.0.0.1", "localhost"):
            raise ValueError("This smoke test only targets explicitly isolated loopback APIs")
    async with httpx.AsyncClient(timeout=10, trust_env=False) as auth, httpx.AsyncClient(base_url=api_url, timeout=10, trust_env=False) as client:
        async with asyncio.timeout(20):
            while True:
                try:
                    ready = await client.get("/ready")
                    if ready.status_code == 200:
                        schema = (await client.get("/openapi.json")).json()
                        assert schema["info"]["title"] == "PipeWatch"
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.1)
        async def identity():
            marker = uuid4().hex
            body = {"email": f"pipewatch-{marker}@example.com", "username": f"pw-{marker}", "password": secrets.token_urlsafe(32)}
            response = await auth.post(f"{auth_url}/api/v1/auth/register", json=body)
            assert response.status_code == 200, f"Register status: {response.status_code}"
            response = await auth.post(f"{auth_url}/api/v1/auth/login", json={"email": body["email"], "password": body["password"]})
            assert response.status_code == 200, f"Login status: {response.status_code}"
            headers = {"Authorization": f"Bearer {response.json()['access_token']}"}
            response = await auth.post(f"{auth_url}/api/v1/tenants", headers=headers, json={"name": f"pipewatch-smoke-{marker}"})
            assert response.status_code == 201, f"Tenant status: {response.status_code}"
            return {**headers, "X-Tenant-ID": response.json()["id"]}

        owner, foreign = await identity(), await identity()
        try:
            assert (await client.get("/ready")).status_code == 200
            assert (await client.get("/api/v1/alerts")).status_code == 401
            assert (await client.get("/metrics", headers=owner)).status_code == 404
            assert (await client.get("/api/v1/alerts", headers={**owner, "X-Tenant-ID": foreign["X-Tenant-ID"]})).status_code == 404
            service = f"identity-smoke-{uuid4().hex}"
            for headers, message in ((owner, "own message"), (foreign, "foreign message")):
                response = await client.post("/api/v1/logs", headers=headers, json={"service": service, "level": "error", "message": message})
                assert response.status_code == 200 and response.json()["status"] == "accepted"
            async with asyncio.timeout(20):
                while True:
                    response = await client.get("/api/v1/logs", headers=owner, params={"service": service})
                    assert response.status_code == 200
                    if response.json()["total"] == 1:
                        assert response.json()["items"][0]["message"] == "own message"
                        break
                    await asyncio.sleep(0.1)
            own_rule = await client.post("/api/v1/alerts", headers=owner, json={"name": service, "service_filter": service})
            foreign_rule = await client.post("/api/v1/alerts", headers=foreign, json={"name": service, "service_filter": service})
            assert own_rule.status_code == foreign_rule.status_code == 201
            own_id, foreign_id = own_rule.json()["rule_id"], foreign_rule.json()["rule_id"]
            assert (await client.get(f"/api/v1/alerts/{foreign_id}", headers=owner)).status_code == 404
            assert (await client.get(f"/api/v1/alerts/{foreign_id}/history", headers=owner)).status_code == 404
            async with asyncio.timeout(20):
                while True:
                    history = await client.get(f"/api/v1/alerts/{own_id}/history", headers=owner)
                    assert history.status_code == 200
                    if history.json()["events"]:
                        assert history.json()["events"][0]["count"] == 1
                        break
                    await asyncio.sleep(0.1)
            ws_url = api_url.replace("http://", "ws://") + f"/ws/tail?service={service}&start=begin"
            async with connect(ws_url, additional_headers=owner) as websocket:
                import json
                frame = json.loads(await asyncio.wait_for(websocket.recv(), 10))
                assert frame["log"]["message"] == "own message"
                response = await auth.post(f"{auth_url}/api/v1/auth/logout", headers=owner)
                assert response.status_code == 204
                assert (await client.get("/api/v1/alerts", headers=owner)).status_code == 401
                try:
                    await asyncio.wait_for(websocket.recv(), 10)
                except ConnectionClosed as error:
                    assert error.rcvd.code == 1008
                else:
                    raise AssertionError("Revoked WebSocket remained authorized")
            print("PASS: real AuthFortress login/tenant authorization, tenant-scoped ingest/query/rules/history/WS and logout revocation")
        finally:
            await auth.post(f"{auth_url}/api/v1/auth/logout", headers=foreign)


if __name__ == "__main__":
    asyncio.run(verify())
