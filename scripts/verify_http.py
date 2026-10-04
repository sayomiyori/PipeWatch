"""Verify the isolated built PipeWatch service over HTTP, WebSocket and CLI."""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus


async def run_cli(*args: str) -> str:
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "cli.pipewatch", "--base-url", "http://127.0.0.1:38084", *args,
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "COLUMNS": "200", "PYTHONIOENCODING": "utf-8"},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
    assert process.returncode == 0, stderr.decode(errors="replace")
    return stdout.decode(errors="replace")


async def verify() -> None:
    service = f"verify-{uuid4().hex[:8]}"
    traces = [uuid4().hex for _ in range(3)]
    async with httpx.AsyncClient(base_url="http://127.0.0.1:38084", timeout=10) as client:
        metrics = await client.get("/metrics")
        assert metrics.status_code == 200 and "pipewatch_" in metrics.text
        logs = [{
            "service": service, "level": "info" if index == 0 else "error",
            "message": f"verification-{index}", "trace_id": traces[index],
        } for index in range(3)]
        response = await client.post("/api/v1/logs/batch", json={"logs": logs})
        assert response.status_code == 200 and response.json()["count"] == 3
        assert (await client.post("/api/v1/logs", json={"service": service, "level": "invalid"})).status_code == 422
        deadline = time.monotonic() + 15
        while True:
            response = await client.get("/api/v1/logs", params={"service": service})
            assert response.status_code == 200
            if response.json()["total"] == 3:
                assert {row["message"] for row in response.json()["items"]} == {
                    f"verification-{index}" for index in range(3)
                }
                break
            assert time.monotonic() < deadline, "Accepted batch was not stored"
            await asyncio.sleep(0.1)
        filtered = await client.get("/api/v1/logs", params={"service": service, "trace_id": traces[0]})
        assert filtered.json()["total"] == 1
        injection = await client.get("/api/v1/logs", params={"service": "missing\\' OR 1=1 -- "})
        assert injection.status_code == 200 and injection.json()["total"] == 0
        assert (await client.get("/api/v1/logs", params={"size": 501})).status_code == 422
        stats = await client.get("/api/v1/logs/stats")
        assert stats.status_code == 200 and stats.json()["count_by_level"]["error"] >= 2
        services = await client.get("/api/v1/logs/services")
        assert any(row["service"] == service for row in services.json()["services"])
        async with connect(f"ws://127.0.0.1:38084/ws/tail?service={service}&start=begin&level=error") as socket:
            async with asyncio.timeout(10):
                messages = [json.loads(await socket.recv()) for _ in range(2)]
            assert {message["log"]["trace_id"] for message in messages} == set(traces[1:])
            assert all(message["log"]["level"] == "error" for message in messages)
        try:
            async with connect("ws://127.0.0.1:38084/ws/tail"):
                raise AssertionError("Missing service WebSocket was accepted")
        except InvalidStatus as error:
            assert error.response.status_code == 403
        # Docker Desktop's VM clock can run ahead of the Windows CLI clock.
        recorded_at = datetime.fromisoformat(response.json()["items"][0]["timestamp"])
        if recorded_at.tzinfo is None:
            recorded_at = recorded_at.replace(tzinfo=timezone.utc)
        delay = (recorded_at - datetime.now(timezone.utc)).total_seconds()
        assert delay < 15, "Docker/host clock skew exceeds the verification allowance"
        if delay > 0:
            await asyncio.sleep(delay + 0.05)
        output = await run_cli("query", "--service", service, "--last", "1h", "--limit", "3")
        assert "Showing 3 of 3 total" in output and "INFO" in output and "ERROR" in output, output
        assert "Total logs" in await run_cli("stats", "--last", "1h")
        await run_cli("alerts", "create", "--name", service, "--service", service,
                      "--min-level", "error", "--window-seconds", "120", "--threshold-count", "3")
        rules = (await client.get("/api/v1/alerts")).json()["rules"]
        matches = [(key, rule) for key, rule in rules.items() if rule["name"] == service]
        assert len(matches) == 1
        rule_id, rule = matches[0]
        assert rule["service_filter"] == service and rule["level_filter"] == "error"
        assert rule["window_minutes"] == 2 and rule["threshold"] == 3
        assert service in await run_cli("alerts", "list")
        assert (await client.patch(f"/api/v1/alerts/{rule_id}", json={"threshold": 4})).status_code == 200
        assert (await client.get(f"/api/v1/alerts/{rule_id}")).json()["rule"]["threshold"] == 4
        await run_cli("alerts", "delete", rule_id)
        assert (await client.get(f"/api/v1/alerts/{rule_id}")).status_code == 404
    print("PASS: built HTTP ingest/query/stats/validation/SQL filtering, Redis WebSocket and CLI query/stats/alert CRUD")


if __name__ == "__main__":
    asyncio.run(verify())
