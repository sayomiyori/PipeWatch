import asyncio
from types import SimpleNamespace

import httpx
from cli.pipewatch import _tail_async, cli
from click.testing import CliRunner


def test_cli_forwards_identity_headers_without_printing_token(monkeypatch):
    monkeypatch.setenv("PIPEWATCH_TOKEN", "local-test-only")
    monkeypatch.setenv("PIPEWATCH_TENANT_ID", "tenant-test")
    requests = []
    def get(url, **kwargs):
        requests.append(kwargs)
        return httpx.Response(200, json={"rules": {}}, request=httpx.Request("GET", url))
    monkeypatch.setattr("cli.pipewatch.httpx.get", get)
    result = CliRunner().invoke(cli, ["alerts", "list"])
    assert result.exit_code == 0
    assert requests[0]["headers"] == {"Authorization": "Bearer local-test-only", "X-Tenant-ID": "tenant-test"}
    assert "local-test-only" not in result.output


def test_cli_websocket_forwards_headers_and_encodes_service(monkeypatch):
    monkeypatch.setenv("PIPEWATCH_TOKEN", "local-test-only")
    monkeypatch.setenv("PIPEWATCH_TENANT_ID", "tenant-test")
    requests = []
    class Socket:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        def __aiter__(self):
            return self
        async def __anext__(self):
            raise StopAsyncIteration
    def connect(url, additional_headers=None):
        requests.append(SimpleNamespace(url=url, headers=additional_headers))
        return Socket()
    monkeypatch.setattr("cli.pipewatch.websockets.connect", connect)
    asyncio.run(_tail_async("http://localhost", "svc&start=begin", None, "latest"))
    assert "svc%26start%3Dbegin" in requests[0].url
    assert requests[0].headers["Authorization"] == "Bearer local-test-only"
