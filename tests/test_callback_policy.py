import asyncio
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from app.api.v1.alerts import AlertRuleCreate
from app.config import settings
from app.services.alert_engine import AlertEngine
from app.services.callback import parse_target, post_callback, resolve_target


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["http://127.0.0.1/", "http://10.0.0.1/", "http://169.254.169.254/latest", "http://[::1]/", "http://[::ffff:127.0.0.1]/", "http://[64:ff9b::a00:1]/", "http://[64:ff9b::e000:1]/", "http://[2002:a00:1::1]/", "http://224.0.0.1/", "http://0.0.0.0/"])
async def test_private_metadata_and_multicast_addresses_are_rejected(target):
    with pytest.raises(ValueError):
        await resolve_target(target)


@pytest.mark.parametrize("target", ["file:///etc/passwd", "http://user:secret@example.com/", "http://example.com/#fragment", "http://example.com/#", "http://example.com/ space", "http://example.com:99999/"])
def test_malformed_callback_urls_are_rejected(target):
    with pytest.raises(ValueError):
        parse_target(target)


@pytest.mark.asyncio
async def test_all_dns_answers_validated_and_ip_pinned_with_tls_hostname(monkeypatch):
    loop = asyncio.get_running_loop()
    answers = ["93.184.216.34", "127.0.0.1"]
    async def lookup(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443)) for address in answers]
    monkeypatch.setattr(loop, "getaddrinfo", lookup)
    with pytest.raises(ValueError):
        await resolve_target("https://callback.example/path")
    answers[:] = ["93.184.216.34"]
    original = httpx.AsyncClient
    requests = []
    def capture(request):
        requests.append(request)
        # DNS changes after validation must not change the socket destination.
        answers[:] = ["127.0.0.1"]
        return httpx.Response(204)
    def client_factory(**kwargs):
        assert kwargs["trust_env"] is False
        assert kwargs["follow_redirects"] is False
        return original(transport=httpx.MockTransport(capture), **kwargs)
    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    await post_callback("https://callback.example/path", {"event_id": "test"})
    assert requests[0].url.host == "93.184.216.34"
    assert requests[0].headers["host"] == "callback.example"
    assert requests[0].extensions["sni_hostname"] == "callback.example"
    with pytest.raises(ValueError):
        await post_callback("https://callback.example/path", {})
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 400, 429, 500])
async def test_redirects_and_http_failures_are_not_reported_as_success(monkeypatch, status):
    monkeypatch.setattr(settings, "CALLBACK_TRUSTED_ORIGINS", "http://127.0.0.1:18888")
    original = httpx.AsyncClient
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(status, headers={"Location": "http://169.254.169.254/"})
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(transport=httpx.MockTransport(respond), **kwargs))
    with pytest.raises(ValueError):
        await post_callback("http://127.0.0.1:18888/callback", {})
    assert len(requests) == 1


@pytest.mark.parametrize("origin", ["http://localhost:8000", "http://169.254.169.254:8000", "http://0.0.0.0:8000", "http://224.0.0.1:8000"])
def test_trusted_exceptions_cannot_allow_dns_metadata_or_nonunicast(monkeypatch, origin):
    monkeypatch.setattr(settings, "CALLBACK_TRUSTED_ORIGINS", origin)
    with pytest.raises(ValueError):
        parse_target(origin + "/callback")


@pytest.mark.asyncio
async def test_failed_delivery_history_and_cooldown_are_durable(monkeypatch, redis_state):
    monkeypatch.setattr(settings, "CALLBACK_TRUSTED_ORIGINS", "http://127.0.0.1:18888")
    rule = AlertRuleCreate(name="failure", notification_channel="webhook", notification_target="http://127.0.0.1:18888/test").model_dump()
    rule_id = str(uuid4())
    await redis_state.save_rule("", rule_id, rule)
    requests = []
    original = httpx.AsyncClient
    def fail(request):
        requests.append(request)
        return httpx.Response(500)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(transport=httpx.MockTransport(fail), **kwargs))
    class Database:
        def count_logs(self, **kwargs):
            assert kwargs["tenant_id"] == ""
            return 2
    app = SimpleNamespace(state=SimpleNamespace(store=redis_state))
    engine = AlertEngine(Database())
    await engine.evaluate_once(app)
    events = await redis_state.events("")
    assert events[0]["delivery_status"] == "failed"
    assert events[0]["rule_id"] == rule_id
    await AlertEngine(Database()).evaluate_once(app)
    assert len(requests) == 1
    assert len(await redis_state.events("")) == 1


@pytest.mark.asyncio
async def test_real_loopback_callback_and_redirect_boundary(monkeypatch):
    paths = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            paths.append(self.path)
            self.send_response(204 if self.path == "/success" else 302)
            if self.path != "/success":
                self.send_header("Location", "/unexpected-redirect")
            self.end_headers()
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setattr(settings, "CALLBACK_TRUSTED_ORIGINS", origin)
    try:
        await post_callback(f"{origin}/success", {"event_id": "local-only"})
        with pytest.raises(ValueError):
            await post_callback(f"{origin}/redirect", {})
        assert paths == ["/success", "/redirect"]
        monkeypatch.setattr(settings, "CALLBACK_TRUSTED_ORIGINS", "")
        with pytest.raises(ValueError):
            await post_callback(f"{origin}/success", {})
        assert paths == ["/success", "/redirect"]
    finally:
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        thread.join(timeout=3)
