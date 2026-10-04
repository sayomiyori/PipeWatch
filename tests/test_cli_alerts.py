from app.api.v1.alerts import router
from cli.pipewatch import cli
from click.testing import CliRunner
from fastapi import FastAPI
from fastapi.testclient import TestClient


def test_cli_alert_create_preserves_requested_filters_and_threshold(monkeypatch):
    app = FastAPI()
    app.include_router(router)
    app.state.alert_rules = {}
    with TestClient(app) as client:
        monkeypatch.setattr("cli.pipewatch.httpx.post", client.post)
        result = CliRunner().invoke(cli, [
            "alerts", "create", "--name", "error-budget", "--service", "billing",
            "--min-level", "error", "--window-seconds", "120", "--threshold-count", "3",
        ])
        assert result.exit_code == 0, result.output
        rule = next(iter(app.state.alert_rules.values()))
        assert rule["service_filter"] == "billing"
        assert rule["level_filter"] == "error"
        assert rule["window_minutes"] == 2
        assert rule["threshold"] == 3


def test_cli_alert_rejects_seconds_not_representable_as_minutes(monkeypatch):
    app = FastAPI()
    app.include_router(router)
    app.state.alert_rules = {}
    with TestClient(app) as client:
        monkeypatch.setattr("cli.pipewatch.httpx.post", client.post)
        result = CliRunner().invoke(cli, ["alerts", "create", "--name", "invalid", "--window-seconds", "30"])
        assert result.exit_code == 2
        assert not app.state.alert_rules
