from app.main import app
from cli.pipewatch import cli
from click.testing import CliRunner
from fastapi.testclient import TestClient


def test_cli_alert_create_preserves_requested_filters_and_threshold(monkeypatch, isolated_prefix):
    with TestClient(app) as client:
        monkeypatch.setattr("cli.pipewatch.httpx.post", client.post)
        result = CliRunner().invoke(cli, [
            "alerts", "create", "--name", "error-budget", "--service", "billing",
            "--min-level", "error", "--window-seconds", "120", "--threshold-count", "3",
        ])
        assert result.exit_code == 0, result.output
        rule = next(iter(client.get("/api/v1/alerts").json()["rules"].values()))
        assert rule["service_filter"] == "billing"
        assert rule["level_filter"] == "error"
        assert rule["window_minutes"] == 2
        assert rule["threshold"] == 3


def test_cli_alert_rejects_seconds_not_representable_as_minutes(monkeypatch, isolated_prefix):
    with TestClient(app) as client:
        monkeypatch.setattr("cli.pipewatch.httpx.post", client.post)
        result = CliRunner().invoke(cli, ["alerts", "create", "--name", "invalid", "--window-seconds", "30"])
        assert result.exit_code == 2
        assert not client.get("/api/v1/alerts").json()["rules"]
