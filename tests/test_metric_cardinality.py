from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from app.api.v1 import ingest
from app.config import settings
from app.models.log_entry import LogEntryCreate
from prometheus_client import CollectorRegistry, Counter


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,platform,series", [(False, True, 0), (True, True, 0), (False, False, 0), (True, False, 1)])
async def test_arbitrary_service_names_do_not_create_unbounded_metric_series(monkeypatch, enabled, platform, series):
    metric = Counter("accepted_test", "Accepted logs", ["endpoint", "service"], registry=CollectorRegistry())
    monkeypatch.setattr(ingest, "LOGS_ACCEPTED", metric)
    monkeypatch.setattr(settings, "PLATFORM_AUTH_ENABLED", platform)
    monkeypatch.setattr(settings, "METRICS_ENABLED", enabled)
    store = AsyncMock()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(store=store)))
    logs = [LogEntryCreate(service=f"service-{number}", level="info", message="controlled") for number in range(1000)]
    assert (await ingest._enqueue(logs, request, "tenant", "logs/batch"))["count"] == 1000
    assert len(metric._metrics) == series
    if series:
        assert sum(sample.value for sample in metric.collect()[0].samples if sample.name == "accepted_test_total") == 1000
    store.enqueue.assert_awaited_once()
    assert len(store.enqueue.call_args.args[0]) == 1000
