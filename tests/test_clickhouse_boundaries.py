import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from uuid import uuid4

import pytest
from app.services.clickhouse import ClickHouseService


@pytest.fixture
def clickhouse():
    if not os.environ.get("CLICKHOUSE_DATABASE", "").endswith("_test"):
        pytest.skip("Set CLICKHOUSE_DATABASE to an isolated database ending in _test")
    service = ClickHouseService()
    service.ensure_tables()
    return service


def test_backslash_quote_filter_cannot_change_query_predicate(clickhouse):
    service = f"boundary-{uuid4().hex}"
    clickhouse.insert_logs([{
        "timestamp": datetime.now(timezone.utc), "service": service,
        "level": 2, "message": "private marker",
    }])
    assert clickhouse.count_logs(service=service) == 1
    assert clickhouse.count_logs(service="missing\\' OR 1=1 -- ") == 0


def test_timezone_filter_uses_utc(clickhouse):
    from datetime import timedelta

    service = f"timezone-{uuid4().hex}"
    timestamp = datetime.now(timezone.utc)
    clickhouse.insert_logs([{
        "timestamp": timestamp, "service": service, "level": 2, "message": "UTC marker",
    }])
    assert clickhouse.count_logs(
        service=service, from_ts=timestamp - timedelta(seconds=1), to_ts=timestamp + timedelta(seconds=1)
    ) == 1


def test_shared_client_allows_concurrent_queries(clickhouse):
    barrier = Barrier(2)

    def query():
        barrier.wait(timeout=5)
        return clickhouse._client.query("SELECT sleep(0.2), 1").result_rows[0][1]

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(query) for _ in range(2)]
        assert [future.result(timeout=10) for future in futures] == [1, 1]
