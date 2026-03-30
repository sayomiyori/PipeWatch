from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import clickhouse_connect

from app.config import settings
from app.models.log_entry import LogLevel, INT_TO_LEVEL, LEVEL_TO_INT


def _escape_sql_string(value: str) -> str:
    # Minimal escaping for safe SQL string literals.
    # Since we only embed user-provided values inside quoted strings, escape ' -> ''.
    return value.replace("'", "''")

def _format_datetime64_3(ts: datetime) -> str:
    # ClickHouse accepts DateTime64(3) like: 'YYYY-MM-DD HH:MM:SS.mmm'
    # Avoid ISO timezone suffixes (e.g. +00:00) to reduce parsing issues.
    ts_utc = ts
    if ts.tzinfo is not None:
        ts_utc = ts.astimezone().replace(tzinfo=None)
    base = ts_utc.strftime("%Y-%m-%d %H:%M:%S.%f")
    return base[:-3]


@dataclass(frozen=True)
class ClickHouseQueries:
    create_database: str
    create_logs_table: str


class ClickHouseService:
    def __init__(self) -> None:
        self._client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD or None,
            database=settings.CLICKHOUSE_DATABASE,
        )

        self._queries = self._build_queries()

    def _build_queries(self) -> ClickHouseQueries:
        create_database = f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DATABASE}"
        # Use the schema provided by the user.
        create_logs_table = f"""
CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DATABASE}.logs (
    timestamp DateTime64(3),
    service String,
    level Enum8('debug'=1, 'info'=2, 'warn'=3, 'error'=4, 'fatal'=5),
    message String,
    trace_id String DEFAULT '',
    span_id String DEFAULT '',
    metadata String DEFAULT '{{}}',
    host String DEFAULT '',
    INDEX idx_service service TYPE set(100) GRANULARITY 1,
    INDEX idx_level level TYPE set(10) GRANULARITY 1,
    INDEX idx_trace trace_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (service, level, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
"""
        return ClickHouseQueries(
            create_database=create_database, create_logs_table=create_logs_table
        )

    def ensure_tables(self) -> None:
        self._client.command(self._queries.create_database)
        self._client.command(self._queries.create_logs_table)

    def insert_logs(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return

        column_names = [
            "timestamp",
            "service",
            "level",
            "message",
            "trace_id",
            "span_id",
            "metadata",
            "host",
        ]

        data = [
            (
                r["timestamp"],
                r["service"],
                int(r["level"]),
                r["message"],
                r.get("trace_id", ""),
                r.get("span_id", ""),
                r.get("metadata", "{}"),
                r.get("host", ""),
            )
            for r in rows
        ]
        # clickhouse-connect accepts a list of tuples.
        self._client.insert(
            table=f"{settings.CLICKHOUSE_DATABASE}.logs",
            data=data,
            column_names=column_names,
        )

    def _build_where(
        self,
        service: str | None = None,
        level: LogLevel | None = None,
        trace_id: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
    ) -> str:
        parts: list[str] = []
        if service:
            parts.append(f"service = '{_escape_sql_string(service)}'")
        if level:
            parts.append(f"level = {LEVEL_TO_INT[level]}")
        if trace_id:
            parts.append(f"trace_id = '{_escape_sql_string(trace_id)}'")
        if from_ts:
            # ClickHouse expects DateTime64(3). Use toDateTime64 with explicit precision.
            parts.append(
                f"timestamp >= toDateTime64('{_format_datetime64_3(from_ts)}', 3)"
            )
        if to_ts:
            parts.append(
                f"timestamp <= toDateTime64('{_format_datetime64_3(to_ts)}', 3)"
            )

        return " AND ".join(parts) if parts else "1"

    def query_logs(
        self,
        service: str | None = None,
        level: LogLevel | None = None,
        trace_id: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        where_sql = self._build_where(service, level, trace_id, from_ts, to_ts)
        limit = max(1, min(int(limit), 5000))

        query = f"""
SELECT
    timestamp,
    service,
    level,
    message,
    trace_id,
    span_id,
    metadata,
    host
FROM {settings.CLICKHOUSE_DATABASE}.logs
WHERE {where_sql}
ORDER BY timestamp DESC
LIMIT {limit}
"""
        result = self._client.query(query)
        if hasattr(result, "result_rows") and result.result_rows is not None:
            # Most clickhouse-connect versions expose result_rows as list[tuple].
            col_order = [
                "timestamp",
                "service",
                "level",
                "message",
                "trace_id",
                "span_id",
                "metadata",
                "host",
            ]
            return [dict(zip(col_order, r)) for r in result.result_rows]

        raise RuntimeError("Unexpected clickhouse query result format")

    def query_stats(
        self,
        group_by: str,
        service: str | None = None,
        level: LogLevel | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        where_sql = self._build_where(service, level, None, from_ts, to_ts)
        limit = max(1, min(int(limit), 5000))

        group_col = "service" if group_by == "service" else "level"
        query = f"""
SELECT
    {group_col} as k,
    count() as cnt
FROM {settings.CLICKHOUSE_DATABASE}.logs
WHERE {where_sql}
GROUP BY k
ORDER BY cnt DESC
LIMIT {limit}
"""
        result = self._client.query(query)
        # Map result similarly.
        if hasattr(result, "result_rows"):
            rows = result.result_rows
            return [{"k": r[0], "cnt": r[1]} for r in rows]
        return []

    def count_by_trace_ids(self, trace_ids: list[str]) -> int:
        if not trace_ids:
            return 0
        # Use IN with escaped strings.
        in_list = ", ".join(f"'{_escape_sql_string(t)}'" for t in trace_ids)
        query = f"""
SELECT count()
FROM {settings.CLICKHOUSE_DATABASE}.logs
WHERE trace_id IN ({in_list})
"""
        result = self._client.query(query)
        if hasattr(result, "result_rows") and result.result_rows:
            return int(result.result_rows[0][0])
        # Fallback: direct scalar.
        if hasattr(result, "result_set") and result.result_set:
            return int(result.result_set[0][0])
        raise RuntimeError("Unexpected clickhouse count result format")

