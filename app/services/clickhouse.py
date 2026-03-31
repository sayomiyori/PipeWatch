from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import clickhouse_connect

from app.config import settings
from app.models.log_entry import LEVEL_TO_INT, LogLevel


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _format_datetime64_3(ts: datetime) -> str:
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

    # ------------------------------------------------------------------ inserts

    def insert_logs(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        column_names = ["timestamp", "service", "level", "message",
                        "trace_id", "span_id", "metadata", "host"]
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
        self._client.insert(
            table=f"{settings.CLICKHOUSE_DATABASE}.logs",
            data=data,
            column_names=column_names,
        )

    # ------------------------------------------------------------------ helpers

    def _build_where(
        self,
        service: str | None = None,
        level: LogLevel | None = None,
        trace_id: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        q: str | None = None,
        min_level_gte: LogLevel | None = None,
    ) -> str:
        parts: list[str] = []
        if service:
            parts.append(f"service = '{_escape_sql_string(service)}'")
        if level:
            parts.append(f"level = {LEVEL_TO_INT[level]}")
        elif min_level_gte:
            parts.append(f"toUInt8(level) >= {LEVEL_TO_INT[min_level_gte]}")
        if trace_id:
            parts.append(f"trace_id = '{_escape_sql_string(trace_id)}'")
        if from_ts:
            parts.append(
                f"timestamp >= toDateTime64('{_format_datetime64_3(from_ts)}', 3)"
            )
        if to_ts:
            parts.append(
                f"timestamp <= toDateTime64('{_format_datetime64_3(to_ts)}', 3)"
            )
        if q:
            parts.append(
                f"positionCaseInsensitive(message, '{_escape_sql_string(q)}') > 0"
            )
        return " AND ".join(parts) if parts else "1"

    def _rows_to_dicts(self, result: Any) -> list[dict[str, Any]]:
        if hasattr(result, "result_rows") and result.result_rows is not None:
            col_order = ["timestamp", "service", "level", "message",
                         "trace_id", "span_id", "metadata", "host"]
            return [dict(zip(col_order, r)) for r in result.result_rows]
        raise RuntimeError("Unexpected clickhouse query result format")

    def _scalar(self, result: Any) -> int:
        if hasattr(result, "result_rows") and result.result_rows:
            return int(result.result_rows[0][0])
        if hasattr(result, "result_set") and result.result_set:
            return int(result.result_set[0][0])
        return 0

    # ------------------------------------------------------------------ queries

    def query_logs(
        self,
        service: str | None = None,
        level: LogLevel | None = None,
        trace_id: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        q: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where_sql = self._build_where(
            service, level, trace_id, from_ts, to_ts, q, None
        )
        limit = max(1, min(int(limit), 5000))
        offset = max(0, int(offset))
        db = settings.CLICKHOUSE_DATABASE
        query = f"""
SELECT timestamp, service, level, message, trace_id, span_id, metadata, host
FROM {db}.logs
WHERE {where_sql}
ORDER BY timestamp DESC
LIMIT {limit} OFFSET {offset}
"""
        return self._rows_to_dicts(self._client.query(query))

    def count_logs(
        self,
        service: str | None = None,
        level: LogLevel | None = None,
        trace_id: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        q: str | None = None,
        min_level_gte: LogLevel | None = None,
    ) -> int:
        where_sql = self._build_where(
            service, level, trace_id, from_ts, to_ts, q, min_level_gte
        )
        db = settings.CLICKHOUSE_DATABASE
        result = self._client.query(
            f"SELECT count() FROM {db}.logs WHERE {where_sql}"
        )
        return self._scalar(result)

    def query_full_stats(
        self,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
    ) -> dict[str, Any]:
        db = settings.CLICKHOUSE_DATABASE
        where = self._build_where(from_ts=from_ts, to_ts=to_ts)
        error_where = self._build_where(level="error", from_ts=from_ts, to_ts=to_ts)

        total_count = self._scalar(
            self._client.query(f"SELECT count() FROM {db}.logs WHERE {where}")
        )

        level_rows = self._client.query(f"""
SELECT level, count() AS cnt
FROM {db}.logs WHERE {where}
GROUP BY level ORDER BY cnt DESC
""").result_rows or []
        count_by_level = {str(r[0]): int(r[1]) for r in level_rows}

        svc_rows = self._client.query(f"""
SELECT service, count() AS cnt
FROM {db}.logs WHERE {where}
GROUP BY service ORDER BY cnt DESC LIMIT 20
""").result_rows or []
        count_by_service = {str(r[0]): int(r[1]) for r in svc_rows}

        hourly_rows = self._client.query(f"""
SELECT toStartOfHour(timestamp) AS hour, count() AS cnt
FROM {db}.logs WHERE {error_where}
GROUP BY hour ORDER BY hour ASC
""").result_rows or []
        error_rate_per_hour = [
            {"hour": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
             "count": int(r[1])}
            for r in hourly_rows
        ]

        top_error_rows = self._client.query(f"""
SELECT message, service, count() AS cnt
FROM {db}.logs WHERE {error_where}
GROUP BY message, service ORDER BY cnt DESC LIMIT 10
""").result_rows or []
        top_errors = [
            {"message": r[0], "service": r[1], "count": int(r[2])}
            for r in top_error_rows
        ]

        return {
            "total_count": total_count,
            "count_by_level": count_by_level,
            "count_by_service": count_by_service,
            "error_rate_per_hour": error_rate_per_hour,
            "top_errors": top_errors,
        }

    def query_services(self) -> list[dict[str, Any]]:
        db = settings.CLICKHOUSE_DATABASE
        result = self._client.query(f"""
SELECT service, max(timestamp) AS last_seen, count() AS total_logs
FROM {db}.logs
GROUP BY service ORDER BY last_seen DESC
""")
        rows = result.result_rows or []
        return [
            {
                "service": r[0],
                "last_seen": r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
                "total_logs": int(r[2]),
            }
            for r in rows
        ]

    def query_stats(
        self,
        group_by: str,
        service: str | None = None,
        level: LogLevel | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Kept for backward compat / alert engine."""
        where_sql = self._build_where(service, level, None, from_ts, to_ts)
        limit = max(1, min(int(limit), 5000))
        group_col = "service" if group_by == "service" else "level"
        db = settings.CLICKHOUSE_DATABASE
        result = self._client.query(f"""
SELECT {group_col} AS k, count() AS cnt
FROM {db}.logs WHERE {where_sql}
GROUP BY k ORDER BY cnt DESC LIMIT {limit}
""")
        rows = result.result_rows if hasattr(result, "result_rows") else []
        return [{"k": r[0], "cnt": r[1]} for r in rows]

    def count_by_trace_ids(self, trace_ids: list[str]) -> int:
        if not trace_ids:
            return 0
        in_list = ", ".join(f"'{_escape_sql_string(t)}'" for t in trace_ids)
        db = settings.CLICKHOUSE_DATABASE
        result = self._client.query(
            f"SELECT count() FROM {db}.logs WHERE trace_id IN ({in_list})"
        )
        return self._scalar(result)
