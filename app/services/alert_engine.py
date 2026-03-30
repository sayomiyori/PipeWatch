from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.services.clickhouse import ClickHouseService


@dataclass(frozen=True)
class AlertRule:
    rule_id: str
    name: str
    service: str | None
    min_level: str | None
    window_seconds: int
    threshold_count: int
    callback_url: str | None


class AlertEngine:
    """
    Background rule evaluator.
    Rules are stored in app.state.alert_rules (in-memory) by the API layer.
    """

    def __init__(self, clickhouse: ClickHouseService) -> None:
        self._clickhouse = clickhouse

        # app.state.{alert_rules, alert_events} are managed in main.py.

    async def run_forever(self, app) -> None:
        interval = 5
        while True:
            await asyncio.sleep(interval)

            rules: dict[str, Any] = getattr(app.state, "alert_rules", {})
            if not rules:
                continue

            now = datetime.now(timezone.utc)
            events: list[dict[str, Any]] = getattr(app.state, "alert_events", [])

            for rule_id, rule_data in list(rules.items()):
                try:
                    window_seconds = int(rule_data.get("window_seconds", 60))
                    threshold_count = int(rule_data.get("threshold_count", 1))
                    service = rule_data.get("service") or None

                    min_level = rule_data.get("min_level") or None
                    # We'll evaluate only with min_level equality for now (simpler & deterministic).
                    # Can be extended to >= comparisons.
                    from_ts = now - timedelta(seconds=window_seconds)

                    # Query logs count by trace isn't meaningful here; just count with filters.
                    # We reuse query_stats-like logic by filtering with `level` if provided.
                    level = min_level

                    # ClickHouseService doesn't expose a direct count endpoint for level+service+time,
                    # so we use query_stats with limit=1 and fallback to 0.
                    if min_level:
                        stats = self._clickhouse.query_stats(
                            group_by="level",
                            service=service,
                            level=level,
                            from_ts=from_ts,
                            to_ts=now,
                            limit=1,
                        )
                        # stats contains k=level, cnt=count
                        # If min_level is set, we just take cnt if level matches.
                        cnt = int(stats[0]["cnt"]) if stats else 0
                    else:
                        # When no min_level, we count all logs for service in the window via query_stats by service.
                        stats = self._clickhouse.query_stats(
                            group_by="service",
                            service=service,
                            level=None,
                            from_ts=from_ts,
                            to_ts=now,
                            limit=1,
                        )
                        cnt = int(stats[0]["cnt"]) if stats else 0

                    if cnt >= threshold_count:
                        event = {
                            "rule_id": rule_id,
                            "rule_name": rule_data.get("name"),
                            "ts": now.isoformat(),
                            "count": cnt,
                        }
                        events.append(event)

                        callback_url = rule_data.get("callback_url")
                        if callback_url:
                            # Fire-and-forget callback.
                            asyncio.create_task(
                                self._post_callback(callback_url, event)
                            )
                except Exception:
                    # Avoid breaking the loop due to a single rule.
                    continue

    async def _post_callback(self, callback_url: str, payload: dict[str, Any]) -> None:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(callback_url, json=payload)
        except Exception:
            pass

