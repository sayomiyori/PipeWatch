from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import settings
from app.middleware.metrics import ALERT_EVALUATIONS, ALERTS_FIRED
from app.models.log_entry import LogLevel
from app.services.clickhouse import ClickHouseService


def _chat_id_for_rule(rule_data: dict[str, Any]) -> str | None:
    rid = rule_data.get("notification_target") or settings.TELEGRAM_CHAT_ID
    return rid.strip() if rid else None


async def _send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
        )
        r.raise_for_status()


class AlertEngine:
    """
    Periodic rule evaluator (default ALERT_EVAL_INTERVAL_SECONDS).
    Uses ClickHouse COUNT with optional min_level (>=) and service filter.
    Sends HTTP callback and/or Telegram when threshold is met (with cooldown).
    """

    def __init__(self, clickhouse: ClickHouseService) -> None:
        self._clickhouse = clickhouse

    async def run_forever(self, app) -> None:
        interval = max(5.0, float(settings.ALERT_EVAL_INTERVAL_SECONDS))

        while True:
            await asyncio.sleep(interval)

            rules: dict[str, Any] = getattr(app.state, "alert_rules", {})
            if not rules:
                continue

            if not hasattr(app.state, "alert_last_fire"):
                app.state.alert_last_fire = {}

            last_fire: dict[str, datetime] = app.state.alert_last_fire
            now = datetime.now(timezone.utc)
            events: list[dict[str, Any]] = getattr(app.state, "alert_events", [])

            for rule_id, rule_data in list(rules.items()):
                try:
                    window_minutes = int(rule_data.get("window_minutes", 1))
                    window_seconds = window_minutes * 60
                    threshold_count = int(rule_data.get("threshold", 1))
                    service = rule_data.get("service_filter") or None
                    min_level_raw = rule_data.get("level_filter")
                    min_level: LogLevel | None = (
                        min_level_raw if isinstance(min_level_raw, str) else None
                    )

                    from_ts = now - timedelta(seconds=window_seconds)

                    cnt = await asyncio.to_thread(
                        self._clickhouse.count_logs,
                        service,
                        None,
                        None,
                        from_ts,
                        now,
                        None,
                        min_level,
                    )
                    ALERT_EVALUATIONS.inc()

                    if cnt < threshold_count:
                        continue

                    rule_cooldown = float(rule_data.get("cooldown_minutes", settings.ALERT_COOLDOWN_SECONDS / 60.0)) * 60.0
                    # Cooldown: avoid spamming Telegram/callback every tick.
                    prev = last_fire.get(rule_id)
                    if prev is not None and rule_cooldown > 0:
                        if (now - prev).total_seconds() < rule_cooldown:
                            continue

                    event = {
                        "rule_id": rule_id,
                        "rule_name": rule_data.get("name"),
                        "ts": now.isoformat(),
                        "count": cnt,
                        "threshold": threshold_count,
                        "window_seconds": window_seconds,
                        "service": service,
                        "min_level": min_level,
                    }
                    events.append(event)
                    last_fire[rule_id] = now
                    ALERTS_FIRED.inc()

                    channel = rule_data.get("notification_channel")
                    target = rule_data.get("notification_target")
                    if channel == "webhook" and target:
                        asyncio.create_task(self._post_callback(target, event))

                    token = settings.TELEGRAM_BOT_TOKEN.strip()
                    if channel == "telegram" and target:
                        chat_id = target.strip()
                        if token and chat_id:
                            text = (
                                f"🚨 PipeWatch: {rule_data.get('name', rule_id)}\n"
                                f"Count: {cnt} (threshold {threshold_count})\n"
                                f"Window: {window_seconds}s\n"
                                f"Service: {service or 'any'}\n"
                                f"Min level: {min_level or 'any'}"
                            )
                            asyncio.create_task(self._send_telegram_safe(token, chat_id, text))
                except Exception:
                    continue

    async def _post_callback(self, callback_url: str, payload: dict[str, Any]) -> None:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(callback_url, json=payload)
        except Exception:
            pass

    async def _send_telegram_safe(
        self, token: str, chat_id: str, text: str
    ) -> None:
        try:
            await _send_telegram(token, chat_id, text)
        except Exception:
            pass
