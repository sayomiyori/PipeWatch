from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx

from app.config import settings
from app.middleware.metrics import ALERT_EVALUATIONS, ALERTS_FIRED
from app.services.callback import post_callback
from app.services.clickhouse import ClickHouseService

logger = logging.getLogger(__name__)


async def _send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        )
        response.raise_for_status()


class AlertEngine:
    def __init__(self, clickhouse: ClickHouseService) -> None:
        self._clickhouse = clickhouse

    async def evaluate_once(self, app) -> None:
        store = app.state.store
        scopes = await store.redis.smembers(f"{store.prefix}:alerts:scopes")
        for scope in scopes:
            tenant_id = "" if scope == "standalone" else scope
            if settings.PLATFORM_AUTH_ENABLED and not tenant_id:
                continue
            if not settings.PLATFORM_AUTH_ENABLED and tenant_id:
                continue
            for rule_id, rule in (await store.rules(tenant_id)).items():
                try:
                    now = datetime.now(timezone.utc)
                    count = await asyncio.to_thread(
                        self._clickhouse.count_logs, service=rule.get("service_filter"),
                        min_level_gte=rule.get("level_filter"),
                        from_ts=now - timedelta(minutes=rule["window_minutes"]), to_ts=now,
                        tenant_id=tenant_id,
                    )
                    ALERT_EVALUATIONS.inc()
                    if count < rule["threshold"]:
                        continue
                    event = {
                        "event_id": str(uuid4()), "rule_id": rule_id, "rule_name": rule["name"],
                        "ts": now.isoformat(), "count": count, "threshold": rule["threshold"],
                        "window_seconds": rule["window_minutes"] * 60,
                        "service": rule.get("service_filter"), "min_level": rule.get("level_filter"),
                        "delivery_status": "pending" if rule.get("notification_channel") else "not_requested",
                    }
                    if not await store.begin_event(tenant_id, rule_id, event, rule["cooldown_minutes"] * 60, rule):
                        continue
                    ALERTS_FIRED.inc()
                    channel, target = rule.get("notification_channel"), rule.get("notification_target")
                    if not channel:
                        continue
                    try:
                        if channel == "webhook":
                            await post_callback(target, event)
                        elif channel == "telegram" and settings.TELEGRAM_NOTIFICATIONS_ENABLED and not settings.PLATFORM_AUTH_ENABLED and settings.TELEGRAM_BOT_TOKEN:
                            await _send_telegram(settings.TELEGRAM_BOT_TOKEN, target, f"PipeWatch: {rule['name']}; count={count}")
                        else:
                            raise ValueError("Notification channel disabled")
                    except Exception:
                        event["delivery_status"] = "failed"
                        # Do not expose callback URLs, HTTP exceptions or Telegram tokens.
                        logger.warning("Alert notification failed")
                    else:
                        event["delivery_status"] = "delivered"
                    await store.finish_event(tenant_id, event)
                except Exception:
                    logger.warning("Alert evaluation failed; durable rule retained")

    async def run_forever(self, app) -> None:
        while True:
            await asyncio.sleep(max(5, settings.ALERT_EVAL_INTERVAL_SECONDS))
            try:
                await self.evaluate_once(app)
            except Exception:
                logger.warning("Alert state unavailable; evaluation paused")
