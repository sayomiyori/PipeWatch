from __future__ import annotations

import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from app.config import settings
from app.models.log_entry import LogLevel
from app.services.callback import resolve_target
from app.services.identity import manage_tenant, read_tenant

router = APIRouter(prefix="/api/v1", tags=["alerts"])


class AlertRuleCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=256)
    service_filter: str | None = Field(default=None, max_length=256)
    level_filter: LogLevel | None = None
    condition: Literal["count_gt"] = "count_gt"
    threshold: int = Field(default=1, ge=1, le=1000000)
    window_minutes: int = Field(default=1, ge=1, le=43200)
    notification_channel: Literal["webhook", "telegram"] | None = None
    notification_target: str | None = Field(default=None, min_length=1, max_length=2048)
    cooldown_minutes: int = Field(default=5, ge=1, le=43200)

    @model_validator(mode="after")
    def validate_notification(self):
        if bool(self.notification_channel) != bool(self.notification_target):
            raise ValueError("Notification channel and target must be supplied together")
        if self.notification_channel == "telegram" and (settings.PLATFORM_AUTH_ENABLED or not settings.TELEGRAM_NOTIFICATIONS_ENABLED):
            raise ValueError("Telegram notifications are disabled")
        return self


class AlertRuleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = None
    service_filter: str | None = None
    level_filter: LogLevel | None = None
    condition: str | None = None
    threshold: int | None = None
    window_minutes: int | None = None
    notification_channel: str | None = None
    notification_target: str | None = None
    cooldown_minutes: int | None = None


async def _validate_target(rule: AlertRuleCreate) -> None:
    if rule.notification_channel == "webhook":
        try:
            await resolve_target(rule.notification_target)
        except (ValueError, OSError, TimeoutError):
            raise HTTPException(422, "Invalid or forbidden callback destination") from None


@router.get("/alerts/events")
async def list_events(request: Request, tenant_id: str = Depends(read_tenant)) -> dict[str, Any]:
    events = await request.app.state.store.events(tenant_id)
    return {"count": len(events), "events": events}


@router.get("/alerts/{rule_id}/history")
async def get_alert_history(rule_id: str, request: Request, tenant_id: str = Depends(read_tenant)) -> dict[str, Any]:
    await request.app.state.store.rule(tenant_id, rule_id)
    events = await request.app.state.store.events(tenant_id)
    history = [event for event in events if event["rule_id"] == rule_id][:200]
    return {"rule_id": rule_id, "count": len(history), "events": history}


@router.post("/alerts", status_code=201)
async def create_alert(payload: AlertRuleCreate, request: Request, tenant_id: str = Depends(manage_tenant)) -> dict[str, Any]:
    await _validate_target(payload)
    rule_id = str(uuid.uuid4())
    rule = payload.model_dump()
    await request.app.state.store.save_rule(tenant_id, rule_id, rule)
    return {"status": "created", "rule_id": rule_id, "rule": rule}


@router.get("/alerts")
async def list_alerts(request: Request, tenant_id: str = Depends(read_tenant)) -> dict[str, Any]:
    rules = await request.app.state.store.rules(tenant_id)
    return {"count": len(rules), "rules": rules}


@router.get("/alerts/{rule_id}")
async def get_alert(rule_id: str, request: Request, tenant_id: str = Depends(read_tenant)) -> dict[str, Any]:
    return {"rule_id": rule_id, "rule": await request.app.state.store.rule(tenant_id, rule_id)}


@router.patch("/alerts/{rule_id}")
async def patch_alert(rule_id: str, payload: AlertRuleUpdate, request: Request, tenant_id: str = Depends(manage_tenant)) -> dict[str, Any]:
    previous = await request.app.state.store.rule(tenant_id, rule_id)
    try:
        rule = AlertRuleCreate.model_validate({**previous, **payload.model_dump(exclude_unset=True)})
    except ValidationError:
        raise HTTPException(422, "Invalid alert rule update") from None
    await _validate_target(rule)
    updated = rule.model_dump()
    await request.app.state.store.save_rule(tenant_id, rule_id, updated, previous)
    return {"status": "updated", "rule_id": rule_id, "rule": updated}


@router.delete("/alerts/{rule_id}")
async def delete_alert(rule_id: str, request: Request, tenant_id: str = Depends(manage_tenant)) -> dict[str, Any]:
    deleted = await request.app.state.store.delete_rule(tenant_id, rule_id)
    return {"status": "deleted", "rule_id": rule_id, "rule": deleted}
