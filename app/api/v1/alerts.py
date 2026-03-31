from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.models.log_entry import LogLevel

router = APIRouter(prefix="/api/v1", tags=["alerts"])


class AlertRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)
    service_filter: str | None = None
    level_filter: LogLevel | None = None
    condition: str = "count_gt"
    threshold: int = 1
    window_minutes: int = 1
    notification_channel: str | None = None
    notification_target: str | None = None
    cooldown_minutes: int = 5


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    service_filter: str | None = None
    level_filter: LogLevel | None = None
    condition: str | None = None
    threshold: int | None = None
    window_minutes: int | None = None
    notification_channel: str | None = None
    notification_target: str | None = None
    cooldown_minutes: int | None = None


# Static paths first (before /alerts/{rule_id})
@router.get("/alerts/events")
async def list_events(request: Request) -> dict[str, Any]:
    events: list[dict[str, Any]] = getattr(request.app.state, "alert_events", [])
    return {"count": len(events), "events": events[-500:]}


@router.get("/alerts/{rule_id}/history")
async def get_alert_history(rule_id: str, request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="rule not found")

    events: list[dict[str, Any]] = getattr(request.app.state, "alert_events", [])
    history = [e for e in events if e.get("rule_id") == rule_id]
    return {"rule_id": rule_id, "count": len(history), "events": history[-200:]}


# Canonical CRUD /api/v1/alerts
@router.post("/alerts", status_code=status.HTTP_201_CREATED)
async def create_alert(payload: AlertRuleCreate, request: Request) -> dict[str, Any]:
    return await _create_alert_impl(payload, request)


@router.get("/alerts")
async def list_alerts(request: Request) -> dict[str, Any]:
    return await _list_alerts_impl(request)


@router.get("/alerts/{rule_id}")
async def get_alert(rule_id: str, request: Request) -> dict[str, Any]:
    return await _get_alert_impl(rule_id, request)


@router.patch("/alerts/{rule_id}")
async def patch_alert(
    rule_id: str, payload: AlertRuleUpdate, request: Request
) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="rule not found")
    update_data = payload.model_dump(exclude_unset=True)
    rules[rule_id].update(update_data)
    return {"status": "updated", "rule_id": rule_id, "rule": rules[rule_id]}


@router.delete("/alerts/{rule_id}")
async def delete_alert(rule_id: str, request: Request) -> dict[str, Any]:
    return await _delete_alert_impl(rule_id, request)


async def _create_alert_impl(
    payload: AlertRuleCreate, request: Request
) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    rule_id = str(uuid.uuid4())
    rules[rule_id] = payload.model_dump()
    return {"status": "created", "rule_id": rule_id, "rule": rules[rule_id]}


async def _list_alerts_impl(request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    return {"count": len(rules), "rules": rules}


async def _get_alert_impl(rule_id: str, request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="rule not found")
    return {"rule_id": rule_id, "rule": rules[rule_id]}


async def _delete_alert_impl(rule_id: str, request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="rule not found")
    deleted = rules.pop(rule_id)
    return {"status": "deleted", "rule_id": rule_id, "rule": deleted}
