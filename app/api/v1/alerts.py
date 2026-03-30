from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.models.log_entry import LogLevel


router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


class AlertRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)
    service: str | None = None
    min_level: LogLevel | None = None
    window_seconds: int = 60
    threshold_count: int = 1
    callback_url: str | None = None


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    service: str | None = None
    min_level: LogLevel | None = None
    window_seconds: int | None = None
    threshold_count: int | None = None
    callback_url: str | None = None


@router.post("/rules")
async def create_rule(payload: AlertRuleCreate, request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules

    rule_id = str(uuid.uuid4())
    rules[rule_id] = payload.model_dump()
    return {"status": "created", "rule_id": rule_id, "rule": rules[rule_id]}


@router.get("/rules")
async def list_rules(request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    return {"count": len(rules), "rules": rules}


@router.get("/rules/{rule_id}")
async def get_rule(rule_id: str, request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        return {"error": "not_found", "rule_id": rule_id}
    return {"rule_id": rule_id, "rule": rules[rule_id]}


@router.put("/rules/{rule_id}")
async def update_rule(
    rule_id: str, payload: AlertRuleUpdate, request: Request
) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        return {"error": "not_found", "rule_id": rule_id}

    update_data = payload.model_dump(exclude_unset=True)
    rules[rule_id].update(update_data)
    return {"status": "updated", "rule_id": rule_id, "rule": rules[rule_id]}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str, request: Request) -> dict[str, Any]:
    rules: dict[str, Any] = request.app.state.alert_rules
    if rule_id not in rules:
        return {"error": "not_found", "rule_id": rule_id}
    deleted = rules.pop(rule_id)
    return {"status": "deleted", "rule_id": rule_id, "rule": deleted}


@router.get("/events")
async def list_events(request: Request) -> dict[str, Any]:
    events: list[dict[str, Any]] = getattr(request.app.state, "alert_events", [])
    return {"count": len(events), "events": events[-200:]}

