from __future__ import annotations

from uuid import UUID

import httpx
from fastapi import HTTPException, Request
from starlette.requests import HTTPConnection

from app.config import settings


async def authorize(connection: HTTPConnection, permission: str) -> str:
    if not settings.PLATFORM_AUTH_ENABLED:
        return ""
    bearer = connection.headers.get("authorization", "")
    if not bearer.startswith("Bearer ") or not bearer[7:].strip():
        raise HTTPException(401, "Bearer authentication required", headers={"WWW-Authenticate": "Bearer"})
    try:
        tenant_id = str(UUID(connection.headers.get("x-tenant-id", "")))
    except ValueError:
        raise HTTPException(422, "X-Tenant-ID must be a UUID") from None
    if not settings.AUTHFORTRESS_URL:
        raise HTTPException(503, "Identity service unavailable")
    try:
        async with httpx.AsyncClient(
            timeout=settings.AUTHFORTRESS_TIMEOUT_SECONDS, trust_env=False, follow_redirects=False,
        ) as client:
            response = await client.post(
                f"{settings.AUTHFORTRESS_URL.rstrip('/')}/api/v1/tenants/{tenant_id}/authorize",
                headers={"Authorization": bearer}, json={"permission": permission},
            )
        if response.status_code in (401, 403, 404):
            raise HTTPException(response.status_code, "Tenant authorization denied")
        if response.status_code != 200:
            raise ValueError("Unexpected identity response")
        context = response.json()
        if (context.get("allowed") is not True or context.get("tenant_id") != tenant_id
                or context.get("permission") != permission or context.get("role") not in ("owner", "member")):
            raise ValueError("Invalid identity context")
        if permission == "tenant.manage" and context["role"] != "owner":
            raise ValueError("Invalid write authorization")
        UUID(context["user_id"])
    except (httpx.HTTPError, httpx.InvalidURL, ValueError, TypeError, KeyError, AttributeError):
        raise HTTPException(503, "Identity service unavailable") from None
    return tenant_id


async def read_tenant(request: Request) -> str:
    return await authorize(request, "tenant.read")


async def manage_tenant(request: Request) -> str:
    return await authorize(request, "tenant.manage")
