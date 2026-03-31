from __future__ import annotations

import time
from typing import Callable

from fastapi import Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import PlainTextResponse

from app.config import settings

REQUEST_COUNT = Counter(
    "pipewatch_request_total",
    "Total requests",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "pipewatch_request_latency_seconds",
    "Request latency in seconds",
    ["method", "path"],
)

LOGS_ACCEPTED = Counter(
    "pipewatch_logs_accepted_total",
    "Total accepted logs via ingest endpoints",
    ["endpoint", "service"],
)

ALERT_EVALUATIONS = Counter(
    "pipewatch_alert_evaluations_total",
    "Alert rule evaluations (one increment per rule per cycle)",
)
ALERTS_FIRED = Counter(
    "pipewatch_alerts_fired_total",
    "Times an alert fired (threshold met after cooldown)",
)


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Response]
    ) -> Response:
        if not settings.METRICS_ENABLED:
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - start

        path = request.url.path
        REQUEST_COUNT.labels(
            method=request.method,
            path=path,
            status=str(response.status_code),
        ).inc()
        REQUEST_LATENCY.labels(method=request.method, path=path).observe(elapsed)
        return response


def register_metrics_route(app) -> None:
    if not settings.METRICS_ENABLED:
        return

    @app.get("/metrics")
    def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
