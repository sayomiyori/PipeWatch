from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any

import httpx
import websockets


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipewatch")
    p.add_argument(
        "--base-url",
        default=os.getenv("PIPEWATCH_BASE_URL", "http://localhost:8000"),
        help="Base URL for PipeWatch API",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    tail = sub.add_parser("tail", help="Live tail via WebSocket")
    tail.add_argument("--service", required=True)
    tail.add_argument("--start", choices=["latest", "begin"], default="latest")

    query = sub.add_parser("query", help="Query logs")
    query.add_argument("--service")
    query.add_argument("--level", choices=["debug", "info", "warn", "error", "fatal"])
    query.add_argument("--trace-id")
    query.add_argument("--limit", type=int, default=50)

    stats = sub.add_parser("stats", help="Query logs stats")
    stats.add_argument("--group-by", choices=["service", "level"], default="service")
    stats.add_argument("--service")
    stats.add_argument("--level", choices=["debug", "info", "warn", "error", "fatal"])
    stats.add_argument("--limit", type=int, default=50)

    alerts = sub.add_parser("alerts", help="Manage alert rules")
    alerts.add_argument("action", choices=["list", "create", "delete"])
    alerts.add_argument("--name")
    alerts.add_argument("--service")
    alerts.add_argument("--min-level", choices=["debug", "info", "warn", "error", "fatal"])
    alerts.add_argument("--window-seconds", type=int, default=60)
    alerts.add_argument("--threshold-count", type=int, default=1)
    alerts.add_argument("--callback-url")
    alerts.add_argument("--rule-id")

    return p


async def _ws_tail(base_url: str, service: str, start: str) -> None:
    ws_url = base_url.replace("http://", "ws://").replace("https://", "wss://")
    url = f"{ws_url}/ws/live-tail?service={service}&start={start}"
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            print(msg)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    if args.cmd == "tail":
        asyncio.run(_ws_tail(base_url, args.service, args.start))
        return

    if args.cmd == "query":
        params: dict[str, Any] = {"limit": args.limit}
        for k in ["service", "level", "trace_id"]:
            v = getattr(args, k, None)
            if v:
                params["trace_id" if k == "trace_id" else k] = v
        r = httpx.get(f"{base_url}/api/v1/logs", params=params, timeout=10)
        r.raise_for_status()
        print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        return

    if args.cmd == "stats":
        params = {"group_by": args.group_by, "limit": args.limit}
        if args.service:
            params["service"] = args.service
        if args.level:
            params["level"] = args.level
        r = httpx.get(f"{base_url}/api/v1/logs/stats", params=params, timeout=10)
        r.raise_for_status()
        print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        return

    if args.cmd == "alerts":
        if args.action == "list":
            r = httpx.get(f"{base_url}/api/v1/alerts/rules", timeout=10)
        elif args.action == "create":
            payload = {
                "name": args.name,
                "service": args.service,
                "min_level": args.min_level,
                "window_seconds": args.window_seconds,
                "threshold_count": args.threshold_count,
                "callback_url": args.callback_url,
            }
            r = httpx.post(f"{base_url}/api/v1/alerts/rules", json=payload, timeout=10)
        else:
            r = httpx.delete(
                f"{base_url}/api/v1/alerts/rules/{args.rule_id}", timeout=10
            )
        r.raise_for_status()
        print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        return


if __name__ == "__main__":
    main()

