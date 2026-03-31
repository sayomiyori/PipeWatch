#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import click
import httpx
import websockets
from rich import box
from rich.console import Console
from rich.table import Table

console = Console()

LEVEL_COLORS: dict[str, str] = {
    "debug": "dim white",
    "info":  "green",
    "warn":  "yellow",
    "error": "red",
    "fatal": "bold magenta",
}


def _parse_last(value: str) -> tuple[datetime, datetime]:
    """Parse '1h', '30m', '7d' into (from_ts, to_ts)."""
    unit = value[-1].lower()
    try:
        amount = int(value[:-1])
    except (ValueError, IndexError):
        raise click.BadParameter(f"Invalid duration '{value}'. Use e.g. 30m, 1h, 7d")
    now = datetime.now(timezone.utc)
    match unit:
        case "m":
            delta = timedelta(minutes=amount)
        case "h":
            delta = timedelta(hours=amount)
        case "d":
            delta = timedelta(days=amount)
        case _:
            raise click.BadParameter(f"Unknown unit '{unit}'. Use m / h / d")
    return now - delta, now


# ─────────────────────────────────────────────────────────────────────────────
# Root group
# ─────────────────────────────────────────────────────────────────────────────

@click.group()
@click.version_option("0.1.0", prog_name="pipewatch")
@click.option(
    "--base-url",
    default=lambda: os.getenv("PIPEWATCH_BASE_URL", "http://localhost:8000"),
    show_default="$PIPEWATCH_BASE_URL or http://localhost:8000",
    help="PipeWatch API base URL",
)
@click.pass_context
def cli(ctx: click.Context, base_url: str) -> None:
    ctx.ensure_object(dict)
    ctx.obj["base_url"] = base_url.rstrip("/")


# ─────────────────────────────────────────────────────────────────────────────
# tail
# ─────────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--service", required=True, help="Service name to tail")
@click.option(
    "--level",
    type=click.Choice(["debug", "info", "warn", "error", "fatal"]),
    default=None,
    help="Filter by log level",
)
@click.option(
    "--start",
    type=click.Choice(["latest", "begin"]),
    default="latest",
    show_default=True,
)
@click.pass_context
def tail(ctx: click.Context, service: str, level: str | None, start: str) -> None:
    """Live tail logs via WebSocket (Ctrl-C to stop)."""
    asyncio.run(_tail_async(ctx.obj["base_url"], service, level, start))


async def _tail_async(
    base_url: str, service: str, level: str | None, start: str
) -> None:
    ws_url = base_url.replace("http://", "ws://").replace("https://", "wss://")
    qs = f"service={service}&start={start}"
    if level:
        qs += f"&level={level}"
    url = f"{ws_url}/ws/tail?{qs}"

    filter_desc = f"level=[bold]{level}[/bold]" if level else "all levels"
    console.print(f"[dim]Connecting to [bold]{service}[/bold] ({filter_desc})  Ctrl-C to stop[/dim]")

    try:
        async with websockets.connect(url) as ws:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    log = msg.get("log", {})
                    ts  = str(log.get("timestamp", ""))[:23]
                    svc = log.get("service", "")
                    lvl = log.get("level", "info")
                    txt = log.get("message", "")
                    tid = log.get("trace_id", "")
                    color = LEVEL_COLORS.get(lvl, "white")
                    trace_str = f" [dim][{tid}][/dim]" if tid else ""
                    console.print(
                        f"[dim]{ts}[/dim] [cyan]{svc}[/cyan] "
                        f"[{color}]{lvl.upper():5}[/{color}]  {txt}{trace_str}"
                    )
                except (json.JSONDecodeError, KeyError):
                    console.print(raw)
    except KeyboardInterrupt:
        console.print("\n[dim]Disconnected.[/dim]")
    except Exception as exc:
        console.print(f"[red]Connection error: {exc}[/red]")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# query
# ─────────────────────────────────────────────────────────────────────────────

@cli.command("query")
@click.option("--service", default=None)
@click.option(
    "--level",
    type=click.Choice(["debug", "info", "warn", "error", "fatal"]),
    default=None,
)
@click.option("--trace-id", default=None)
@click.option("-q", "--search", default=None, metavar="TEXT", help="Full-text search in message")
@click.option("--last", default=None, metavar="DURATION", help="Time window: 1h, 30m, 7d")
@click.option("--limit", default=20, show_default=True)
@click.option("--page",  default=1,  show_default=True)
@click.pass_context
def query_cmd(
    ctx: click.Context,
    service: str | None,
    level: str | None,
    trace_id: str | None,
    search: str | None,
    last: str | None,
    limit: int,
    page: int,
) -> None:
    """Query logs and display as a table."""
    base_url = ctx.obj["base_url"]
    params: dict[str, Any] = {"size": limit, "page": page}
    if service:
        params["service"] = service
    if level:
        params["level"] = level
    if trace_id:
        params["trace_id"] = trace_id
    if search:
        params["q"] = search
    if last:
        from_ts, to_ts = _parse_last(last)
        params["from_ts"] = from_ts.isoformat()
        params["to_ts"]   = to_ts.isoformat()

    try:
        r = httpx.get(f"{base_url}/api/v1/logs", params=params, timeout=15)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]HTTP error: {exc}[/red]")
        sys.exit(1)

    data  = r.json()
    items = data.get("items", [])
    total = data.get("total", len(items))

    tbl = Table(box=box.SIMPLE_HEAD, show_footer=False, highlight=True)
    tbl.add_column("Time",     style="dim",   no_wrap=True, min_width=23)
    tbl.add_column("Service",  style="cyan",  min_width=12)
    tbl.add_column("Level",    min_width=6)
    tbl.add_column("Message")
    tbl.add_column("Trace ID", style="dim",   min_width=10)

    for item in items:
        lvl   = item.get("level", "info")
        color = LEVEL_COLORS.get(lvl, "white")
        tbl.add_row(
            str(item.get("timestamp", ""))[:23],
            str(item.get("service",   "")),
            f"[{color}]{lvl.upper()}[/{color}]",
            str(item.get("message",   "")),
            str(item.get("trace_id",  "")),
        )

    console.print(tbl)
    console.print(
        f"[dim]Showing {len(items)} of {total} total  "
        f"(page {data.get('page', page)} · size {data.get('size', limit)})[/dim]"
    )


# ─────────────────────────────────────────────────────────────────────────────
# stats
# ─────────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--last", default="24h", show_default=True, metavar="DURATION")
@click.pass_context
def stats(ctx: click.Context, last: str) -> None:
    """Show log statistics for a time window."""
    base_url = ctx.obj["base_url"]
    from_ts, to_ts = _parse_last(last)
    params = {"from_ts": from_ts.isoformat(), "to_ts": to_ts.isoformat()}

    try:
        r = httpx.get(f"{base_url}/api/v1/logs/stats", params=params, timeout=15)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]HTTP error: {exc}[/red]")
        sys.exit(1)

    data = r.json()
    console.print(f"\n[bold]Stats — last {last}[/bold]\n")
    console.print(f"  Total logs: [bold]{data.get('total_count', 0):,}[/bold]\n")

    if data.get("count_by_level"):
        tbl = Table("Level", "Count", box=box.SIMPLE, show_header=True)
        for lvl, cnt in sorted(data["count_by_level"].items(),
                                key=lambda kv: -kv[1]):
            color = LEVEL_COLORS.get(lvl, "white")
            tbl.add_row(f"[{color}]{lvl.upper()}[/{color}]", f"{cnt:,}")
        console.print("  [bold]By level[/bold]")
        console.print(tbl)

    if data.get("count_by_service"):
        tbl = Table("Service", "Count", box=box.SIMPLE)
        for svc, cnt in list(data["count_by_service"].items())[:10]:
            tbl.add_row(f"[cyan]{svc}[/cyan]", f"{cnt:,}")
        console.print("  [bold]By service[/bold]")
        console.print(tbl)

    if data.get("top_errors"):
        tbl = Table("Service", "Count", "Error message", box=box.SIMPLE)
        for e in data["top_errors"][:10]:
            tbl.add_row(
                f"[cyan]{e['service']}[/cyan]",
                str(e["count"]),
                e["message"][:90],
            )
        console.print("  [bold]Top errors[/bold]")
        console.print(tbl)


# ─────────────────────────────────────────────────────────────────────────────
# alerts group
# ─────────────────────────────────────────────────────────────────────────────

@cli.group()
def alerts() -> None:
    """Manage alert rules."""


@alerts.command("list")
@click.pass_context
def alerts_list(ctx: click.Context) -> None:
    """List all alert rules."""
    base_url = ctx.obj["base_url"]
    r = httpx.get(f"{base_url}/api/v1/alerts", timeout=10)
    r.raise_for_status()
    rules = r.json().get("rules", {})
    if not rules:
        console.print("[dim]No alert rules defined.[/dim]")
        return
    tbl = Table("ID", "Name", "Service", "Min level",
                "Window (s)", "Threshold", box=box.SIMPLE_HEAD)
    for rid, rule in rules.items():
        tbl.add_row(
            rid[:8] + "…",
            rule.get("name", ""),
            rule.get("service") or "[dim]any[/dim]",
            rule.get("min_level") or "[dim]any[/dim]",
            str(rule.get("window_seconds", "")),
            str(rule.get("threshold_count", "")),
        )
    console.print(tbl)


@alerts.command("create")
@click.option("--name",             required=True)
@click.option("--service",          default=None)
@click.option("--min-level",
              type=click.Choice(["debug", "info", "warn", "error", "fatal"]),
              default=None)
@click.option("--window-seconds",   default=60,  show_default=True)
@click.option("--threshold-count",  default=1,   show_default=True)
@click.option("--callback-url",     default=None)
@click.pass_context
def alerts_create(
    ctx: click.Context,
    name: str,
    service: str | None,
    min_level: str | None,
    window_seconds: int,
    threshold_count: int,
    callback_url: str | None,
) -> None:
    """Create a new alert rule."""
    base_url = ctx.obj["base_url"]
    payload = {
        "name": name, "service": service, "min_level": min_level,
        "window_seconds": window_seconds, "threshold_count": threshold_count,
        "callback_url": callback_url,
    }
    r = httpx.post(f"{base_url}/api/v1/alerts", json=payload, timeout=10)
    r.raise_for_status()
    rid = r.json().get("rule_id", "?")
    console.print(f"[green]Created rule [bold]{rid}[/bold][/green]")


@alerts.command("delete")
@click.argument("rule_id")
@click.pass_context
def alerts_delete(ctx: click.Context, rule_id: str) -> None:
    """Delete an alert rule by ID."""
    base_url = ctx.obj["base_url"]
    r = httpx.delete(f"{base_url}/api/v1/alerts/{rule_id}", timeout=10)
    r.raise_for_status()
    console.print(f"[yellow]Deleted rule {rule_id}[/yellow]")


if __name__ == "__main__":
    cli()
