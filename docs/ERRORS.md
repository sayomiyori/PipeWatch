# Verified defects

## 2026-10-12: Image inherited an outdated package installer

Actual image inspection found pip 25.0.1, the version rejected by the current
package advisory audit. Upgrade the installer before the existing requirements;
retain UID 10001 and all runtime contracts. Verify the rebuilt inventory,
pip check and the complete Redis/ClickHouse/HTTP/WS acceptance gates.

## 2026-10-12: Callback translation and metric cardinality review

IPv6 NAT64/6to4/Teredo/mapped destinations could hide an unsafe IPv4 endpoint.
Reject translation and tunnel ranges before both public-IP and trusted-origin
checks. Callback policy regressions include translated private and multicast IPs.

Caller-selected service names created unbounded accepted-log metric labels, and
disabled/platform metrics still collected counters. Use one standalone `all`
bucket and skip collection in disabled/platform modes. Four mode combinations
with 1000 distinct services verify bounded series, the retained accepted count
and unchanged backlog admission.

## 2026-10-12: Local identity, durability and notification boundaries

HTTP/WS management and reads previously lacked tenant authorization. Opt-in
platform mode now forwards each authorization to AuthFortress, rejects denied
or unavailable identity and scopes ClickHouse, Redis streams, rules and history.
Process-wide metrics are unavailable in this mode. Negative HTTP/WS isolation
checks and real ClickHouse tenant queries cover these boundaries.

Accepted ingestion, rules and history lived only in process memory. Redis now
holds atomic bounded admission, claimed batches, rules, bounded history and
cooldown. A renewable lease fences batch acknowledgement. An abrupt subprocess
death regression replays a retained batch into ClickHouse; an isolated Redis
SIGKILL/AOF restart probe verifies backlog/rules/history/cooldown survival.
Ambiguous ClickHouse acknowledgements may still duplicate rows.

Callbacks allowed private addresses, swallowed HTTP failures and spawned
untracked tasks. Validate all fresh DNS answers and connect to a pinned numeric
IP with original Host/TLS SNI, no proxy or redirects. Persist history/cooldown
before one bounded attempt and record delivery failure. Tests cover mixed DNS,
private/metadata targets, rebinding, redirects, HTTP errors and a real trusted
loopback callback. Telegram is an explicit standalone opt-in and cannot be
enabled in platform mode. See `local-hardening.md` for commands and limits.

## 2026-10-09: Shutdown draining and blocking queries

Cancellation during insert/publication could duplicate a confirmed batch and
leave queued records unsaved. Shield the in-flight insert, clear confirmed data
before Redis publication, then drain queued batches on graceful cancellation.
Two regressions cover cancellation during insert and publication with 100 unique
records. Abrupt process loss remains unsafe because the ingest queue is in memory.

Three async query endpoints called synchronous ClickHouse methods on the event
loop. Sync handlers now use FastAPI's threadpool. A real 300 ms database query
delayed a neighbouring timer by 320 ms before and 24 ms after; all three routes
have responsiveness regressions. Full suite 25 passed, Ruff clean; source HTTP,
Redis WebSocket and CLI smoke passed. Independent review approved.

## 2026-10-09: CI test discovery and missing infrastructure

GitHub run `37849286678` failed collection with `ModuleNotFoundError: app`;
the pytest launcher did not add the repository root to Python's import path.
Use the locally verified `python -m pytest` invocation. The workflow now starts
the existing test Compose ClickHouse/Redis services, supplies isolated test
configuration and rejects skipped tests through its JUnit report. Cleanup stops
only that Compose project without deleting volumes.

## 2026-10-03 verification

| Symptom | Root cause | Fix | Regression |
| --- | --- | --- | --- |
| Integration test failed before application startup | Unsupported HTTPX lifespan parameter and stale response expectations | Real application lifespan; current documented query/stats contracts | `tests/test_ingest_query_verify.py`, real ClickHouse/Redis |
| Filter could read unrelated rows | Backslashes were not escaped in ClickHouse SQL string literals | Escape backslashes before quotes | `test_backslash_quote_filter_cannot_change_query_predicate`: real DB returned 4 before fix, 0 afterward |
| UTC time window excluded its inserted marker | `astimezone()` used host local timezone | Convert explicitly to UTC | `test_timezone_filter_uses_utc`: 0 before fix, 1 afterward on Windows |
| Temporary DB outage permanently stopped ingestion flush | Storage exception escaped the task | Keep batch and retry with interval backoff | `test_flush_recovers_from_temporary_database_failure`: failed before, passed after |

Final complete suite: 17 passed. See `verification-checkpoint.md` for commands and
unfinished acceptance gates. No public-deployment readiness is implied.
# 2026-10-04 — Concurrent ClickHouse requests and CLI alert contract

Real HTTP query during flush returned 500: the shared client automatically created
one session and rejected overlapping queries. A real two-thread barrier test
reproduced ProgrammingError; `autogenerate_session_id=False` fixes it without
changing query semantics. Full suite: 20 passed.

CLI alert creation sent legacy field names which the API ignored, saving default
filters and thresholds. Use canonical API fields and validate positive whole-minute
windows. Two HTTP-boundary CLI regressions cover preserved values and invalid input.
