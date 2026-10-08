# Verified defects

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
