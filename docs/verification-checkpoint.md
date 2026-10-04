# Verification checkpoint — 2026-10-03

Status: **Local scenarios verified on 2026-10-04; public/tenant acceptance blocked.**

Initial user changes to `docs/images/dc ps.png` and `docs/images/docker-services.png`
were preserved. No commits, pushes, production changes, data cleaning or destructive
commands occurred. New verification data/resources are retained.

## Changes and evidence

- Existing integration test used unsupported `ASGITransport(lifespan='on')` and old
  query/stats contracts. Reproduced failure, then used the real application lifespan
  and current documented response shapes, unique service/trace IDs.
- Backslash+quote SQL filter bypass returned **4 rows instead of zero** on real
  ClickHouse. Escape backslashes before quotes; the regression now returns zero.
- Time filters converted UTC timestamps to the Windows local timezone and returned
  zero rows for an inserted marker. Explicit UTC conversion fixes the test.
- Temporary storage failure terminated the background flush task. Retain the batch,
  retry with interval backoff and backpressure; the failure-path regression passes.
- Added `.dockerignore` and `docker-compose.test.yml` for independent ClickHouse,
  Redis and built application resources bound to loopback.

## Commands and results

From `D:/Programming/PipeWatch`:

```powershell
uv venv --python 3.12 .venv
uv pip install --python .venv/Scripts/python.exe -r requirements.txt
docker compose -p pipewatch-verification -f docker-compose.test.yml config --quiet
docker compose -p pipewatch-verification -f docker-compose.test.yml up -d --build --wait --wait-timeout 180
$env:CLICKHOUSE_HOST='127.0.0.1'
$env:CLICKHOUSE_PORT='58123'
$env:CLICKHOUSE_USER='verification'
$env:CLICKHOUSE_PASSWORD='local-test-only'
$env:CLICKHOUSE_DATABASE='pipewatch_test'
$env:REDIS_HOST='127.0.0.1'
$env:REDIS_PORT='56383'
.venv/Scripts/python.exe -m pytest -q --tb=short
# Final: 17 passed, 2 warnings, 2.71s
.venv/Scripts/python.exe -m ruff check app cli tests
# All checks passed
docker compose -p pipewatch-verification -f docker-compose.test.yml up -d --build --no-deps --wait --wait-timeout 120 app
# Latest build succeeded; app healthy
docker compose -p pipewatch-verification -f docker-compose.test.yml ps
# API38084, ClickHouse58123 and Redis56383 healthy
```

Warnings: upstream TestClient/httpx deprecation and existing Redis close deprecation.
Reviewer independently ran flush recovery (1 passed), ClickHouse boundaries (2 passed)
and narrow Ruff. Final review verdict has not yet been collected.

## Not verified and limitations

- Built-image HTTP/WebSocket/CLI smoke, image-exclusion assertion, dependency audit
  and Bandit remain pending. Passing local ASGI integration is not built HTTP evidence.
- No configured type gate or migration lifecycle was found; tables are created by code.
- No auth/tenant isolation; public alert callbacks can fetch arbitrary targets (SSRF).
  Production exposure and security configuration are not certified.
- Accepted ingestion is held in memory; process death/shutdown can lose pending data,
  ambiguous DB failures may duplicate inserts, queue saturation behavior remains unverified.
- Alert rules are in memory; alert limits, callback errors/cooldown and persistent
  configuration, real Telegram notifications and public deployment remain unverified.

Resume instructions: `../../NexusCore/docs/verification-resume-2026-10-03.md`.

## Final resume results — 2026-10-04

The earlier pending list is historical. CLI alert creation now sends the API's
canonical filter/window/threshold fields, instead of silently saving defaults.
`--window-seconds` accepts positive whole minutes only; invalid values fail before HTTP.
Two CLI/API regression tests prove this behavior. A real HTTP query returned 500
while the background flush shared a ClickHouse session. A two-thread real query
regression failed with ProgrammingError before the fix; disabling automatic session
IDs permits shared concurrent requests, as recommended by clickhouse-connect.

With the environment above:

```powershell
.venv/Scripts/python.exe -m pytest -q --tb=short
# 20 passed, 3 warnings, 2.51s; independent reviewer: 20 passed, 2.57s
.venv/Scripts/python.exe -m ruff check app cli tests scripts
# All checks passed
docker compose -p pipewatch-verification -f docker-compose.test.yml up -d --build --no-deps --wait app
.venv/Scripts/python.exe scripts/verify_http.py
# PASS: real HTTP ingest/query/stats/validation/SQL filters, Redis WebSocket,
# CLI query/stats and alert CRUD
uvx --from pip-audit pip-audit --path .venv/Lib/site-packages --format json --output .venv/verification-audit.json
# No known vulnerabilities found
uvx --from bandit bandit -r app cli -q -f json -o .venv/verification-bandit.json
# Exit 1: 21 findings, not a clean security gate
```

Image exclusion assertions passed for `/app/.env`, `/app/.git`, `/app/.venv`.
Image config: `sha256:53bf075b2e17f6b6fb73d4a4d8d940a9c035ac06d17e8073a9a10666f9067590`.
Docker VM clock was ahead of Windows by approximately 2–3 seconds. The smoke waits
at most 15 seconds for its recorded timestamp before host-clock CLI time filters;
actual query semantics are unchanged. Rich output truncates table text, so exact
messages are asserted through HTTP JSON, and CLI asserts rendered counts/levels.
Latest built logs no longer contain the shared-session traceback.

Bandit findings include dynamic SQL (escaped values and whitelisted grouping),
the normal container bind address, and existing swallowed exceptions. The real
injection regression passes, but remaining error visibility and callback egress
policy are not certified. No blanket suppression was added. Missing identity,
tenant isolation, durable ingestion/alert persistence and outage recovery remain
blockers described above. No real notification or production system was used.
