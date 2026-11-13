# Local hardening and operations

## Behavior

Standalone mode retains the existing routes and response shapes. Platform mode
requires `PLATFORM_AUTH_ENABLED=true` and `AUTHFORTRESS_URL` pointing to the
independent AuthFortress service. Send `Authorization: Bearer ...` and
`X-Tenant-ID: <uuid>` on every HTTP/WebSocket request. Reads require tenant.read;
ingestion and alert mutations require tenant.manage. AuthFortress is called
fresh for every operation; WebSockets reauthorize each read cycle (up to the
5-second Redis read window). Authorization is not cached.

Auth failures retain 401/403/404, unavailable or malformed identity is 503.
Other tenants' rule IDs return 404. Tenant context is selected by authorization,
never by payload fields. Old standalone logs and rules keep their empty scope;
platform requests and its evaluator cannot access them. Aggregate `/metrics`
returns 404 in platform mode because its process-wide values are unscoped.
`/health` is liveness; `/ready` checks Redis, ClickHouse and background tasks.

Use `PIPEWATCH_TOKEN` and `PIPEWATCH_TENANT_ID` in the CLI process environment.
The CLI sends them in headers, including WebSocket headers. It never includes
tokens in URLs or command arguments. `.env.example` documents the settings;
the application reads process environment, not `.env` files directly.

## Durability and replay

HTTP ingest success remains `{status: accepted, count: N}` and means the complete
request was appended atomically to Redis. It does not promise a ClickHouse flush.
Backlog capacity includes queued and claimed records; a full backlog is 429 and
Redis failure is 503. `/ready` may remain healthy during a bounded backlog, so
operators should inspect queue lengths and storage failures when capacity fills.

Redis must run with `appendonly yes`, `appendfsync always` and
`maxmemory-policy noeviction`; startup rejects weaker configuration. Ordinary
Compose adds a named Redis volume and leaves the existing ClickHouse volume
unchanged. External Redis installations must meet the same requirements.
The app completes an in-flight insert on graceful cancellation and leaves the
remaining durable backlog for the next process. Abrupt death retains the claimed
batch. A renewable 30-second lease allows recovery; stale workers cannot remove
the current worker's claimed batch. Allow up to 30 seconds for lease expiry after
process loss. Keep `REDIS_KEY_PREFIX` stable across restarts.

ClickHouse writes can have ambiguous acknowledgements. These records are replayed
and can produce duplicate rows, sharing their server-created log_id. This is
at-least-once storage, not exactly once. Live tail publication occurs after
confirmed storage and is best effort; losing publication does not remove the
stored log. Losing Redis or its data volume loses pending records and alert state.
The volume is persistence, not a backup. Historical in-memory rules are unrecoverable.

Alert rules, cooldown and the latest 500 events per tenant persist in Redis.
Per-rule history returns the latest 200 retained events. PATCH validates the
merged rule and uses compare-and-set; conflicting writes return 409. A tenant
may hold at most 100 rules. Request bodies are limited to 1 MiB and batches to
1000 logs. Unknown fields, nonpositive thresholds/windows/cooldowns and invalid
notifications are rejected. The legacy count_gt condition preserves its existing
`count >= threshold` meaning.

## Notifications

Public HTTP(S) callback destinations are validated at rule creation/update and
again before sending. All DNS answers must be public unicast. The transport
connects to the validated numeric IP, preserves the original Host/TLS SNI,
verifies certificates, ignores proxy environment and never follows redirects.
No URL userinfo or fragments are accepted. Callback response bodies are not
buffered. The only private exceptions are operator-configured exact origins with
numeric private/loopback hosts. Metadata, link-local, multicast, unspecified and
reserved addresses stay forbidden, including under an exception. Do not use
hostname exceptions; they cannot be made safe against rebinding.

An alert history event and cooldown claim are persisted before one bounded
notification attempt. History records delivered, failed, pending or
not_requested. Pending after process loss means uncertain delivery. Automatic
POST replay is deliberately absent because arbitrary receivers have no
idempotency contract. A failure still holds its cooldown and remains visible.
Telegram defaults off and requires an explicit standalone opt-in. Platform mode
rejects Telegram rules because tenant-owned Telegram credentials are absent.

## Verification commands

Use the isolated `pipewatch-closure` Compose project. It publishes ClickHouse on
127.0.0.1:58123, Redis on 127.0.0.1:56383 and the API on 127.0.0.1:38084.

```powershell
docker compose -p pipewatch-closure -f docker-compose.test.yml config --quiet
docker compose -p pipewatch-closure -f docker-compose.test.yml up -d --wait --wait-timeout 180 clickhouse redis
$env:CLICKHOUSE_HOST='127.0.0.1'
$env:CLICKHOUSE_PORT='58123'
$env:CLICKHOUSE_USER='verification'
$env:CLICKHOUSE_PASSWORD='local-test-only'
$env:CLICKHOUSE_DATABASE='pipewatch_test'
$env:REDIS_HOST='127.0.0.1'
$env:REDIS_PORT='56383'
.venv/Scripts/python.exe -m pytest -q --junitxml=.pytest_cache/local-closure.xml
.venv/Scripts/python.exe -m ruff check app cli tests scripts
uv pip check --python .venv/Scripts/python.exe
docker compose -p pipewatch-closure -f docker-compose.test.yml up -d --build --wait --wait-timeout 180 app
.venv/Scripts/python.exe scripts/verify_http.py
docker compose -p pipewatch-closure -f docker-compose.test.yml exec -T app python -m pip check
```

`tests/test_durable_state.py` runs a child flusher that exits abruptly after claim,
then proves the replacement flushes the retained record into real ClickHouse.
`scripts/verify_redis_restart.py` prepares and checks a unique durability marker
namespace. For a controlled test, use only the explicitly isolated Redis
container; never target the root/project Redis or delete volumes:

```powershell
$env:PYTHONPATH=(Get-Location).Path
.venv/Scripts/python.exe scripts/verify_redis_restart.py prepare pipewatch-restart-test:UNIQUE_ID
docker kill --signal SIGKILL pipewatch-closure-redis-1
docker compose -p pipewatch-closure -f docker-compose.test.yml up -d --wait --wait-timeout 90 redis
.venv/Scripts/python.exe scripts/verify_redis_restart.py verify pipewatch-restart-test:UNIQUE_ID
```

The probe verifies backlog, rule, pending history and cooldown survival. It
expires only its own marker keys after verification. Stop this test project with
`docker compose -p pipewatch-closure -f docker-compose.test.yml stop` to keep data.

## Security review and limits

Dependency audit of both the host environment and built image reported no known
vulnerabilities. Bandit reported one medium-confidence container bind warning
and ten low-confidence SQL interpolation warnings, with no high-severity
findings. Container binding is isolated by loopback Compose publications. SQL
string inputs pass the existing quote/backslash escaping and real injection
regression; numeric limits are bounded and grouping columns are server-selected.
Database identifiers come only from operator configuration. No warning was
silently suppressed.

This validates a local standalone service and the platform boundary with a
mocked AuthFortress HTTP transport. Live identity integration, public TLS/VPS,
backups, disaster recovery, real Telegram and external callback delivery remain
outside this checkpoint. No root runtime was activated and no commit/push was made.
