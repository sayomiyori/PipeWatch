# Local hardening contract

## Scope and assumptions

Keep PipeWatch independently runnable. Close local identity, tenant isolation,
process-loss durability and callback egress defects using existing ClickHouse,
Redis and HTTPX. No public deployment, new UI, external traffic or live Telegram
activation. `PLATFORM_AUTH_ENABLED=false` retains standalone API paths and payloads.
Platform mode requires a configured AuthFortress URL and fails closed.

## Contracts

- Existing HTTP routes require `Authorization: Bearer ...` and `X-Tenant-ID`
  in platform mode. Forward each request to
  `POST /api/v1/tenants/{tenant_id}/authorize`: `tenant.read` for reads,
  `tenant.manage` for ingestion and rule writes. Preserve 401/403/404; malformed
  or unavailable identity responses return 503. Never cache authorization.
- WebSocket routes use the same headers, authorize before accepting, and
  reauthorize every read cycle. No credentials in URL parameters.
- Store server-selected tenant IDs with logs; constrain every query, aggregate,
  rule evaluation, history lookup and live stream. Existing logs retain the empty
  standalone scope via an additive ClickHouse column. Platform requests cannot
  read this scope. Disable aggregate `/metrics` in platform mode.
- A successful ingest means the whole request is appended to a bounded Redis
  backlog atomically. Redis failures return 503; capacity exhaustion returns 429.
  A leased flusher keeps claimed records until a confirmed ClickHouse insert.
  App process loss and flusher restart replay pending records. Configure Redis
  AOF with synchronous fsync and a volume for local Compose. Redis/volume loss is
  outside this guarantee. Ambiguous ClickHouse acknowledgements can duplicate
  records; delivery is at least once, never advertised as exactly once.
- Persist rules, bounded history and cooldown claims in Redis. Validate merged
  PATCH payloads. Rule IDs from other tenants return 404. Rule updates use a
  Redis compare-and-set to reject conflicting writes with 409.
- Callback destinations must use HTTP(S), no userinfo/fragments, and every DNS
  answer must be public unicast. Validate again immediately before sending and
  pin the numeric destination while preserving Host/TLS SNI. Disable redirects
  and environment proxies. Only operator-configured exact origins with numeric
  private/loopback hosts may bypass rejection for local smoke tests. Even these
  exceptions forbid metadata, link-local, multicast, unspecified and reserved IPs.
- Persist an alert event before one bounded notification attempt. Record HTTP
  failures and delivery status; persist cooldown before delivery to prevent a
  restart storm. Pending status after a process loss means uncertain delivery;
  do not retry non-idempotent callbacks automatically. Telegram requires an
  explicit enabled flag and is disallowed in platform mode until tenant-owned
  credentials exist.
- Bind ordinary Compose ports to loopback and preserve existing data volumes.

## Implementation and acceptance

1. Add shared AuthFortress dependency, tenant filtering and isolated stream keys.
   Verify missing/invalid identity, membership, write permission, auth outage,
   malformed identity, legacy compatibility and WebSocket revocation.
2. Replace the ingest queue and alert state with Redis-backed state, keeping
   ClickHouse writes in the threadpool. Verify atomic capacity admission,
   transient DB failure, cancellation, abrupt worker death and restart against
   real isolated Redis/ClickHouse; verify rules/history/cooldown survive restart.
3. Add DNS-pinned callback sender and strict rule/log input bounds. Verify
   private/mixed DNS, rebinding, redirects, HTTP failures and successful trusted
   local callback; verify no real Telegram or external network sends.
4. Run the complete suite without skipped integration tests, Ruff, image build,
   dependency audit and a read-only security pass. Record exact evidence and
   preserve unrelated user changes. No commit or push is authorized.

## Limits

The flusher lease prevents concurrent processing while Redis is available;
ambiguous DB writes still allow duplicate storage. Redis must use no eviction
for durable queue/state. Historical standalone alert rules lived only in memory
and cannot be recovered. Host/volume backups and public deployment are excluded.
For compatibility, the legacy `count_gt` condition keeps its existing
`count >= threshold` behavior. Request bodies are limited to 1 MiB, batches to
1000 logs, rules to 100 per tenant and history to the latest 500 events. Numeric
rule values are positive, cooldown is at least one minute, and unknown input
fields are rejected. HTTP ingest success remains `status=accepted`, never flushed.
