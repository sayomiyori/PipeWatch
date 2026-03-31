# PipeWatch

Log aggregator: **FastAPI** + **ClickHouse** + **Redis Streams** (live tail), with **alerts**, **Prometheus** metrics, and optional **Grafana** dashboards.

## Features

- Ingest: `POST /api/v1/logs`, `POST /api/v1/logs/batch`
- Query: `GET /api/v1/logs` (filters, pagination, full-text `q`), `GET /api/v1/logs/stats`, `GET /api/v1/logs/services`
- Live tail: WebSocket `GET /ws/tail?service=…&level=…`
- Alerts: CRUD ` /api/v1/alerts`, engine evaluates every **30s** (configurable), ClickHouse `COUNT`, optional **HTTP callback** and **Telegram**
- Metrics: `GET /metrics` (Prometheus)
- CLI: `pipewatch` (Click + Rich)

## Quick start (Docker Compose)

```bash
docker compose up -d --build
```

| Service     | Host port | Notes        |
|------------|-----------|--------------|
| API        | 18080     | Base URL     |
| ClickHouse | 18123 HTTP, 19000 native | |
| Redis      | 16379     |              |
| Prometheus | 19090     | UI           |
| Grafana    | 13000     | admin / admin |

Health: `curl -s http://localhost:18080/metrics | head`

## Configuration (environment)

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_*` | see `app/config.py` | DB connection |
| `REDIS_*` | | Redis for streams |
| `ALERT_EVAL_INTERVAL_SECONDS` | `30` | Alert engine tick |
| `ALERT_COOLDOWN_SECONDS` | `300` | Min seconds between fires per rule |
| `TELEGRAM_BOT_TOKEN` | empty | Bot token from [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | empty | Default chat; per-rule override: `telegram_chat_id` |
| `METRICS_ENABLED` | `true` | `/metrics` |
| `PIPEWATCH_BASE_URL` | — | CLI default base URL |

## API — Alerts

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/alerts` | Create rule |
| `GET` | `/api/v1/alerts` | List rules |
| `GET` | `/api/v1/alerts/{id}` | Get rule |
| `PATCH` | `/api/v1/alerts/{id}` | Update rule |
| `DELETE` | `/api/v1/alerts/{id}` | Delete rule |
| `GET` | `/api/v1/alerts/events` | Recent alert events |

Legacy paths `/api/v1/alerts/rules` are kept (deprecated).

Rule body: `name`, optional `service`, optional `min_level` (severity **≥** this level), `window_seconds`, `threshold_count`, optional `callback_url`, optional `telegram_chat_id`.

## CLI

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e cli/   # or: pip install -e .
export PIPEWATCH_BASE_URL=http://localhost:18080

pipewatch stats --last=24h
pipewatch query --service=payment-api --level=error --last=1h --limit=20
pipewatch tail --service=payment-api --level=error
pipewatch alerts list
```

## Monitoring

- **Prometheus**: `http://localhost:19090` — scrapes `app:8000/metrics`.
- **Grafana**: `http://localhost:13000` — datasource **Prometheus** is provisioned; build dashboards from metrics `pipewatch_*`.

## Development

```bash
pip install -r requirements.txt
pip install ruff
ruff check app cli tests
pytest -q
```

## CI

GitHub Actions (`.github/workflows/ci.yml`): **ruff**, **pytest**, **docker build**.

## License

MIT (or as specified by the repository owner).
