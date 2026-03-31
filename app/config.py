import os


class Settings:
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))

    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE: str = os.getenv("CLICKHOUSE_DATABASE", "pipewatch")

    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))

    APP_BATCH_FLUSH_INTERVAL_SECONDS: float = float(
        os.getenv("APP_BATCH_FLUSH_INTERVAL_SECONDS", "1")
    )
    APP_BATCH_SIZE: int = int(os.getenv("APP_BATCH_SIZE", "1000"))
    APP_QUEUE_MAXSIZE: int = int(os.getenv("APP_QUEUE_MAXSIZE", "20000"))

    REDIS_PUBLISH_ENABLED: bool = os.getenv("REDIS_PUBLISH_ENABLED", "true").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    METRICS_ENABLED: bool = os.getenv("METRICS_ENABLED", "true").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    # Alert engine
    ALERT_EVAL_INTERVAL_SECONDS: float = float(
        os.getenv("ALERT_EVAL_INTERVAL_SECONDS", "30")
    )
    ALERT_COOLDOWN_SECONDS: float = float(
        os.getenv("ALERT_COOLDOWN_SECONDS", "300")
    )
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")


settings = Settings()
