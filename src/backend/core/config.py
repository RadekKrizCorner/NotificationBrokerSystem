from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from OS environment variables.

    Each field has a local-development default, but any value can be overridden
    at runtime with the `NOTIFICATION_CENTER_` prefix. For example,
    `kafka_bootstrap_servers` is read from
    `NOTIFICATION_CENTER_KAFKA_BOOTSTRAP_SERVERS` when that environment variable
    is present, otherwise it falls back to the default declared in this class.
    """

    model_config = SettingsConfigDict(env_prefix="NOTIFICATION_CENTER_", extra="ignore")

    database_url: str = "postgresql+psycopg://notification:notification@localhost:5432/notification_center"
    jwt_secret: str = Field(default="change-me-local-secret-please-change", min_length=16)
    jwt_algorithm: str = "HS256"
    fallback_deduplication_window_seconds: int = Field(default=600, gt=0)
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8000, gt=0, le=65535)
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_client_id: str = "notification-center-outbox"
    outbox_worker_id: str = "outbox-publisher-1"
    outbox_lease_seconds: int = Field(default=30, gt=0)
    outbox_retry_delay_seconds: int = Field(default=60, gt=0)
    outbox_max_attempts: int = Field(default=3, gt=0)
    outbox_publish_batch_size: int = Field(default=100, gt=0)
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    web_delivery_worker_id: str = "web-delivery-worker-1"
    email_delivery_worker_id: str = "email-delivery-worker-1"
    delivery_lease_seconds: int = Field(default=30, gt=0)
    delivery_retry_delay_seconds: int = Field(default=60, gt=0)
    delivery_batch_size: int = Field(default=100, gt=0)
    delivery_poll_interval_seconds: float = Field(default=1.0, gt=0)
    notification_consumer_name: str = "notification-requested-consumer"
    notification_consumer_topic: str = "notifications.requests"
    notification_consumer_group_id: str = "notification-center-notification-consumer"
    notification_consumer_client_id: str = "notification-center-notification-consumer"
    notification_consumer_batch_size: int = Field(default=100, gt=0)
    notification_consumer_poll_timeout_seconds: float = Field(default=1.0, gt=0)
    notification_consumer_poll_interval_seconds: float = Field(default=1.0, gt=0)
    demo_seed_user_count: int = Field(default=5000, gt=0)
    workload_api_base_url: str = "http://localhost:8000"
    workload_source_service: str = Field(default="demo-workload-generator", min_length=1)
    workload_notifications_per_window: int = Field(default=100, gt=0)
    workload_window_seconds: float = Field(default=300.0, gt=0)
    workload_request_timeout_seconds: float = Field(default=5.0, gt=0)
    workload_run_id: str | None = None
    smtp_host: str = "localhost"
    smtp_port: int = Field(default=1025, gt=0, le=65535)
    smtp_from_address: str = "notifications@example.test"
    smtp_timeout_seconds: float = Field(default=10.0, gt=0)

    @property
    def workload_interval_seconds(self) -> float:
        return self.workload_window_seconds / self.workload_notifications_per_window
