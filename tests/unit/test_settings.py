import pytest

from backend.core.config import Settings


class TestSettings:
    def test_settings_docstring_explains_environment_overrides(self) -> None:
        assert Settings.__doc__ is not None
        assert "NOTIFICATION_CENTER_" in Settings.__doc__
        assert "environment" in Settings.__doc__.lower()
        assert "default" in Settings.__doc__.lower()

    def test_environment_variables_override_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "NOTIFICATION_CENTER_KAFKA_BOOTSTRAP_SERVERS",
            "redpanda:9092",
        )
        monkeypatch.setenv("NOTIFICATION_CENTER_OUTBOX_PUBLISH_BATCH_SIZE", "25")
        monkeypatch.setenv("NOTIFICATION_CENTER_API_PORT", "9001")
        monkeypatch.setenv("NOTIFICATION_CENTER_DELIVERY_POLL_INTERVAL_SECONDS", "3.5")
        monkeypatch.setenv("NOTIFICATION_CENTER_NOTIFICATION_CONSUMER_BATCH_SIZE", "11")
        monkeypatch.setenv("NOTIFICATION_CENTER_DEMO_SEED_USER_COUNT", "10000")
        monkeypatch.setenv("NOTIFICATION_CENTER_WORKLOAD_API_BASE_URL", "http://api:8000")
        monkeypatch.setenv("NOTIFICATION_CENTER_WORKLOAD_NOTIFICATIONS_PER_WINDOW", "100")
        monkeypatch.setenv("NOTIFICATION_CENTER_WORKLOAD_WINDOW_SECONDS", "300")
        monkeypatch.setenv("NOTIFICATION_CENTER_WORKLOAD_REQUEST_TIMEOUT_SECONDS", "7.5")
        monkeypatch.setenv("NOTIFICATION_CENTER_WORKLOAD_RUN_ID", "demo-run")

        settings = Settings()

        assert settings.kafka_bootstrap_servers == "redpanda:9092"
        assert settings.outbox_publish_batch_size == 25
        assert settings.api_port == 9001
        assert settings.delivery_poll_interval_seconds == 3.5
        assert settings.notification_consumer_batch_size == 11
        assert settings.demo_seed_user_count == 10000
        assert settings.workload_api_base_url == "http://api:8000"
        assert settings.workload_notifications_per_window == 100
        assert settings.workload_window_seconds == 300.0
        assert settings.workload_request_timeout_seconds == 7.5
        assert settings.workload_run_id == "demo-run"
        assert settings.workload_interval_seconds == 3.0
