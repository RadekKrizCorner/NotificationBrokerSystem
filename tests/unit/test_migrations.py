from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


class TestMigrations:
    def test_alembic_upgrade_creates_initial_schema(self, tmp_path: Path) -> None:
        database_path = tmp_path / "backend.db"
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", f"sqlite+pysqlite:///{database_path}")

        command.upgrade(config, "head")

        engine = create_engine(f"sqlite+pysqlite:///{database_path}")
        try:
            inspector = inspect(engine)
            assert {
                "users",
                "groups",
                "user_groups",
                "user_labels",
                "notification_requests",
                "notification_recipients",
                "notification_deliveries",
                "delivery_attempts",
                "notification_action_invocations",
                "outbox_events",
                "processed_events",
                "producer_quotas",
            }.issubset(set(inspector.get_table_names()))

            delivery_indexes = {
                index["name"] for index in inspector.get_indexes("notification_deliveries")
            }
            assert "ix_notification_deliveries_me" in delivery_indexes
            assert "ix_notification_deliveries_worker" in delivery_indexes
            assert "ix_notification_deliveries_notification_id" in delivery_indexes
            delivery_checks = {
                constraint["name"]
                for constraint in inspector.get_check_constraints("notification_deliveries")
            }
            assert "ck_notification_deliveries_attempt_bounds" in delivery_checks
            assert "ck_notification_deliveries_channel" in delivery_checks
            assert "ck_notification_deliveries_status" in delivery_checks

            recipient_indexes = {
                index["name"] for index in inspector.get_indexes("notification_recipients")
            }
            assert "ix_notification_recipients_user_id" in recipient_indexes
            delivery_columns = {
                column["name"] for column in inspector.get_columns("notification_deliveries")
            }
            assert "claim_token" in delivery_columns

            outbox_indexes = {index["name"] for index in inspector.get_indexes("outbox_events")}
            assert "ix_outbox_events_publisher" in outbox_indexes
            outbox_columns = {column["name"] for column in inspector.get_columns("outbox_events")}
            assert "claim_token" in outbox_columns
        finally:
            engine.dispose()
