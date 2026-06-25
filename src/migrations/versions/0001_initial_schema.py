"""initial notification center schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-06-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

uuid_type = sa.Uuid(as_uuid=True)
json_document = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )
    op.create_table(
        "groups",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "notification_requests",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("source_service", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("audience_type", sa.String(length=32), nullable=False),
        sa.Column("audience", json_document, nullable=False),
        sa.Column("channels", json_document, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("payload_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("deduplication_hash", sa.String(length=64), nullable=True),
        sa.Column("deduplication_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recipient_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("delivery_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "idempotency_key IS NOT NULL OR "
            "(deduplication_hash IS NOT NULL AND deduplication_window_start IS NOT NULL)",
            name="ck_notification_requests_deduplication_present",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", uuid_type, nullable=False),
        sa.Column("event_key", sa.Text(), nullable=False),
        sa.Column("payload", json_document, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "processed_events",
        sa.Column("event_id", uuid_type, nullable=False),
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "event_id"),
    )
    op.create_table(
        "user_groups",
        sa.Column("user_id", uuid_type, nullable=False),
        sa.Column("group_id", uuid_type, nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["groups.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "group_id"),
    )
    op.create_table(
        "user_labels",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("user_id", uuid_type, nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "key", name="uq_user_labels_user_key"),
    )
    op.create_table(
        "notification_recipients",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("notification_id", uuid_type, nullable=False),
        sa.Column("user_id", uuid_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notification_requests.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "notification_id",
            "user_id",
            name="uq_notification_recipients_notification_user",
        ),
    )
    op.create_table(
        "notification_deliveries",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("notification_recipient_id", uuid_type, nullable=False),
        sa.Column("notification_id", uuid_type, nullable=False),
        sa.Column("user_id", uuid_type, nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("replay_id", uuid_type, nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_message_id", sa.Text(), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notification_requests.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["notification_recipient_id"],
            ["notification_recipients.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "notification_recipient_id",
            "channel",
            name="uq_notification_deliveries_recipient_channel",
        ),
    )
    op.create_table(
        "delivery_attempts",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("delivery_id", uuid_type, nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("provider_message_id", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["delivery_id"],
            ["notification_deliveries.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "delivery_id",
            "attempt_number",
            name="uq_delivery_attempts_delivery_attempt_number",
        ),
    )
    op.create_table(
        "notification_action_invocations",
        sa.Column("id", uuid_type, nullable=False),
        sa.Column("notification_id", uuid_type, nullable=False),
        sa.Column("web_delivery_id", uuid_type, nullable=True),
        sa.Column("requested_by_type", sa.String(length=32), nullable=False),
        sa.Column("requested_by_id", sa.Text(), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("result", sa.String(length=32), nullable=False),
        sa.Column("replay_id", uuid_type, nullable=True),
        sa.Column("replayed_delivery_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(result = 'queued' AND replay_id IS NOT NULL) OR "
            "(result = 'no_eligible' AND replay_id IS NULL)",
            name="ck_notification_action_invocations_replay_result",
        ),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notification_requests.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["web_delivery_id"],
            ["notification_deliveries.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "uq_notification_requests_source_idempotency_key",
        "notification_requests",
        ["source_service", "idempotency_key"],
        unique=True,
        sqlite_where=sa.text("idempotency_key IS NOT NULL"),
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index(
        "uq_notification_requests_source_deduplication",
        "notification_requests",
        ["source_service", "deduplication_hash", "deduplication_window_start"],
        unique=True,
        sqlite_where=sa.text("idempotency_key IS NULL"),
        postgresql_where=sa.text("idempotency_key IS NULL"),
    )
    op.create_index("ix_user_groups_lookup", "user_groups", ["group_id", "user_id"])
    op.create_index("ix_user_labels_lookup", "user_labels", ["key", "value", "user_id"])
    op.create_index(
        "ix_notification_deliveries_me",
        "notification_deliveries",
        ["user_id", "channel", "status", "read_at", "delivered_at", "id"],
    )
    op.create_index(
        "ix_notification_deliveries_worker",
        "notification_deliveries",
        ["status", "next_attempt_at", "lease_expires_at"],
    )
    op.create_index(
        "ix_outbox_events_publisher",
        "outbox_events",
        ["status", "next_attempt_at", "lease_expires_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_publisher", table_name="outbox_events")
    op.drop_index("ix_notification_deliveries_worker", table_name="notification_deliveries")
    op.drop_index("ix_notification_deliveries_me", table_name="notification_deliveries")
    op.drop_index("ix_user_labels_lookup", table_name="user_labels")
    op.drop_index("ix_user_groups_lookup", table_name="user_groups")
    op.drop_index(
        "uq_notification_requests_source_deduplication",
        table_name="notification_requests",
    )
    op.drop_index(
        "uq_notification_requests_source_idempotency_key",
        table_name="notification_requests",
    )
    op.drop_table("notification_action_invocations")
    op.drop_table("delivery_attempts")
    op.drop_table("notification_deliveries")
    op.drop_table("notification_recipients")
    op.drop_table("user_labels")
    op.drop_table("user_groups")
    op.drop_table("processed_events")
    op.drop_table("outbox_events")
    op.drop_table("notification_requests")
    op.drop_table("groups")
    op.drop_table("users")
