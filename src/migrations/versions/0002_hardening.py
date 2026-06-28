"""add notification center hardening state

Revision ID: 0002_hardening
Revises: 0001_initial_schema
Create Date: 2026-06-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_hardening"
down_revision: str | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "producer_quotas",
        sa.Column("source_service", sa.Text(), nullable=False),
        sa.Column(
            "window_start",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "request_count > 0",
            name="ck_producer_quotas_positive_request_count",
        ),
        sa.PrimaryKeyConstraint(
            "source_service",
            "window_start",
        ),
    )
    op.add_column(
        "notification_deliveries",
        sa.Column("claim_token", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("claim_token", sa.Uuid(), nullable=True),
    )
    with op.batch_alter_table("notification_requests") as batch_op:
        batch_op.create_check_constraint(
            "ck_notification_requests_nonnegative_counts",
            "recipient_count >= 0 AND delivery_count >= 0",
        )
        batch_op.create_check_constraint(
            "ck_notification_requests_severity",
            "severity IN ('info', 'warning', 'error', 'critical')",
        )
        batch_op.create_check_constraint(
            "ck_notification_requests_audience_type",
            "audience_type IN ('all', 'group', 'labels')",
        )
        batch_op.create_check_constraint(
            "ck_notification_requests_status",
            "status = 'accepted'",
        )
    with op.batch_alter_table("notification_deliveries") as batch_op:
        batch_op.create_check_constraint(
            "ck_notification_deliveries_attempt_bounds",
            "attempt_count >= 0 AND max_attempts > 0 AND attempt_count <= max_attempts",
        )
        batch_op.create_check_constraint(
            "ck_notification_deliveries_channel",
            "channel IN ('web', 'email')",
        )
        batch_op.create_check_constraint(
            "ck_notification_deliveries_status",
            "status IN ('pending', 'processing', 'replay_requested', 'delivered', "
            "'failed_retryable', 'failed_terminal')",
        )
    with op.batch_alter_table("delivery_attempts") as batch_op:
        batch_op.create_check_constraint(
            "ck_delivery_attempts_positive_attempt_number",
            "attempt_number > 0",
        )
    with op.batch_alter_table("outbox_events") as batch_op:
        batch_op.create_check_constraint(
            "ck_outbox_events_nonnegative_attempts",
            "attempts >= 0",
        )
        batch_op.create_check_constraint(
            "ck_outbox_events_status",
            "status IN ('pending', 'publishing', 'published', "
            "'failed_retryable', 'failed_terminal')",
        )

    op.create_index(
        "ix_notification_recipients_user_id",
        "notification_recipients",
        ["user_id"],
    )
    op.create_index(
        "ix_notification_deliveries_notification_id",
        "notification_deliveries",
        ["notification_id"],
    )
    op.create_index(
        "ix_notification_action_invocations_notification_id",
        "notification_action_invocations",
        ["notification_id"],
    )
    op.create_index(
        "ix_outbox_events_aggregate",
        "outbox_events",
        ["aggregate_type", "aggregate_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_aggregate", table_name="outbox_events")
    op.drop_index(
        "ix_notification_action_invocations_notification_id",
        table_name="notification_action_invocations",
    )
    op.drop_index(
        "ix_notification_deliveries_notification_id",
        table_name="notification_deliveries",
    )
    op.drop_index(
        "ix_notification_recipients_user_id",
        table_name="notification_recipients",
    )
    with op.batch_alter_table("outbox_events") as batch_op:
        batch_op.drop_constraint(
            "ck_outbox_events_status",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_outbox_events_nonnegative_attempts",
            type_="check",
        )
    with op.batch_alter_table("delivery_attempts") as batch_op:
        batch_op.drop_constraint(
            "ck_delivery_attempts_positive_attempt_number",
            type_="check",
        )
    with op.batch_alter_table("notification_deliveries") as batch_op:
        batch_op.drop_constraint("ck_notification_deliveries_status", type_="check")
        batch_op.drop_constraint("ck_notification_deliveries_channel", type_="check")
        batch_op.drop_constraint(
            "ck_notification_deliveries_attempt_bounds",
            type_="check",
        )
        batch_op.drop_column("claim_token")
    with op.batch_alter_table("notification_requests") as batch_op:
        batch_op.drop_constraint("ck_notification_requests_status", type_="check")
        batch_op.drop_constraint(
            "ck_notification_requests_audience_type",
            type_="check",
        )
        batch_op.drop_constraint("ck_notification_requests_severity", type_="check")
        batch_op.drop_constraint(
            "ck_notification_requests_nonnegative_counts",
            type_="check",
        )
    with op.batch_alter_table("outbox_events") as batch_op:
        batch_op.drop_column("claim_token")
    op.drop_table("producer_quotas")
