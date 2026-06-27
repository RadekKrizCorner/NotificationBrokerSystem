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


def downgrade() -> None:
    op.drop_table("producer_quotas")
