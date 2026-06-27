from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Uuid

from backend.db.models.base import Base, TimestampMixin, json_document, utc_now
from backend.db.models.identity import UserModel


class NotificationRequestModel(TimestampMixin, Base):
    __tablename__ = "notification_requests"
    __table_args__ = (
        CheckConstraint(
            "recipient_count >= 0 AND delivery_count >= 0",
            name="ck_notification_requests_nonnegative_counts",
        ),
        CheckConstraint(
            "severity IN ('info', 'warning', 'error', 'critical')",
            name="ck_notification_requests_severity",
        ),
        CheckConstraint(
            "audience_type IN ('all', 'group', 'labels')",
            name="ck_notification_requests_audience_type",
        ),
        CheckConstraint(
            "status = 'accepted'",
            name="ck_notification_requests_status",
        ),
        CheckConstraint(
            "idempotency_key IS NOT NULL OR "
            "(deduplication_hash IS NOT NULL AND deduplication_window_start IS NOT NULL)",
            name="ck_notification_requests_deduplication_present",
        ),
        Index(
            "uq_notification_requests_source_idempotency_key",
            "source_service",
            "idempotency_key",
            unique=True,
            sqlite_where=text("idempotency_key IS NOT NULL"),
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index(
            "uq_notification_requests_source_deduplication",
            "source_service",
            "deduplication_hash",
            "deduplication_window_start",
            unique=True,
            sqlite_where=text("idempotency_key IS NULL"),
            postgresql_where=text("idempotency_key IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    source_service: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    audience_type: Mapped[str] = mapped_column(String(32), nullable=False)
    audience: Mapped[dict[str, object]] = mapped_column(json_document, nullable=False)
    channels: Mapped[list[str]] = mapped_column(json_document, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    deduplication_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deduplication_window_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    recipient_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    delivery_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )

    recipients: Mapped[list[NotificationRecipientModel]] = relationship(
        back_populates="notification",
        cascade="all, delete-orphan",
    )
    deliveries: Mapped[list[NotificationDeliveryModel]] = relationship(
        back_populates="notification",
        cascade="all, delete-orphan",
    )


class NotificationRecipientModel(Base):
    __tablename__ = "notification_recipients"
    __table_args__ = (
        UniqueConstraint(
            "notification_id",
            "user_id",
            name="uq_notification_recipients_notification_user",
        ),
        Index(
            "ix_notification_recipients_user_id",
            "user_id",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("notification_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )

    notification: Mapped[NotificationRequestModel] = relationship(back_populates="recipients")
    user: Mapped[UserModel] = relationship(back_populates="recipients")
    deliveries: Mapped[list[NotificationDeliveryModel]] = relationship(
        back_populates="recipient",
        cascade="all, delete-orphan",
    )


class NotificationDeliveryModel(TimestampMixin, Base):
    __tablename__ = "notification_deliveries"
    __table_args__ = (
        CheckConstraint(
            "attempt_count >= 0 AND max_attempts > 0 AND attempt_count <= max_attempts",
            name="ck_notification_deliveries_attempt_bounds",
        ),
        CheckConstraint(
            "channel IN ('web', 'email')",
            name="ck_notification_deliveries_channel",
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'replay_requested', 'delivered', "
            "'failed_retryable', 'failed_terminal')",
            name="ck_notification_deliveries_status",
        ),
        Index(
            "ix_notification_deliveries_notification_id",
            "notification_id",
        ),
        UniqueConstraint(
            "notification_recipient_id",
            "channel",
            name="uq_notification_deliveries_recipient_channel",
        ),
        Index(
            "ix_notification_deliveries_me",
            "user_id",
            "channel",
            "status",
            "read_at",
            "delivered_at",
            "id",
        ),
        Index(
            "ix_notification_deliveries_worker",
            "status",
            "next_attempt_at",
            "lease_expires_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    notification_recipient_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("notification_recipients.id", ondelete="CASCADE"),
        nullable=False,
    )
    notification_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("notification_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
        server_default="3",
    )
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claimed_by: Mapped[str | None] = mapped_column(Text)
    claim_token: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    replay_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_message_id: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error_message: Mapped[str | None] = mapped_column(Text)

    recipient: Mapped[NotificationRecipientModel] = relationship(back_populates="deliveries")
    notification: Mapped[NotificationRequestModel] = relationship(back_populates="deliveries")
    user: Mapped[UserModel] = relationship(back_populates="deliveries")
    attempts: Mapped[list[DeliveryAttemptModel]] = relationship(
        back_populates="delivery",
        cascade="all, delete-orphan",
    )


class DeliveryAttemptModel(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (
        CheckConstraint(
            "attempt_number > 0",
            name="ck_delivery_attempts_positive_attempt_number",
        ),
        UniqueConstraint(
            "delivery_id",
            "attempt_number",
            name="uq_delivery_attempts_delivery_attempt_number",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    delivery_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("notification_deliveries.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    provider_message_id: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    delivery: Mapped[NotificationDeliveryModel] = relationship(back_populates="attempts")


class NotificationActionInvocationModel(Base):
    __tablename__ = "notification_action_invocations"
    __table_args__ = (
        CheckConstraint(
            "(result = 'queued' AND replay_id IS NOT NULL) OR "
            "(result = 'no_eligible' AND replay_id IS NULL)",
            name="ck_notification_action_invocations_replay_result",
        ),
        Index(
            "ix_notification_action_invocations_notification_id",
            "notification_id",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("notification_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    web_delivery_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("notification_deliveries.id", ondelete="SET NULL"),
    )
    requested_by_type: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_by_id: Mapped[str] = mapped_column(Text, nullable=False)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    result: Mapped[str] = mapped_column(String(32), nullable=False)
    replay_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    replayed_delivery_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
