from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Text, UniqueConstraint, true
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Uuid

from backend.db.models.base import Base, TimestampMixin, utc_now

if TYPE_CHECKING:
    from backend.db.models.notifications import (
        NotificationDeliveryModel,
        NotificationRecipientModel,
    )


class UserModel(TimestampMixin, Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=true(),
    )

    recipients: Mapped[list[NotificationRecipientModel]] = relationship(back_populates="user")
    deliveries: Mapped[list[NotificationDeliveryModel]] = relationship(back_populates="user")


class GroupModel(Base):
    __tablename__ = "groups"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class UserGroupModel(Base):
    __tablename__ = "user_groups"
    __table_args__ = (
        Index("ix_user_groups_lookup", "group_id", "user_id"),
    )

    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    group_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("groups.id", ondelete="CASCADE"),
        primary_key=True,
    )


class UserLabelModel(Base):
    __tablename__ = "user_labels"
    __table_args__ = (
        UniqueConstraint("user_id", "key", name="uq_user_labels_user_key"),
        Index("ix_user_labels_lookup", "key", "value", "user_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    key: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
