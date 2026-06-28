from datetime import datetime
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import Select, and_, or_, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, joinedload

from backend.db.models import (
    DeliveryAttemptModel,
    NotificationActionInvocationModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
)
from backend.domain.enums import Channel, DeliveryStatus, Severity, WebNotificationStatus
from backend.domain.read_models import WebNotificationRow


class NotificationRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, notification: NotificationRequestModel) -> None:
        self._session.add(notification)

    def add_recipient(self, recipient: NotificationRecipientModel) -> None:
        self._session.add(recipient)

    def add_delivery(self, delivery: NotificationDeliveryModel) -> None:
        self._session.add(delivery)

    def add_delivery_attempt(self, attempt: DeliveryAttemptModel) -> None:
        self._session.add(attempt)

    def add_action_invocation(self, invocation: NotificationActionInvocationModel) -> None:
        self._session.add(invocation)

    def add_recipients_ignore_conflicts(
        self,
        *,
        notification_id: UUID,
        user_ids: list[UUID],
        created_at: datetime,
    ) -> None:
        if not user_ids:
            return
        values = [
            {
                "id": uuid4(),
                "notification_id": notification_id,
                "user_id": user_id,
                "created_at": created_at,
            }
            for user_id in user_ids
        ]
        dialect_name = self._session.get_bind().dialect.name
        if dialect_name == "postgresql":
            pg_statement = postgresql_insert(NotificationRecipientModel).values(values)
            self._session.execute(
                pg_statement.on_conflict_do_nothing(
                    index_elements=["notification_id", "user_id"],
                )
            )
            return
        if dialect_name == "sqlite":
            sqlite_statement = sqlite_insert(NotificationRecipientModel).values(values)
            self._session.execute(
                sqlite_statement.on_conflict_do_nothing(
                    index_elements=["notification_id", "user_id"],
                )
            )
            return
        raise RuntimeError(f"unsupported database dialect: {dialect_name}")

    def add_deliveries_ignore_conflicts(
        self,
        *,
        values: list[dict[str, object]],
    ) -> None:
        if not values:
            return
        dialect_name = self._session.get_bind().dialect.name
        if dialect_name == "postgresql":
            pg_statement = postgresql_insert(NotificationDeliveryModel).values(values)
            self._session.execute(
                pg_statement.on_conflict_do_nothing(
                    index_elements=["notification_recipient_id", "channel"],
                )
            )
            return
        if dialect_name == "sqlite":
            sqlite_statement = sqlite_insert(NotificationDeliveryModel).values(values)
            self._session.execute(
                sqlite_statement.on_conflict_do_nothing(
                    index_elements=["notification_recipient_id", "channel"],
                )
            )
            return
        raise RuntimeError(f"unsupported database dialect: {dialect_name}")

    def get(self, notification_id: UUID) -> NotificationRequestModel | None:
        return self._session.get(NotificationRequestModel, notification_id)

    def find_by_explicit_idempotency_key(
        self,
        *,
        source_service: str,
        idempotency_key: str,
    ) -> NotificationRequestModel | None:
        same_source_service = NotificationRequestModel.source_service == source_service
        same_idempotency_key = NotificationRequestModel.idempotency_key == idempotency_key

        statement = select(NotificationRequestModel).where(
            same_source_service,
            same_idempotency_key,
        )
        return self._session.scalar(statement)

    def find_by_fallback_hash(
        self,
        *,
        source_service: str,
        deduplication_hash: str,
        deduplication_window_start: datetime,
    ) -> NotificationRequestModel | None:
        same_source_service = NotificationRequestModel.source_service == source_service
        no_explicit_idempotency_key = NotificationRequestModel.idempotency_key.is_(None)
        same_deduplication_hash = NotificationRequestModel.deduplication_hash == deduplication_hash
        same_deduplication_window = (
            NotificationRequestModel.deduplication_window_start == deduplication_window_start
        )

        statement = select(NotificationRequestModel).where(
            same_source_service,
            no_explicit_idempotency_key,
            same_deduplication_hash,
            same_deduplication_window,
        )
        return self._session.scalar(statement)

    def list_recipient_ids_by_user(self, notification_id: UUID) -> dict[UUID, UUID]:
        same_notification = NotificationRecipientModel.notification_id == notification_id

        statement = select(
            NotificationRecipientModel.user_id,
            NotificationRecipientModel.id,
        ).where(same_notification)
        return {
            user_id: recipient_id
            for user_id, recipient_id in self._session.execute(statement).all()
        }

    def list_delivery_pairs(self, notification_id: UUID) -> set[tuple[UUID, Channel]]:
        same_notification = NotificationDeliveryModel.notification_id == notification_id

        statement = select(
            NotificationDeliveryModel.user_id,
            NotificationDeliveryModel.channel,
        ).where(same_notification)
        return {
            (user_id, Channel(channel))
            for user_id, channel in self._session.execute(statement).all()
        }

    def list_failed_retryable_deliveries(
        self,
        notification_id: UUID,
    ) -> list[NotificationDeliveryModel]:
        same_notification = NotificationDeliveryModel.notification_id == notification_id
        retryable_failure = (
            NotificationDeliveryModel.status == DeliveryStatus.FAILED_RETRYABLE.value
        )

        statement = (
            select(NotificationDeliveryModel)
            .where(same_notification, retryable_failure)
            .order_by(NotificationDeliveryModel.id)
        )
        return list(self._session.scalars(statement))

    def mark_deliveries_replay_requested(
        self,
        *,
        delivery_ids: list[UUID],
        replay_id: UUID,
        requested_at: datetime,
    ) -> list[UUID]:
        if not delivery_ids:
            return []

        statement = (
            update(NotificationDeliveryModel)
            .where(
                NotificationDeliveryModel.id.in_(delivery_ids),
                NotificationDeliveryModel.status == DeliveryStatus.FAILED_RETRYABLE.value,
            )
            .values(
                status=DeliveryStatus.REPLAY_REQUESTED.value,
                replay_id=replay_id,
                next_attempt_at=requested_at,
                processing_started_at=None,
                lease_expires_at=None,
                claimed_by=None,
                updated_at=requested_at,
                claim_token=None,
            )
            .returning(NotificationDeliveryModel.id)
            .execution_options(synchronize_session=False)
        )
        return list(self._session.scalars(statement))

    def claim_due_deliveries(
        self,
        *,
        now: datetime,
        limit: int,
        worker_id: str,
        lease_expires_at: datetime,
        channels: tuple[Channel, ...] | None = None,
    ) -> list[NotificationDeliveryModel]:
        if limit <= 0:
            return []

        ready_to_process = and_(
            NotificationDeliveryModel.status.in_(
                [
                    DeliveryStatus.PENDING.value,
                    DeliveryStatus.REPLAY_REQUESTED.value,
                    DeliveryStatus.FAILED_RETRYABLE.value,
                ]
            ),
            NotificationDeliveryModel.next_attempt_at <= now,
        )
        expired_processing_lease = and_(
            NotificationDeliveryModel.status == DeliveryStatus.PROCESSING.value,
            NotificationDeliveryModel.lease_expires_at < now,
        )
        predicates = [or_(ready_to_process, expired_processing_lease)]
        if channels is not None:
            predicates.append(
                NotificationDeliveryModel.channel.in_([channel.value for channel in channels])
            )

        statement = (
            select(NotificationDeliveryModel)
            .where(*predicates)
            .order_by(NotificationDeliveryModel.next_attempt_at, NotificationDeliveryModel.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        deliveries = list(self._session.scalars(statement))
        for delivery in deliveries:
            delivery.status = DeliveryStatus.PROCESSING.value
            delivery.processing_started_at = now
            delivery.lease_expires_at = lease_expires_at
            delivery.claimed_by = worker_id
            delivery.claim_token = uuid4()
            delivery.updated_at = now
        return deliveries

    def get_claimed_delivery_context(
        self,
        *,
        delivery_id: UUID,
        claim_token: UUID,
    ) -> NotificationDeliveryModel | None:
        statement = (
            select(NotificationDeliveryModel)
            .options(
                joinedload(NotificationDeliveryModel.notification),
                joinedload(NotificationDeliveryModel.user),
            )
            .where(
                NotificationDeliveryModel.id == delivery_id,
                NotificationDeliveryModel.status == DeliveryStatus.PROCESSING.value,
                NotificationDeliveryModel.claim_token == claim_token,
            )
        )
        return self._session.scalar(statement)

    def get_claimed_delivery_for_update(
        self,
        *,
        delivery_id: UUID,
        claim_token: UUID,
    ) -> NotificationDeliveryModel | None:
        statement = (
            select(NotificationDeliveryModel)
            .where(
                NotificationDeliveryModel.id == delivery_id,
                NotificationDeliveryModel.status == DeliveryStatus.PROCESSING.value,
                NotificationDeliveryModel.claim_token == claim_token,
            )
            .with_for_update()
        )
        return self._session.scalar(statement)

    def get_visible_web_delivery_for_user(
        self,
        *,
        delivery_id: UUID,
        user_id: UUID,
    ) -> NotificationDeliveryModel | None:
        same_delivery = NotificationDeliveryModel.id == delivery_id
        same_user = NotificationDeliveryModel.user_id == user_id
        web_channel = NotificationDeliveryModel.channel == Channel.WEB.value
        delivered = NotificationDeliveryModel.status == DeliveryStatus.DELIVERED.value
        has_delivery_timestamp = NotificationDeliveryModel.delivered_at.is_not(None)

        statement = select(NotificationDeliveryModel).where(
            same_delivery,
            same_user,
            web_channel,
            delivered,
            has_delivery_timestamp,
        )
        return self._session.scalar(statement)

    def list_user_retryable_sibling_deliveries(
        self,
        web_delivery: NotificationDeliveryModel,
    ) -> list[NotificationDeliveryModel]:
        same_recipient = (
            NotificationDeliveryModel.notification_recipient_id
            == web_delivery.notification_recipient_id
        )
        same_notification = (
            NotificationDeliveryModel.notification_id == web_delivery.notification_id
        )
        same_user = NotificationDeliveryModel.user_id == web_delivery.user_id
        not_web_channel = NotificationDeliveryModel.channel != Channel.WEB.value
        retryable_failure = (
            NotificationDeliveryModel.status == DeliveryStatus.FAILED_RETRYABLE.value
        )

        statement = (
            select(NotificationDeliveryModel)
            .where(
                same_recipient,
                same_notification,
                same_user,
                not_web_channel,
                retryable_failure,
            )
            .order_by(NotificationDeliveryModel.id)
        )
        return list(self._session.scalars(statement))

    def mark_visible_web_notification_read(
        self,
        *,
        delivery_id: UUID,
        user_id: UUID,
        read_at: datetime,
    ) -> NotificationDeliveryModel | None:
        delivery = self.get_visible_web_delivery_for_user(
            delivery_id=delivery_id,
            user_id=user_id,
        )
        if delivery is None:
            return None
        if delivery.read_at is None:
            delivery.read_at = read_at
            delivery.updated_at = read_at
        return delivery

    def list_web_notifications_for_user(
        self,
        *,
        user_id: UUID,
        status: WebNotificationStatus,
        limit: int,
        after: tuple[datetime, UUID] | None = None,
    ) -> list[WebNotificationRow]:
        statement = self._visible_web_notifications_statement(user_id=user_id, status=status)

        if after is not None:
            delivered_at, delivery_id = after
            older_delivery_timestamp = NotificationDeliveryModel.delivered_at < delivered_at
            same_timestamp_earlier_id = and_(
                NotificationDeliveryModel.delivered_at == delivered_at,
                NotificationDeliveryModel.id < delivery_id,
            )
            after_cursor = or_(older_delivery_timestamp, same_timestamp_earlier_id)
            statement = statement.where(after_cursor)

        statement = statement.order_by(
            NotificationDeliveryModel.delivered_at.desc(),
            NotificationDeliveryModel.id.desc(),
        ).limit(limit)

        return [
            WebNotificationRow(
                id=row.id,
                notification_id=row.notification_id,
                message=row.message,
                severity=Severity(row.severity),
                read_at=row.read_at,
                delivered_at=cast(datetime, row.delivered_at),
                created_at=row.created_at,
            )
            for row in self._session.execute(statement)
        ]

    def _visible_web_notifications_statement(
        self,
        *,
        user_id: UUID,
        status: WebNotificationStatus,
    ) -> Select[tuple[UUID, UUID, str, str, datetime | None, datetime | None, datetime]]:
        same_user = NotificationDeliveryModel.user_id == user_id
        web_channel = NotificationDeliveryModel.channel == Channel.WEB.value
        delivered = NotificationDeliveryModel.status == DeliveryStatus.DELIVERED.value
        has_delivery_timestamp = NotificationDeliveryModel.delivered_at.is_not(None)

        statement = (
            select(
                NotificationDeliveryModel.id,
                NotificationDeliveryModel.notification_id,
                NotificationRequestModel.message,
                NotificationRequestModel.severity,
                NotificationDeliveryModel.read_at,
                NotificationDeliveryModel.delivered_at,
                NotificationRequestModel.created_at,
            )
            .join(
                NotificationRequestModel,
                NotificationRequestModel.id == NotificationDeliveryModel.notification_id,
            )
            .where(
                same_user,
                web_channel,
                delivered,
                has_delivery_timestamp,
            )
        )

        if status is WebNotificationStatus.UNREAD:
            statement = statement.where(NotificationDeliveryModel.read_at.is_(None))
        elif status is WebNotificationStatus.READ:
            statement = statement.where(NotificationDeliveryModel.read_at.is_not(None))
        elif status is not WebNotificationStatus.ALL:
            raise ValueError("status must be a WebNotificationStatus")

        return statement
