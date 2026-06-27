from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from uuid import UUID

from backend.db.models import (
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import AudienceType, Channel, DeliveryStatus
from backend.domain.errors import FanoutLimitExceeded
from backend.domain.results import NotificationFanoutResult
from backend.domain.value_objects import AudienceSelection
from backend.services.audience_service import AudienceResolutionService

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class NotificationFanoutService:
    def __init__(
        self,
        *,
        unit_of_work_factory: UnitOfWorkFactory,
        now: Callable[[], datetime],
        max_recipients: int = 10_000,
        max_deliveries: int = 20_000,
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now = now
        self._max_recipients = max_recipients
        self._max_deliveries = max_deliveries

    def fanout_notification(self, notification_id: UUID) -> NotificationFanoutResult:
        next_attempt_at = self._aware_utc_now()

        with self._unit_of_work_factory() as uow:
            notification = uow.notifications.get(notification_id)
            if notification is None:
                raise ValueError("notification does not exist")

            audience = self._audience_selection(notification)
            user_ids = AudienceResolutionService(uow.users).resolve(audience)
            recipient_ids_by_user = uow.notifications.list_recipient_ids_by_user(notification.id)
            delivery_pairs = uow.notifications.list_delivery_pairs(notification.id)
            channels = tuple(Channel(channel) for channel in notification.channels)

            requested_delivery_count = len(user_ids) * len(channels)
            if (
                len(user_ids) > self._max_recipients
                or requested_delivery_count > self._max_deliveries
            ):
                raise FanoutLimitExceeded(
                    recipients=len(user_ids),
                    deliveries=requested_delivery_count,
                )
            for user_id in user_ids:
                if user_id not in recipient_ids_by_user:
                    recipient = NotificationRecipientModel(
                        notification_id=notification.id,
                        user_id=user_id,
                    )
                    uow.notifications.add_recipient(recipient)
                    uow.session.flush()
                    recipient_ids_by_user[user_id] = recipient.id

                recipient_id = recipient_ids_by_user[user_id]
                for channel in channels:
                    delivery_pair = (user_id, channel)
                    if delivery_pair in delivery_pairs:
                        continue
                    delivery = NotificationDeliveryModel(
                        notification_recipient_id=recipient_id,
                        notification_id=notification.id,
                        user_id=user_id,
                        channel=channel.value,
                        status=DeliveryStatus.PENDING.value,
                        next_attempt_at=next_attempt_at,
                        created_at=next_attempt_at,
                        updated_at=next_attempt_at,
                    )
                    uow.notifications.add_delivery(delivery)
                    delivery_pairs.add(delivery_pair)

            notification.recipient_count = len(recipient_ids_by_user)
            notification.delivery_count = len(delivery_pairs)
            uow.commit()

            return NotificationFanoutResult(
                notification_id=notification.id,
                recipient_count=notification.recipient_count,
                delivery_count=notification.delivery_count,
                next_attempt_at=next_attempt_at,
            )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)

    def _audience_selection(self, notification: NotificationRequestModel) -> AudienceSelection:
        group = notification.audience.get("group")
        labels = notification.audience.get("labels")
        return AudienceSelection(
            type=AudienceType(notification.audience_type),
            group=group if isinstance(group, str) else None,
            labels=tuple(labels.items()) if isinstance(labels, dict) else None,
        )
