from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from uuid import UUID, uuid4

from backend.db.models import NotificationRequestModel
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
            recipient_ids_by_user = uow.notifications.list_recipient_ids_by_user(notification.id)
            missing_user_ids = [
                user_id for user_id in user_ids if user_id not in recipient_ids_by_user
            ]
            uow.notifications.add_recipients_ignore_conflicts(
                notification_id=notification.id,
                user_ids=missing_user_ids,
                created_at=next_attempt_at,
            )
            recipient_ids_by_user = uow.notifications.list_recipient_ids_by_user(notification.id)
            delivery_pairs = uow.notifications.list_delivery_pairs(notification.id)
            delivery_values: list[dict[str, object]] = []
            for user_id in user_ids:
                recipient_id = recipient_ids_by_user[user_id]
                for channel in channels:
                    if (user_id, channel) in delivery_pairs:
                        continue
                    delivery_values.append(
                        {
                            "id": uuid4(),
                            "notification_recipient_id": recipient_id,
                            "notification_id": notification.id,
                            "user_id": user_id,
                            "channel": channel.value,
                            "status": DeliveryStatus.PENDING.value,
                            "attempt_count": 0,
                            "max_attempts": 3,
                            "next_attempt_at": next_attempt_at,
                            "created_at": next_attempt_at,
                            "updated_at": next_attempt_at,
                        }
                    )
            uow.notifications.add_deliveries_ignore_conflicts(values=delivery_values)
            delivery_pairs = uow.notifications.list_delivery_pairs(notification.id)

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
