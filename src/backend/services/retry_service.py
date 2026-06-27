from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from uuid import UUID, uuid4

from backend.db.models import (
    NotificationActionInvocationModel,
    NotificationDeliveryModel,
    NotificationRequestModel,
    OutboxEventModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    ActionInvocationResult,
    ActionType,
    OutboxEventStatus,
    RequestedByType,
)
from backend.domain.results import RetryResult

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class RetryService:
    def __init__(
        self,
        *,
        unit_of_work_factory: UnitOfWorkFactory,
        now: Callable[[], datetime],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now = now

    def retry_notification(
        self,
        *,
        notification_id: UUID,
        requested_by_type: RequestedByType,
        requested_by_id: str,
    ) -> RetryResult:
        requested_at = self._aware_utc_now()

        with self._unit_of_work_factory() as uow:
            notification = uow.notifications.get(notification_id)
            if notification is None:
                raise ValueError("notification does not exist")

            eligible_deliveries = uow.notifications.list_failed_retryable_deliveries(
                notification.id,
            )
            return self._record_retry(
                uow,
                notification=notification,
                eligible_deliveries=eligible_deliveries,
                requested_by_type=requested_by_type,
                requested_by_id=requested_by_id,
                requested_at=requested_at,
            )

    def retry_notification_for_service(
        self,
        *,
        notification_id: UUID,
        requested_by_id: str,
        can_retry_any: bool,
    ) -> RetryResult:
        requested_at = self._aware_utc_now()

        with self._unit_of_work_factory() as uow:
            notification = uow.notifications.get(notification_id)
            if notification is None:
                raise ValueError("notification does not exist")
            if not can_retry_any and notification.source_service != requested_by_id:
                raise PermissionError("service cannot retry notification")

            eligible_deliveries = uow.notifications.list_failed_retryable_deliveries(
                notification.id,
            )
            return self._record_retry(
                uow,
                notification=notification,
                eligible_deliveries=eligible_deliveries,
                requested_by_type=RequestedByType.SERVICE,
                requested_by_id=requested_by_id,
                requested_at=requested_at,
            )

    def retry_user_notification(
        self,
        *,
        web_notification_id: UUID,
        user_id: UUID,
    ) -> RetryResult:
        requested_at = self._aware_utc_now()

        with self._unit_of_work_factory() as uow:
            web_delivery = uow.notifications.get_visible_web_delivery_for_user(
                delivery_id=web_notification_id,
                user_id=user_id,
            )
            if web_delivery is None:
                raise ValueError("web notification does not exist")
            notification = uow.notifications.get(web_delivery.notification_id)
            if notification is None:
                raise ValueError("notification does not exist")

            eligible_deliveries = uow.notifications.list_user_retryable_sibling_deliveries(
                web_delivery,
            )
            return self._record_retry(
                uow,
                notification=notification,
                eligible_deliveries=eligible_deliveries,
                requested_by_type=RequestedByType.USER,
                requested_by_id=str(user_id),
                requested_at=requested_at,
                web_delivery_id=web_delivery.id,
            )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)

    def _record_retry(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        notification: NotificationRequestModel,
        eligible_deliveries: list[NotificationDeliveryModel],
        requested_by_type: RequestedByType,
        requested_by_id: str,
        requested_at: datetime,
        web_delivery_id: UUID | None = None,
    ) -> RetryResult:
        candidate_replay_id = uuid4() if eligible_deliveries else None
        replayed_delivery_ids = (
            uow.notifications.mark_deliveries_replay_requested(
                delivery_ids=[delivery.id for delivery in eligible_deliveries],
                replay_id=candidate_replay_id,
                requested_at=requested_at,
            )
            if candidate_replay_id is not None
            else []
        )
        replay_id = candidate_replay_id if replayed_delivery_ids else None
        result = (
            ActionInvocationResult.QUEUED
            if replayed_delivery_ids
            else ActionInvocationResult.NO_ELIGIBLE
        )

        if replay_id is not None:
            uow.outbox.add(
                self._replay_requested_event(
                    notification,
                    delivery_ids=replayed_delivery_ids,
                    replay_id=replay_id,
                    requested_at=requested_at,
                )
            )

        uow.notifications.add_action_invocation(
            NotificationActionInvocationModel(
                notification_id=notification.id,
                web_delivery_id=web_delivery_id,
                requested_by_type=requested_by_type.value,
                requested_by_id=requested_by_id,
                action_type=ActionType.RETRY.value,
                result=result.value,
                replay_id=replay_id,
                replayed_delivery_count=len(replayed_delivery_ids),
                created_at=requested_at,
            )
        )
        uow.commit()

        return RetryResult(
            notification_id=notification.id,
            status=result,
            replay_id=replay_id,
            replayed_delivery_count=len(replayed_delivery_ids),
            requested_at=requested_at,
        )

    def _replay_requested_event(
        self,
        notification: NotificationRequestModel,
        *,
        delivery_ids: list[UUID],
        replay_id: UUID,
        requested_at: datetime,
    ) -> OutboxEventModel:
        return OutboxEventModel(
            topic="notifications.requests",
            event_type="notification.replay_requested",
            aggregate_type="notification_request",
            aggregate_id=notification.id,
            event_key=str(replay_id),
            payload={
                "delivery_ids": [str(delivery_id) for delivery_id in delivery_ids],
                "notification_id": str(notification.id),
                "replay_id": str(replay_id),
            },
            status=OutboxEventStatus.PENDING.value,
            next_attempt_at=requested_at,
            created_at=requested_at,
            updated_at=requested_at,
        )
