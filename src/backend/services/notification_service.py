from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta

from backend.db.models import NotificationRequestModel, OutboxEventModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    NotificationCreateResultStatus,
    NotificationRequestStatus,
    OutboxEventStatus,
)
from backend.domain.results import NotificationCreateResult
from backend.domain.value_objects import NotificationCreationInput
from backend.services.intake.idempotency import (
    canonical_audience,
    deduplication_window_start,
    fallback_deduplication_hash,
    payload_fingerprint,
)

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class NotificationCreationService:
    def __init__(
        self,
        *,
        unit_of_work_factory: UnitOfWorkFactory,
        now: Callable[[], datetime],
        fallback_deduplication_window: timedelta,
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now = now
        self._fallback_deduplication_window = fallback_deduplication_window

    def create_notification(
        self,
        *,
        source_service: str,
        request: NotificationCreationInput,
        idempotency_key: str | None,
    ) -> NotificationCreateResult:
        now = self._aware_utc_now()
        fingerprint = payload_fingerprint(request)
        deduplication_hash: str | None = None
        window_start: datetime | None = None

        with self._unit_of_work_factory() as uow:
            existing = None
            if idempotency_key is not None:
                existing = uow.notifications.find_by_explicit_idempotency_key(
                    source_service=source_service,
                    idempotency_key=idempotency_key,
                )
            else:
                window_start = deduplication_window_start(
                    now,
                    window=self._fallback_deduplication_window,
                )
                deduplication_hash = fallback_deduplication_hash(
                    request,
                    source_service=source_service,
                    now=now,
                    window=self._fallback_deduplication_window,
                )
                existing = uow.notifications.find_by_fallback_hash(
                    source_service=source_service,
                    deduplication_hash=deduplication_hash,
                    deduplication_window_start=window_start,
                )

            if existing is not None:
                return NotificationCreateResult(
                    notification_id=existing.id,
                    status=NotificationCreateResultStatus.EXISTING,
                )

            notification = NotificationRequestModel(
                source_service=source_service,
                message=request.message,
                severity=request.severity.value,
                audience_type=request.audience.type,
                audience=canonical_audience(request.audience),
                channels=[channel.value for channel in request.channels],
                status=NotificationRequestStatus.ACCEPTED.value,
                idempotency_key=idempotency_key,
                payload_fingerprint=fingerprint,
                deduplication_hash=deduplication_hash,
                deduplication_window_start=window_start,
                created_at=now,
                updated_at=now,
            )
            uow.notifications.add(notification)
            uow.session.flush()
            uow.outbox.add(self._notification_requested_event(notification, now=now))
            uow.commit()

            return NotificationCreateResult(
                notification_id=notification.id,
                status=NotificationCreateResultStatus.CREATED,
            )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)

    def _notification_requested_event(
        self,
        notification: NotificationRequestModel,
        *,
        now: datetime,
    ) -> OutboxEventModel:
        return OutboxEventModel(
            topic="notifications.requests",
            event_type="notification.requested",
            aggregate_type="notification_request",
            aggregate_id=notification.id,
            event_key=str(notification.id),
            payload={
                "notification_id": str(notification.id),
                "source_service": notification.source_service,
            },
            status=OutboxEventStatus.PENDING.value,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
