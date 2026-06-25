from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import Protocol

from backend.core.metrics import PrometheusMetrics
from backend.db.models import OutboxEventModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import OutboxEventStatus
from backend.domain.results import OutboxPublisherResult

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class EventPublisher(Protocol):
    def publish(self, *, topic: str, key: str, payload: Mapping[str, object]) -> None:
        pass


class OutboxPublisher:
    def __init__(
        self,
        *,
        unit_of_work_factory: UnitOfWorkFactory,
        event_publisher: EventPublisher,
        now: Callable[[], datetime],
        worker_id: str,
        lease_duration: timedelta,
        retry_delay: timedelta,
        max_attempts: int,
        metrics: PrometheusMetrics | None = None,
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._event_publisher = event_publisher
        self._now = now
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._metrics = metrics

    def publish_due_events(self, *, limit: int) -> OutboxPublisherResult:
        now = self._aware_utc_now()
        lease_expires_at = now + self._lease_duration

        with self._unit_of_work_factory() as uow:
            events = uow.outbox.claim_due_events(
                now=now,
                limit=limit,
                worker_id=self._worker_id,
                lease_expires_at=lease_expires_at,
            )
            published_count = 0
            failed_retryable_count = 0
            failed_terminal_count = 0

            for event in events:
                result = self._publish_event(uow, event=event, now=now)
                self._record_outbox_event_metric(event=event, status=result)
                if result is OutboxEventStatus.PUBLISHED:
                    published_count += 1
                elif result is OutboxEventStatus.FAILED_RETRYABLE:
                    failed_retryable_count += 1
                elif result is OutboxEventStatus.FAILED_TERMINAL:
                    failed_terminal_count += 1

            self._record_oldest_pending_metric(uow=uow, now=now)
            uow.commit()

        return OutboxPublisherResult(
            claimed_count=len(events),
            published_count=published_count,
            failed_retryable_count=failed_retryable_count,
            failed_terminal_count=failed_terminal_count,
        )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)

    def _publish_event(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        event: OutboxEventModel,
        now: datetime,
    ) -> OutboxEventStatus:
        try:
            self._event_publisher.publish(
                topic=event.topic,
                key=event.event_key,
                payload=event.payload,
            )
        except Exception as exc:
            return uow.outbox.mark_publish_failed(
                event,
                failed_at=now,
                retry_at=now + self._retry_delay,
                error_message=str(exc),
                max_attempts=self._max_attempts,
            )

        uow.outbox.mark_published(event, published_at=now)
        return OutboxEventStatus.PUBLISHED

    def _record_outbox_event_metric(
        self,
        *,
        event: OutboxEventModel,
        status: OutboxEventStatus,
    ) -> None:
        if self._metrics is None:
            return
        self._metrics.record_outbox_event(event_type=event.event_type, status=status.value)

    def _record_oldest_pending_metric(self, *, uow: SqlAlchemyUnitOfWork, now: datetime) -> None:
        if self._metrics is None:
            return
        age_seconds = uow.outbox.oldest_publishable_event_age_seconds(now=now)
        self._metrics.set_outbox_oldest_pending_seconds(age_seconds)
