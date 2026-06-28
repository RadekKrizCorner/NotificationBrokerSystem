from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from backend.core.metrics import PrometheusMetrics
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import OutboxEventStatus
from backend.domain.results import OutboxPublisherResult

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class EventPublisher(Protocol):
    def publish(self, *, topic: str, key: str, payload: Mapping[str, object]) -> None:
        pass


@dataclass(frozen=True)
class ClaimedOutboxEvent:
    event_id: UUID
    claim_token: UUID
    topic: str
    event_type: str
    event_key: str
    payload: Mapping[str, object]

    occurred_at: datetime


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
            claims = [
                ClaimedOutboxEvent(
                    event_id=event.id,
                    claim_token=event.claim_token,
                    topic=event.topic,
                    event_type=event.event_type,
                    event_key=event.event_key,
                    payload=dict(event.payload),
                    occurred_at=event.created_at,
                )
                for event in events
                if event.claim_token is not None
            ]
            uow.commit()

        published_count = 0
        failed_retryable_count = 0
        failed_terminal_count = 0
        for claim in claims:
            result = self._publish_event(claim=claim, now=now)
            if result is None:
                continue
            self._record_outbox_event_metric(event_type=claim.event_type, status=result)
            if result is OutboxEventStatus.PUBLISHED:
                published_count += 1
            elif result is OutboxEventStatus.FAILED_RETRYABLE:
                failed_retryable_count += 1
            elif result is OutboxEventStatus.FAILED_TERMINAL:
                failed_terminal_count += 1

        self._record_oldest_pending_metric(now=now)
        return OutboxPublisherResult(
            claimed_count=len(claims),
            published_count=published_count,
            failed_retryable_count=failed_retryable_count,
            failed_terminal_count=failed_terminal_count,
        )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)

    def _event_envelope(self, claim: ClaimedOutboxEvent) -> Mapping[str, object]:
        occurred_at = claim.occurred_at
        if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
            occurred_at = occurred_at.replace(tzinfo=UTC)
        else:
            occurred_at = occurred_at.astimezone(UTC)

        return {
            "schema_version": 1,
            "event_id": str(claim.event_id),
            "event_type": claim.event_type,
            "occurred_at": occurred_at.isoformat(),
            "data": dict(claim.payload),
        }

    def _publish_event(
        self,
        *,
        claim: ClaimedOutboxEvent,
        now: datetime,
    ) -> OutboxEventStatus | None:
        try:
            self._event_publisher.publish(
                topic=claim.topic,
                key=claim.event_key,
                payload=self._event_envelope(claim),
            )
        except Exception as exc:
            return self._finalize_event(
                claim=claim,
                now=now,
                error_message=str(exc)[:1_000],
            )

        return self._finalize_event(claim=claim, now=now, error_message=None)

    def _finalize_event(
        self,
        *,
        claim: ClaimedOutboxEvent,
        now: datetime,
        error_message: str | None,
    ) -> OutboxEventStatus | None:
        with self._unit_of_work_factory() as uow:
            event = uow.outbox.get_claimed_event_for_update(
                event_id=claim.event_id,
                claim_token=claim.claim_token,
            )
            if event is None:
                return None

            if error_message is None:
                uow.outbox.mark_published(event, published_at=now)
                status = OutboxEventStatus.PUBLISHED
            else:
                status = uow.outbox.mark_publish_failed(
                    event,
                    failed_at=now,
                    retry_at=now + self._retry_delay,
                    error_message=error_message,
                    max_attempts=self._max_attempts,
                )
            uow.commit()
            return status

    def _record_outbox_event_metric(
        self,
        *,
        event_type: str,
        status: OutboxEventStatus,
    ) -> None:
        if self._metrics is None:
            return
        self._metrics.record_outbox_event(event_type=event_type, status=status.value)

    def _record_oldest_pending_metric(self, *, now: datetime) -> None:
        if self._metrics is None:
            return
        with self._unit_of_work_factory() as uow:
            age_seconds = uow.outbox.oldest_publishable_event_age_seconds(now=now)
        self._metrics.set_outbox_oldest_pending_seconds(age_seconds)
