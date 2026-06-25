from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.results import NotificationConsumerResult, NotificationFanoutResult
from workers.kafka.consumer import NotificationKafkaMessage

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class NotificationEventConsumer(Protocol):
    def consume_one(self, *, timeout_seconds: float) -> NotificationKafkaMessage | None:
        pass

    def commit(self) -> None:
        pass


class NotificationRequestedEventHandler(Protocol):
    def handle(self, payload: Mapping[str, object]) -> NotificationFanoutResult:
        pass


class NotificationConsumerWorker:
    def __init__(
        self,
        *,
        event_consumer: NotificationEventConsumer,
        handler: NotificationRequestedEventHandler,
        unit_of_work_factory: UnitOfWorkFactory,
        consumer_name: str,
        now: Callable[[], datetime],
        batch_size: int,
        poll_timeout_seconds: float,
    ) -> None:
        self._event_consumer = event_consumer
        self._handler = handler
        self._unit_of_work_factory = unit_of_work_factory
        self._consumer_name = consumer_name
        self._now = now
        self._batch_size = batch_size
        self._poll_timeout_seconds = poll_timeout_seconds

    def run_once(self) -> NotificationConsumerResult:
        received_count = 0
        processed_count = 0
        duplicate_count = 0
        committed_count = 0

        for _ in range(self._batch_size):
            message = self._event_consumer.consume_one(
                timeout_seconds=self._poll_timeout_seconds,
            )
            if message is None:
                break

            received_count += 1
            event_id = self._event_id(message)

            if self._already_processed(event_id):
                duplicate_count += 1
                self._event_consumer.commit()
                committed_count += 1
                continue

            self._handler.handle(message.payload)
            self._mark_processed(event_id)
            processed_count += 1
            self._event_consumer.commit()
            committed_count += 1

        return NotificationConsumerResult(
            received_count=received_count,
            processed_count=processed_count,
            duplicate_count=duplicate_count,
            committed_count=committed_count,
        )

    def _event_id(self, message: NotificationKafkaMessage) -> UUID:
        return UUID(message.key)

    def _already_processed(self, event_id: UUID) -> bool:
        with self._unit_of_work_factory() as uow:
            already_processed = uow.processed_events.exists(
                event_id=event_id,
                consumer_name=self._consumer_name,
            )
            uow.commit()
            return already_processed

    def _mark_processed(self, event_id: UUID) -> None:
        now = self._aware_utc_now()
        with self._unit_of_work_factory() as uow:
            uow.processed_events.add(
                event_id=event_id,
                consumer_name=self._consumer_name,
                processed_at=now,
            )
            uow.commit()

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)
