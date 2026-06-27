import json
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import (
    Base,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    ProcessedEventModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.results import NotificationFanoutResult
from workers.kafka.consumer import (
    InvalidNotificationKafkaMessage,
    KafkaRawMessage,
    NotificationKafkaMessage,
)
from workers.notifications.consumer import NotificationConsumerWorker

SessionFactory = sessionmaker[Session]


@pytest.fixture()
def session_factory() -> Iterator[SessionFactory]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection: Any, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


class RecordingNotificationEventConsumer:
    def __init__(
        self, messages: list[NotificationKafkaMessage | InvalidNotificationKafkaMessage]
    ) -> None:
        self.messages = messages
        self.commits = 0
        self.poll_timeouts: list[float] = []

    def consume_one(
        self, *, timeout_seconds: float
    ) -> NotificationKafkaMessage | InvalidNotificationKafkaMessage | None:
        self.poll_timeouts.append(timeout_seconds)
        if not self.messages:
            return None
        return self.messages.pop(0)

    def commit(self) -> None:
        self.commits += 1


class RecordingNotificationRequestedHandler:
    def __init__(self, *, result: NotificationFanoutResult | None = None) -> None:
        self.result = result
        self.payloads: list[Mapping[str, object]] = []

    def handle(self, payload: Mapping[str, object]) -> NotificationFanoutResult:
        self.payloads.append(payload)
        if self.result is not None:
            return self.result
        notification_id = UUID(str(payload["notification_id"]))
        return NotificationFanoutResult(
            notification_id=notification_id,
            recipient_count=1,
            delivery_count=2,
            next_attempt_at=NotificationConsumerFixtures.now,
        )


class FailingNotificationRequestedHandler:
    def handle(self, payload: Mapping[str, object]) -> NotificationFanoutResult:
        raise RuntimeError("fanout failed")


class RecordingDeadLetterPublisher:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[InvalidNotificationKafkaMessage] = []

    def publish(self, message: InvalidNotificationKafkaMessage) -> None:
        if self.fail:
            raise RuntimeError("DLQ unavailable")
        self.messages.append(message)


class NotificationConsumerFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def unit_of_work_factory(session_factory: SessionFactory) -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    @staticmethod
    def message(notification_id: UUID | None = None) -> NotificationKafkaMessage:
        event_id = notification_id or uuid4()
        return NotificationKafkaMessage(
            key=str(event_id),
            event_id=event_id,
            event_type="notification.requested",
            occurred_at=NotificationConsumerFixtures.now,
            payload={"notification_id": str(event_id), "source_service": "billing"},
            raw_message=KafkaRawMessage(
                key=str(event_id).encode(),
                value=json.dumps({"schema_version": 1, "event_id": str(event_id)}).encode(),
            ),
        )

    @staticmethod
    def worker(
        session_factory: SessionFactory,
        *,
        event_consumer: RecordingNotificationEventConsumer,
        handler: RecordingNotificationRequestedHandler | FailingNotificationRequestedHandler,
        dead_letter_publisher: RecordingDeadLetterPublisher | None = None,
    ) -> NotificationConsumerWorker:
        return NotificationConsumerWorker(
            event_consumer=event_consumer,
            handler=handler,
            dead_letter_publisher=dead_letter_publisher or RecordingDeadLetterPublisher(),
            unit_of_work_factory=lambda: NotificationConsumerFixtures.unit_of_work_factory(
                session_factory
            ),
            consumer_name="notification-requested-consumer",
            now=lambda: NotificationConsumerFixtures.now,
            batch_size=10,
            poll_timeout_seconds=1.5,
        )

    @staticmethod
    def mark_processed(session_factory: SessionFactory, *, event_id: UUID) -> None:
        with session_factory() as session:
            session.add(
                ProcessedEventModel(
                    event_id=event_id,
                    consumer_name="notification-requested-consumer",
                    processed_at=NotificationConsumerFixtures.now,
                )
            )
            session.commit()


class TestNotificationConsumerWorker:
    def test_run_once_handles_message_records_processed_event_and_commits_offset(
        self,
        session_factory: SessionFactory,
    ) -> None:
        message = NotificationConsumerFixtures.message()
        event_consumer = RecordingNotificationEventConsumer([message])
        handler = RecordingNotificationRequestedHandler()
        worker = NotificationConsumerFixtures.worker(
            session_factory,
            event_consumer=event_consumer,
            handler=handler,
        )

        result = worker.run_once()

        assert result.received_count == 1
        assert result.processed_count == 1
        assert result.duplicate_count == 0
        assert event_consumer.commits == 1
        assert event_consumer.poll_timeouts == [1.5, 1.5]
        assert handler.payloads == [message.payload]

        with session_factory() as session:
            processed_event = session.get(
                ProcessedEventModel,
                (UUID(message.key), "notification-requested-consumer"),
            )
            assert processed_event is not None
            assert processed_event.processed_at == NotificationConsumerFixtures.now.replace(
                tzinfo=None
            )

    def test_run_once_commits_duplicate_message_without_calling_handler(
        self,
        session_factory: SessionFactory,
    ) -> None:
        message = NotificationConsumerFixtures.message()
        NotificationConsumerFixtures.mark_processed(
            session_factory,
            event_id=UUID(message.key),
        )
        event_consumer = RecordingNotificationEventConsumer([message])
        handler = RecordingNotificationRequestedHandler()
        worker = NotificationConsumerFixtures.worker(
            session_factory,
            event_consumer=event_consumer,
            handler=handler,
        )

        result = worker.run_once()

        assert result.received_count == 1
        assert result.processed_count == 0
        assert result.duplicate_count == 1
        assert event_consumer.commits == 1
        assert handler.payloads == []

    def test_run_once_does_not_commit_offset_when_handler_fails(
        self,
        session_factory: SessionFactory,
    ) -> None:
        message = NotificationConsumerFixtures.message()
        event_consumer = RecordingNotificationEventConsumer([message])
        worker = NotificationConsumerFixtures.worker(
            session_factory,
            event_consumer=event_consumer,
            handler=FailingNotificationRequestedHandler(),
        )

        with pytest.raises(RuntimeError, match="fanout failed"):
            worker.run_once()

        assert event_consumer.commits == 0
        with session_factory() as session:
            assert session.scalar(select(func.count(ProcessedEventModel.event_id))) == 0
            assert session.scalar(select(func.count(NotificationRecipientModel.id))) == 0
            assert session.scalar(select(func.count(NotificationDeliveryModel.id))) == 0

    def test_run_once_dead_letters_invalid_message_before_committing(
        self,
        session_factory: SessionFactory,
    ) -> None:
        raw_message = KafkaRawMessage(
            key=b"poison",
            value=b"not-json",
        )
        message = InvalidNotificationKafkaMessage(
            raw_message=raw_message,
            error_code="invalid_json",
            error_message="message is not valid JSON",
        )
        event_consumer = RecordingNotificationEventConsumer([message])
        dead_letter_publisher = RecordingDeadLetterPublisher()
        worker = NotificationConsumerFixtures.worker(
            session_factory,
            event_consumer=event_consumer,
            handler=RecordingNotificationRequestedHandler(),
            dead_letter_publisher=dead_letter_publisher,
        )

        result = worker.run_once()

        assert result.received_count == 1
        assert result.processed_count == 0
        assert result.dead_lettered_count == 1
        assert result.committed_count == 1
        assert event_consumer.commits == 1
        assert dead_letter_publisher.messages == [message]

    def test_run_once_does_not_commit_when_dead_letter_publish_fails(
        self,
        session_factory: SessionFactory,
    ) -> None:
        message = InvalidNotificationKafkaMessage(
            raw_message=KafkaRawMessage(key=b"poison", value=b"not-json"),
            error_code="invalid_json",
            error_message="message is not valid JSON",
        )
        event_consumer = RecordingNotificationEventConsumer([message])
        dead_letter_publisher = RecordingDeadLetterPublisher(fail=True)
        worker = NotificationConsumerFixtures.worker(
            session_factory,
            event_consumer=event_consumer,
            handler=RecordingNotificationRequestedHandler(),
            dead_letter_publisher=dead_letter_publisher,
        )

        with pytest.raises(RuntimeError, match="DLQ unavailable"):
            worker.run_once()

        assert event_consumer.commits == 0
        assert dead_letter_publisher.messages == []
