from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.core.config import Settings
from backend.db.models import Base, OutboxEventModel
from backend.domain.enums import Channel, OutboxEventStatus
from workers.delivery.worker import DeliveryWorker
from workers.factory import DeliveryWorkerFactory, OutboxWorkerFactory

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


class RecordingEventPublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, Mapping[str, object]]] = []

    def publish(self, *, topic: str, key: str, payload: Mapping[str, object]) -> None:
        self.events.append((topic, key, payload))


class FailingEventPublisher:
    def publish(self, *, topic: str, key: str, payload: Mapping[str, object]) -> None:
        raise RuntimeError("broker unavailable")


class WorkerFactoryFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def settings() -> Settings:
        return Settings(
            database_url="sqlite+pysqlite:///:memory:",
            jwt_secret="test-secret-long-enough",
            kafka_bootstrap_servers="redpanda:9092",
            kafka_client_id="notification-test",
            outbox_worker_id="factory-worker",
            outbox_lease_seconds=45,
            outbox_retry_delay_seconds=17,
            outbox_max_attempts=3,
            outbox_publish_batch_size=25,
        )

    @staticmethod
    def seed_event(
        session_factory: SessionFactory,
        *,
        attempts: int = 0,
    ) -> UUID:
        event_id = uuid4()
        with session_factory() as session:
            session.add(
                OutboxEventModel(
                    id=event_id,
                    topic="notifications.requests",
                    event_type="notification.requested",
                    aggregate_type="notification_request",
                    aggregate_id=uuid4(),
                    event_key="notification-1",
                    payload={"notification_id": "notification-1", "source_service": "billing"},
                    status=OutboxEventStatus.PENDING.value,
                    attempts=attempts,
                    next_attempt_at=WorkerFactoryFixtures.now,
                    created_at=WorkerFactoryFixtures.now,
                    updated_at=WorkerFactoryFixtures.now,
                )
            )
            session.commit()
        return event_id


class TestOutboxWorkerFactory:
    def test_create_outbox_publisher_uses_injected_event_publisher(
        self,
        session_factory: SessionFactory,
    ) -> None:
        event_id = WorkerFactoryFixtures.seed_event(session_factory)
        event_publisher = RecordingEventPublisher()
        factory = OutboxWorkerFactory(
            settings=WorkerFactoryFixtures.settings(),
            session_factory=session_factory,
            event_publisher=event_publisher,
            now=lambda: WorkerFactoryFixtures.now,
        )

        publisher = factory.create_outbox_publisher()
        result = publisher.publish_due_events(limit=10)

        assert result.published_count == 1
        assert event_publisher.events == [
            (
                "notifications.requests",
                "notification-1",
                {
                    "schema_version": 1,
                    "event_id": str(event_id),
                    "event_type": "notification.requested",
                    "occurred_at": WorkerFactoryFixtures.now.isoformat(),
                    "data": {
                        "notification_id": "notification-1",
                        "source_service": "billing",
                    },
                },
            )
        ]

        with session_factory() as session:
            event = session.get(OutboxEventModel, event_id)
            assert event is not None
            assert event.status == OutboxEventStatus.PUBLISHED.value

    def test_create_outbox_publisher_uses_retry_settings(
        self,
        session_factory: SessionFactory,
    ) -> None:
        event_id = WorkerFactoryFixtures.seed_event(session_factory)
        factory = OutboxWorkerFactory(
            settings=WorkerFactoryFixtures.settings(),
            session_factory=session_factory,
            event_publisher=FailingEventPublisher(),
            now=lambda: WorkerFactoryFixtures.now,
        )

        publisher = factory.create_outbox_publisher()
        result = publisher.publish_due_events(limit=10)

        assert result.failed_retryable_count == 1
        with session_factory() as session:
            event = session.get(OutboxEventModel, event_id)
            assert event is not None
            assert event.status == OutboxEventStatus.FAILED_RETRYABLE.value
            assert event.next_attempt_at == (
                WorkerFactoryFixtures.now + timedelta(seconds=17)
            ).replace(tzinfo=None)

    def test_create_outbox_worker_uses_configured_batch_size(
        self,
        session_factory: SessionFactory,
    ) -> None:
        WorkerFactoryFixtures.seed_event(session_factory)
        WorkerFactoryFixtures.seed_event(session_factory)
        event_publisher = RecordingEventPublisher()
        settings = WorkerFactoryFixtures.settings().model_copy(
            update={"outbox_publish_batch_size": 1}
        )
        factory = OutboxWorkerFactory(
            settings=settings,
            session_factory=session_factory,
            event_publisher=event_publisher,
            now=lambda: WorkerFactoryFixtures.now,
        )

        worker = factory.create_outbox_worker()
        result = worker.run_once()

        assert result.claimed_count == 1
        assert result.published_count == 1
        assert len(event_publisher.events) == 1

    def test_from_env_loads_settings(self) -> None:
        factory = OutboxWorkerFactory.from_env()

        assert isinstance(factory.settings, Settings)


class TestDeliveryWorkerFactory:
    @pytest.mark.kwparametrize(
        [
            {
                "id": "web",
                "method_name": "create_web_delivery_processor",
                "expected_channels": (Channel.WEB,),
            },
            {
                "id": "email",
                "method_name": "create_email_delivery_processor",
                "expected_channels": (Channel.EMAIL,),
            },
        ]
    )
    def test_create_channel_delivery_processors_filter_claimed_channels(
        self,
        session_factory: SessionFactory,
        method_name: str,
        expected_channels: tuple[Channel, ...],
    ) -> None:
        factory = DeliveryWorkerFactory(
            settings=WorkerFactoryFixtures.settings(),
            session_factory=session_factory,
        )

        processor = getattr(factory, method_name)()

        assert isinstance(processor, DeliveryWorker)
        assert processor.channels == expected_channels
