from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import Base, OutboxEventModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import OutboxEventStatus
from workers.outbox.publisher import OutboxPublisher

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


class OutboxPublisherFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def publisher(
        session_factory: SessionFactory,
        *,
        event_publisher: RecordingEventPublisher | FailingEventPublisher,
        max_attempts: int = 3,
    ) -> OutboxPublisher:
        return OutboxPublisher(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            event_publisher=event_publisher,
            now=lambda: OutboxPublisherFixtures.now,
            worker_id="outbox-worker-1",
            lease_duration=timedelta(seconds=30),
            retry_delay=timedelta(minutes=2),
            max_attempts=max_attempts,
        )

    @staticmethod
    def seed_event(
        session_factory: SessionFactory,
        *,
        status: OutboxEventStatus = OutboxEventStatus.PENDING,
        attempts: int = 0,
        next_attempt_at: datetime | None = None,
        lease_expires_at: datetime | None = None,
        claimed_by: str | None = None,
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
                    status=status.value,
                    attempts=attempts,
                    next_attempt_at=next_attempt_at or OutboxPublisherFixtures.now,
                    lease_expires_at=lease_expires_at,
                    claimed_by=claimed_by,
                    created_at=OutboxPublisherFixtures.now,
                    updated_at=OutboxPublisherFixtures.now,
                )
            )
            session.commit()
        return event_id


class TestOutboxPublisher:
    def test_publishes_due_event_and_marks_it_published(
        self,
        session_factory: SessionFactory,
    ) -> None:
        event_id = OutboxPublisherFixtures.seed_event(session_factory)
        event_publisher = RecordingEventPublisher()
        publisher = OutboxPublisherFixtures.publisher(
            session_factory,
            event_publisher=event_publisher,
        )

        result = publisher.publish_due_events(limit=10)

        assert result.claimed_count == 1
        assert result.published_count == 1
        assert result.failed_retryable_count == 0
        assert result.failed_terminal_count == 0
        assert event_publisher.events == [
            (
                "notifications.requests",
                "notification-1",
                {
                    "schema_version": 1,
                    "event_id": str(event_id),
                    "event_type": "notification.requested",
                    "occurred_at": OutboxPublisherFixtures.now.isoformat(),
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
            assert event.published_at == OutboxPublisherFixtures.now.replace(tzinfo=None)
            assert event.claimed_by is None
            assert event.claim_token is None
            assert event.lease_expires_at is None
            assert event.last_error is None

    @pytest.mark.kwparametrize(
        [
            {
                "id": "pending-due",
                "status": OutboxEventStatus.PENDING,
                "next_attempt_at": OutboxPublisherFixtures.now,
                "lease_expires_at": None,
                "claimed_by": None,
                "expected_published": True,
            },
            {
                "id": "retryable-due",
                "status": OutboxEventStatus.FAILED_RETRYABLE,
                "next_attempt_at": OutboxPublisherFixtures.now,
                "lease_expires_at": None,
                "claimed_by": None,
                "expected_published": True,
            },
            {
                "id": "expired-publishing-lease",
                "status": OutboxEventStatus.PUBLISHING,
                "next_attempt_at": OutboxPublisherFixtures.now,
                "lease_expires_at": OutboxPublisherFixtures.now - timedelta(seconds=1),
                "claimed_by": "stale-worker",
                "expected_published": True,
            },
            {
                "id": "pending-future",
                "status": OutboxEventStatus.PENDING,
                "next_attempt_at": OutboxPublisherFixtures.now + timedelta(minutes=1),
                "lease_expires_at": None,
                "claimed_by": None,
                "expected_published": False,
            },
            {
                "id": "active-publishing-lease",
                "status": OutboxEventStatus.PUBLISHING,
                "next_attempt_at": OutboxPublisherFixtures.now,
                "lease_expires_at": OutboxPublisherFixtures.now + timedelta(seconds=30),
                "claimed_by": "active-worker",
                "expected_published": False,
            },
            {
                "id": "already-published",
                "status": OutboxEventStatus.PUBLISHED,
                "next_attempt_at": OutboxPublisherFixtures.now,
                "lease_expires_at": None,
                "claimed_by": None,
                "expected_published": False,
            },
            {
                "id": "terminal-failure",
                "status": OutboxEventStatus.FAILED_TERMINAL,
                "next_attempt_at": OutboxPublisherFixtures.now,
                "lease_expires_at": None,
                "claimed_by": None,
                "expected_published": False,
            },
        ]
    )
    def test_claim_eligibility_matrix(
        self,
        session_factory: SessionFactory,
        status: OutboxEventStatus,
        next_attempt_at: datetime,
        lease_expires_at: datetime | None,
        claimed_by: str | None,
        expected_published: bool,
    ) -> None:
        event_id = OutboxPublisherFixtures.seed_event(
            session_factory,
            status=status,
            next_attempt_at=next_attempt_at,
            lease_expires_at=lease_expires_at,
            claimed_by=claimed_by,
        )
        event_publisher = RecordingEventPublisher()
        publisher = OutboxPublisherFixtures.publisher(
            session_factory,
            event_publisher=event_publisher,
        )

        result = publisher.publish_due_events(limit=10)

        assert result.claimed_count == (1 if expected_published else 0)
        assert result.published_count == (1 if expected_published else 0)
        assert len(event_publisher.events) == (1 if expected_published else 0)

        with session_factory() as session:
            event = session.get(OutboxEventModel, event_id)
            assert event is not None
            if expected_published:
                assert event.status == OutboxEventStatus.PUBLISHED.value
                assert event.claimed_by is None
                assert event.lease_expires_at is None
            else:
                assert event.status == status.value

    @pytest.mark.kwparametrize(
        [
            {
                "id": "retryable-failure",
                "initial_attempts": 0,
                "max_attempts": 3,
                "expected_status": OutboxEventStatus.FAILED_RETRYABLE,
                "expected_next_attempt_at": OutboxPublisherFixtures.now + timedelta(minutes=2),
                "expected_failed_retryable_count": 1,
                "expected_failed_terminal_count": 0,
            },
            {
                "id": "exhausted-failure",
                "initial_attempts": 2,
                "max_attempts": 3,
                "expected_status": OutboxEventStatus.FAILED_TERMINAL,
                "expected_next_attempt_at": OutboxPublisherFixtures.now,
                "expected_failed_retryable_count": 0,
                "expected_failed_terminal_count": 1,
            },
        ]
    )
    def test_publish_failure_state_transition_matrix(
        self,
        session_factory: SessionFactory,
        initial_attempts: int,
        max_attempts: int,
        expected_status: OutboxEventStatus,
        expected_next_attempt_at: datetime,
        expected_failed_retryable_count: int,
        expected_failed_terminal_count: int,
    ) -> None:
        event_id = OutboxPublisherFixtures.seed_event(
            session_factory,
            attempts=initial_attempts,
        )
        publisher = OutboxPublisherFixtures.publisher(
            session_factory,
            event_publisher=FailingEventPublisher(),
            max_attempts=max_attempts,
        )

        result = publisher.publish_due_events(limit=10)

        assert result.claimed_count == 1
        assert result.published_count == 0
        assert result.failed_retryable_count == expected_failed_retryable_count
        assert result.failed_terminal_count == expected_failed_terminal_count

        with session_factory() as session:
            event = session.get(OutboxEventModel, event_id)
            assert event is not None
            assert event.status == expected_status.value
            assert event.attempts == initial_attempts + 1
            assert event.next_attempt_at == expected_next_attempt_at.replace(tzinfo=None)
            assert event.claimed_by is None
            assert event.lease_expires_at is None
            assert event.last_error == "broker unavailable"

    def test_respects_limit(self, session_factory: SessionFactory) -> None:
        first_id = OutboxPublisherFixtures.seed_event(session_factory)
        second_id = OutboxPublisherFixtures.seed_event(session_factory)
        event_publisher = RecordingEventPublisher()
        publisher = OutboxPublisherFixtures.publisher(
            session_factory,
            event_publisher=event_publisher,
        )

        result = publisher.publish_due_events(limit=1)

        assert result.claimed_count == 1
        assert result.published_count == 1
        assert len(event_publisher.events) == 1

        with session_factory() as session:
            events = {
                event.id: event.status
                for event in session.scalars(
                    select(OutboxEventModel).where(OutboxEventModel.id.in_([first_id, second_id]))
                )
            }
            assert list(events.values()).count(OutboxEventStatus.PUBLISHED.value) == 1
            assert list(events.values()).count(OutboxEventStatus.PENDING.value) == 1

    def test_rejects_naive_clock(self, session_factory: SessionFactory) -> None:
        publisher = OutboxPublisher(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            event_publisher=RecordingEventPublisher(),
            now=lambda: datetime(2026, 6, 24, 12, 0),
            worker_id="outbox-worker-1",
            lease_duration=timedelta(seconds=30),
            retry_delay=timedelta(minutes=2),
            max_attempts=3,
        )

        with pytest.raises(ValueError, match="timezone-aware"):
            publisher.publish_due_events(limit=10)
