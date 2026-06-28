from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import (
    Base,
    DeliveryAttemptModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    UserLabelModel,
    UserModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    AudienceType,
    Channel,
    DeliveryOutcomeStatus,
    DeliveryStatus,
    Severity,
)
from backend.domain.results import DeliveryOutcome
from backend.services.notification_fanout_service import NotificationFanoutService
from workers.delivery.base import DeliveryAdapter
from workers.delivery.web import WebDeliveryAdapter
from workers.delivery.worker import DeliveryWorker
from workers.notifications.requested import NotificationRequestedHandler

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


class FakeDeliveryAdapter:
    def __init__(self, outcome: DeliveryOutcome) -> None:
        self.outcome = outcome
        self.delivery_ids: list[UUID] = []

    def deliver(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> DeliveryOutcome:
        self.delivery_ids.append(delivery.id)
        return self.outcome


class RaisingDeliveryAdapter:
    def deliver(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> DeliveryOutcome:
        raise RuntimeError("provider secret must not leak: " + ("x" * 2_000))


class WorkerFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def unit_of_work_factory(session_factory: SessionFactory) -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    @staticmethod
    def fanout_handler(session_factory: SessionFactory) -> NotificationRequestedHandler:
        service = NotificationFanoutService(
            unit_of_work_factory=lambda: WorkerFixtures.unit_of_work_factory(session_factory),
            now=lambda: WorkerFixtures.now,
        )
        return NotificationRequestedHandler(fanout_service=service)

    @staticmethod
    def delivery_worker(
        session_factory: SessionFactory,
        *,
        adapters: Mapping[Channel, DeliveryAdapter],
        channels: tuple[Channel, ...] | None = None,
    ) -> DeliveryWorker:
        return DeliveryWorker(
            unit_of_work_factory=lambda: WorkerFixtures.unit_of_work_factory(session_factory),
            adapters=adapters,
            channels=channels,
            now=lambda: WorkerFixtures.now,
            worker_id="worker-1",
            lease_duration=timedelta(seconds=30),
            retry_delay=timedelta(minutes=5),
        )

    @staticmethod
    def seed_labeled_notification(session_factory: SessionFactory) -> UUID:
        with session_factory() as session:
            eu_user = UserModel(email="eu@example.test", display_name="EU")
            us_user = UserModel(email="us@example.test", display_name="US")
            notification = WorkerFixtures.notification(
                audience_type=AudienceType.LABELS,
                audience={"type": "labels", "labels": {"region": "EU"}},
                channels=(Channel.WEB, Channel.EMAIL),
            )
            session.add_all([eu_user, us_user, notification])
            session.flush()
            session.add_all(
                [
                    UserLabelModel(user_id=eu_user.id, key="region", value="EU"),
                    UserLabelModel(user_id=us_user.id, key="region", value="US"),
                ]
            )
            session.commit()
            return notification.id

    @staticmethod
    def seed_delivery(
        session_factory: SessionFactory,
        *,
        channel: Channel,
        status: DeliveryStatus = DeliveryStatus.PENDING,
        attempt_count: int = 0,
        max_attempts: int = 3,
        next_attempt_at: datetime | None = None,
        processing_started_at: datetime | None = None,
        lease_expires_at: datetime | None = None,
        claimed_by: str | None = None,
        user_email: str = "worker-user@example.test",
    ) -> UUID:
        with session_factory() as session:
            user = UserModel(email=user_email, display_name="Worker User")
            notification = WorkerFixtures.notification(
                audience_type=AudienceType.ALL,
                audience={"type": "all"},
                channels=(channel,),
            )
            session.add_all([user, notification])
            session.flush()
            recipient = NotificationRecipientModel(
                notification_id=notification.id,
                user_id=user.id,
            )
            session.add(recipient)
            session.flush()
            delivery = NotificationDeliveryModel(
                notification_recipient_id=recipient.id,
                notification_id=notification.id,
                user_id=user.id,
                channel=channel.value,
                status=status.value,
                attempt_count=attempt_count,
                max_attempts=max_attempts,
                next_attempt_at=next_attempt_at or WorkerFixtures.now,
                processing_started_at=processing_started_at,
                lease_expires_at=lease_expires_at,
                claimed_by=claimed_by,
                created_at=WorkerFixtures.now,
                updated_at=WorkerFixtures.now,
            )
            session.add(delivery)
            session.commit()
            return delivery.id

    @staticmethod
    def notification(
        *,
        audience_type: AudienceType,
        audience: dict[str, object],
        channels: tuple[Channel, ...],
    ) -> NotificationRequestModel:
        return NotificationRequestModel(
            source_service="billing",
            message="Worker notification",
            severity=Severity.WARNING.value,
            audience_type=audience_type.value,
            audience=audience,
            channels=[channel.value for channel in channels],
            status="accepted",
            idempotency_key=str(uuid4()),
            payload_fingerprint=str(uuid4()),
            created_at=WorkerFixtures.now,
            updated_at=WorkerFixtures.now,
        )


class TestNotificationRequestedHandler:
    def test_requested_event_fans_out_notification_idempotently(
        self,
        session_factory: SessionFactory,
    ) -> None:
        notification_id = WorkerFixtures.seed_labeled_notification(session_factory)
        handler = WorkerFixtures.fanout_handler(session_factory)

        first = handler.handle({"notification_id": str(notification_id)})
        second = handler.handle({"notification_id": str(notification_id)})

        assert first.recipient_count == 1
        assert first.delivery_count == 2
        assert second.recipient_count == 1
        assert second.delivery_count == 2

        with session_factory() as session:
            assert session.scalar(select(func.count(NotificationRecipientModel.id))) == 1
            assert session.scalar(select(func.count(NotificationDeliveryModel.id))) == 2

    def test_requested_event_rejects_missing_notification_id(
        self,
        session_factory: SessionFactory,
    ) -> None:
        handler = WorkerFixtures.fanout_handler(session_factory)

        with pytest.raises(ValueError, match="notification_id"):
            handler.handle({"source_service": "billing"})

    def test_large_fanout_uses_a_bounded_number_of_database_round_trips(
        self,
        session_factory: SessionFactory,
    ) -> None:
        with session_factory() as session:
            users = [
                UserModel(
                    email=f"bulk-{index}@example.test",
                    display_name=f"Bulk {index}",
                )
                for index in range(50)
            ]
            notification = WorkerFixtures.notification(
                audience_type=AudienceType.ALL,
                audience={"type": "all"},
                channels=(Channel.WEB, Channel.EMAIL),
            )
            session.add_all([*users, notification])
            session.commit()
            notification_id = notification.id

        statement_count = 0

        def count_statement(*_args: object) -> None:
            nonlocal statement_count
            statement_count += 1

        engine = session_factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", count_statement)
        try:
            result = WorkerFixtures.fanout_handler(session_factory).handle(
                {"notification_id": str(notification_id)}
            )
        finally:
            event.remove(engine, "before_cursor_execute", count_statement)

        assert result.recipient_count == 50
        assert result.delivery_count == 100
        assert statement_count <= 10


class TestDeliveryWorker:
    def test_web_delivery_makes_notification_visible(
        self,
        session_factory: SessionFactory,
    ) -> None:
        delivery_id = WorkerFixtures.seed_delivery(session_factory, channel=Channel.WEB)
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={Channel.WEB: WebDeliveryAdapter()},
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.claimed_count == 1
        assert result.processed_count == 1

        with session_factory() as session:
            delivery = session.get(NotificationDeliveryModel, delivery_id)
            attempt = session.scalar(select(DeliveryAttemptModel))
            assert delivery is not None
            assert delivery.status == DeliveryStatus.DELIVERED.value
            assert delivery.delivered_at == WorkerFixtures.now.replace(tzinfo=None)
            assert delivery.attempt_count == 1
            assert delivery.claimed_by is None
            assert delivery.claim_token is None
            assert delivery.lease_expires_at is None
            assert attempt is not None
            assert attempt.status == DeliveryStatus.DELIVERED.value

    @pytest.mark.kwparametrize(
        [
            {
                "id": "success",
                "outcome": DeliveryOutcome(
                    status=DeliveryOutcomeStatus.DELIVERED,
                    provider_message_id="provider-1",
                ),
                "initial_attempt_count": 0,
                "max_attempts": 3,
                "expected_status": DeliveryStatus.DELIVERED,
                "expected_next_attempt_at": None,
            },
            {
                "id": "retryable-failure",
                "outcome": DeliveryOutcome(
                    status=DeliveryOutcomeStatus.FAILED_RETRYABLE,
                    error_code="temporary",
                    error_message="try later",
                ),
                "initial_attempt_count": 0,
                "max_attempts": 3,
                "expected_status": DeliveryStatus.FAILED_RETRYABLE,
                "expected_next_attempt_at": WorkerFixtures.now + timedelta(minutes=5),
            },
            {
                "id": "retryable-failure-exhausted",
                "outcome": DeliveryOutcome(
                    status=DeliveryOutcomeStatus.FAILED_RETRYABLE,
                    error_code="temporary",
                    error_message="try later",
                ),
                "initial_attempt_count": 2,
                "max_attempts": 3,
                "expected_status": DeliveryStatus.FAILED_TERMINAL,
                "expected_next_attempt_at": None,
            },
            {
                "id": "terminal-failure",
                "outcome": DeliveryOutcome(
                    status=DeliveryOutcomeStatus.FAILED_TERMINAL,
                    error_code="permanent",
                    error_message="bad address",
                ),
                "initial_attempt_count": 0,
                "max_attempts": 3,
                "expected_status": DeliveryStatus.FAILED_TERMINAL,
                "expected_next_attempt_at": None,
            },
        ]
    )
    def test_delivery_state_transition_matrix(
        self,
        session_factory: SessionFactory,
        outcome: DeliveryOutcome,
        initial_attempt_count: int,
        max_attempts: int,
        expected_status: DeliveryStatus,
        expected_next_attempt_at: datetime | None,
    ) -> None:
        delivery_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.EMAIL,
            attempt_count=initial_attempt_count,
            max_attempts=max_attempts,
        )
        adapter = FakeDeliveryAdapter(outcome)
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={Channel.EMAIL: adapter},
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.processed_count == 1
        assert adapter.delivery_ids == [delivery_id]

        with session_factory() as session:
            delivery = session.get(NotificationDeliveryModel, delivery_id)
            attempt = session.scalar(select(DeliveryAttemptModel))
            assert delivery is not None
            assert delivery.status == expected_status.value
            assert delivery.attempt_count == initial_attempt_count + 1
            assert delivery.claimed_by is None
            assert delivery.processing_started_at is None
            assert delivery.lease_expires_at is None
            assert delivery.next_attempt_at == (
                expected_next_attempt_at.replace(tzinfo=None)
                if expected_next_attempt_at is not None
                else WorkerFixtures.now.replace(tzinfo=None)
            )
            assert attempt is not None
            assert attempt.attempt_number == initial_attempt_count + 1
            assert attempt.status == expected_status.value

    def test_worker_ignores_future_delivery_rows(self, session_factory: SessionFactory) -> None:
        WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.EMAIL,
            next_attempt_at=WorkerFixtures.now + timedelta(minutes=1),
        )
        adapter = FakeDeliveryAdapter(DeliveryOutcome(status=DeliveryOutcomeStatus.DELIVERED))
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={Channel.EMAIL: adapter},
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.claimed_count == 0
        assert result.processed_count == 0
        assert adapter.delivery_ids == []

    def test_worker_claims_only_configured_channels(
        self,
        session_factory: SessionFactory,
    ) -> None:
        web_delivery_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.WEB,
            user_email="web-worker-user@example.test",
        )
        email_delivery_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.EMAIL,
            user_email="email-worker-user@example.test",
        )
        web_adapter = FakeDeliveryAdapter(DeliveryOutcome(status=DeliveryOutcomeStatus.DELIVERED))
        email_adapter = FakeDeliveryAdapter(DeliveryOutcome(status=DeliveryOutcomeStatus.DELIVERED))
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={
                Channel.WEB: web_adapter,
                Channel.EMAIL: email_adapter,
            },
            channels=(Channel.WEB,),
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.claimed_count == 1
        assert web_adapter.delivery_ids == [web_delivery_id]
        assert email_adapter.delivery_ids == []
        with session_factory() as session:
            email_delivery = session.get(NotificationDeliveryModel, email_delivery_id)
            assert email_delivery is not None
            assert email_delivery.status == DeliveryStatus.PENDING.value

    def test_worker_processes_replay_requested_rows(self, session_factory: SessionFactory) -> None:
        delivery_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.WEB,
            status=DeliveryStatus.REPLAY_REQUESTED,
        )
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={Channel.WEB: WebDeliveryAdapter()},
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.processed_count == 1
        with session_factory() as session:
            delivery = session.get(NotificationDeliveryModel, delivery_id)
            assert delivery is not None
            assert delivery.status == DeliveryStatus.DELIVERED.value

    def test_worker_recovers_expired_processing_rows(self, session_factory: SessionFactory) -> None:
        delivery_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.WEB,
            status=DeliveryStatus.PROCESSING,
            processing_started_at=WorkerFixtures.now - timedelta(minutes=5),
            lease_expires_at=WorkerFixtures.now - timedelta(seconds=1),
            claimed_by="stale-worker",
        )
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={Channel.WEB: WebDeliveryAdapter()},
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.processed_count == 1
        with session_factory() as session:
            delivery = session.get(NotificationDeliveryModel, delivery_id)
            assert delivery is not None
            assert delivery.status == DeliveryStatus.DELIVERED.value
            assert delivery.claimed_by is None

    def test_worker_ignores_active_processing_rows(self, session_factory: SessionFactory) -> None:
        WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.WEB,
            status=DeliveryStatus.PROCESSING,
            processing_started_at=WorkerFixtures.now,
            lease_expires_at=WorkerFixtures.now + timedelta(seconds=30),
            claimed_by="active-worker",
        )
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={Channel.WEB: WebDeliveryAdapter()},
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.claimed_count == 0
        assert result.processed_count == 0

    def test_worker_records_terminal_failure_when_adapter_is_missing(
        self,
        session_factory: SessionFactory,
    ) -> None:
        delivery_id = WorkerFixtures.seed_delivery(session_factory, channel=Channel.EMAIL)
        worker = WorkerFixtures.delivery_worker(session_factory, adapters={})

        result = worker.process_due_deliveries(limit=10)

        assert result.processed_count == 1
        with session_factory() as session:
            delivery = session.get(NotificationDeliveryModel, delivery_id)
            attempt = session.scalar(select(DeliveryAttemptModel))
            assert delivery is not None
            assert delivery.status == DeliveryStatus.FAILED_TERMINAL.value
            assert delivery.last_error_code == "missing_adapter"
            assert attempt is not None
            assert attempt.status == DeliveryStatus.FAILED_TERMINAL.value

    def test_worker_isolates_unexpected_adapter_errors(
        self,
        session_factory: SessionFactory,
    ) -> None:
        failed_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.EMAIL,
            user_email="failed@example.test",
        )
        delivered_id = WorkerFixtures.seed_delivery(
            session_factory,
            channel=Channel.WEB,
            user_email="delivered@example.test",
        )
        worker = WorkerFixtures.delivery_worker(
            session_factory,
            adapters={
                Channel.EMAIL: RaisingDeliveryAdapter(),
                Channel.WEB: WebDeliveryAdapter(),
            },
        )

        result = worker.process_due_deliveries(limit=10)

        assert result.claimed_count == 2
        assert result.processed_count == 2
        with session_factory() as session:
            failed = session.get(NotificationDeliveryModel, failed_id)
            delivered = session.get(NotificationDeliveryModel, delivered_id)
            assert failed is not None
            assert failed.status == DeliveryStatus.FAILED_RETRYABLE.value
            assert failed.last_error_code == "adapter_exception"
            assert failed.last_error_message == "delivery adapter raised RuntimeError"
            assert failed.claim_token is None
            assert delivered is not None
            assert delivered.status == DeliveryStatus.DELIVERED.value
