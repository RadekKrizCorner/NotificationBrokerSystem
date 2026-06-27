from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import (
    Base,
    NotificationActionInvocationModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    OutboxEventModel,
    UserModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    ActionInvocationResult,
    ActionType,
    AudienceType,
    Channel,
    DeliveryStatus,
    RequestedByType,
    Severity,
)
from backend.services.retry_service import RetryService

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


class RetryServiceFixtures:
    @staticmethod
    def service(session_factory: SessionFactory) -> RetryService:
        return RetryService(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            now=lambda: datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
        )

    @staticmethod
    def seed_notification_with_deliveries(
        session_factory: SessionFactory,
        *,
        statuses: tuple[DeliveryStatus, ...] = (
            DeliveryStatus.FAILED_RETRYABLE,
            DeliveryStatus.DELIVERED,
            DeliveryStatus.FAILED_TERMINAL,
            DeliveryStatus.FAILED_RETRYABLE,
        ),
    ) -> UUID:
        with session_factory() as session:
            notification = RetryServiceFixtures._add_notification(session)
            for index, status in enumerate(statuses):
                RetryServiceFixtures._add_delivery(
                    session,
                    notification_id=notification.id,
                    index=index,
                    status=status,
                )
            session.commit()
            return notification.id

    @staticmethod
    def _add_notification(session: Session) -> NotificationRequestModel:
        notification = NotificationRequestModel(
            id=uuid4(),
            source_service="billing",
            message="Billing sync failed",
            severity=Severity.WARNING.value,
            audience_type=AudienceType.ALL.value,
            audience={"type": AudienceType.ALL.value},
            channels=[Channel.WEB.value, Channel.EMAIL.value],
            status="accepted",
            idempotency_key=str(uuid4()),
            payload_fingerprint="fingerprint",
        )
        session.add(notification)
        session.flush()
        return notification

    @staticmethod
    def _add_delivery(
        session: Session,
        *,
        notification_id: UUID,
        index: int,
        status: DeliveryStatus,
    ) -> None:
        user = UserModel(
            email=f"user-{index}@example.test",
            display_name=f"User {index}",
        )
        session.add(user)
        session.flush()
        recipient = NotificationRecipientModel(
            notification_id=notification_id,
            user_id=user.id,
        )
        session.add(recipient)
        session.flush()
        delivery = NotificationDeliveryModel(
            notification_recipient_id=recipient.id,
            notification_id=notification_id,
            user_id=user.id,
            channel=Channel.EMAIL.value if index % 2 else Channel.WEB.value,
            status=status.value,
            next_attempt_at=datetime(2026, 6, 24, 11, 0, tzinfo=UTC),
            processing_started_at=datetime(2026, 6, 24, 11, 30, tzinfo=UTC),
            lease_expires_at=datetime(2026, 6, 24, 11, 35, tzinfo=UTC),
            claimed_by="worker-1",
        )
        session.add(delivery)


class TestRetryService:
    def test_replays_only_failed_retryable_deliveries(
        self,
        session_factory: SessionFactory,
    ) -> None:
        notification_id = RetryServiceFixtures.seed_notification_with_deliveries(session_factory)
        service = RetryServiceFixtures.service(session_factory)

        result = service.retry_notification(
            notification_id=notification_id,
            requested_by_type=RequestedByType.SERVICE,
            requested_by_id="billing",
        )

        assert result.status is ActionInvocationResult.QUEUED
        assert result.replay_id is not None
        assert result.replayed_delivery_count == 2

        with session_factory() as session:
            replayed = session.scalars(
                select(NotificationDeliveryModel)
                .where(NotificationDeliveryModel.replay_id == result.replay_id)
                .order_by(NotificationDeliveryModel.channel)
            ).all()
            untouched = session.scalars(
                select(NotificationDeliveryModel).where(
                    NotificationDeliveryModel.replay_id.is_(None)
                )
            ).all()
            invocation = session.scalar(select(NotificationActionInvocationModel))
            outbox_event = session.scalar(select(OutboxEventModel))

            assert {delivery.status for delivery in replayed} == {
                DeliveryStatus.REPLAY_REQUESTED.value,
            }
            assert {delivery.channel for delivery in replayed} == {
                Channel.EMAIL.value,
                Channel.WEB.value,
            }
            assert all(
                delivery.next_attempt_at.replace(tzinfo=UTC) == result.requested_at
                for delivery in replayed
            )
            assert all(delivery.processing_started_at is None for delivery in replayed)
            assert all(delivery.lease_expires_at is None for delivery in replayed)
            assert all(delivery.claimed_by is None for delivery in replayed)
            assert {delivery.status for delivery in untouched} == {
                DeliveryStatus.DELIVERED.value,
                DeliveryStatus.FAILED_TERMINAL.value,
            }

            assert invocation is not None
            assert invocation.result == ActionInvocationResult.QUEUED.value
            assert invocation.replay_id == result.replay_id
            assert invocation.replayed_delivery_count == 2
            assert invocation.requested_by_type == RequestedByType.SERVICE.value
            assert invocation.requested_by_id == "billing"
            assert invocation.action_type == ActionType.RETRY.value

            assert outbox_event is not None
            assert outbox_event.event_type == "notification.replay_requested"
            assert outbox_event.aggregate_id == notification_id
            assert outbox_event.payload["notification_id"] == str(notification_id)
            assert outbox_event.payload["replay_id"] == str(result.replay_id)
            payload_delivery_ids = cast(list[str], outbox_event.payload["delivery_ids"])
            assert set(payload_delivery_ids) == {str(delivery.id) for delivery in replayed}

    def test_records_no_eligible_without_outbox_event(
        self,
        session_factory: SessionFactory,
    ) -> None:
        notification_id = RetryServiceFixtures.seed_notification_with_deliveries(
            session_factory,
            statuses=(DeliveryStatus.DELIVERED, DeliveryStatus.FAILED_TERMINAL),
        )
        service = RetryServiceFixtures.service(session_factory)

        result = service.retry_notification(
            notification_id=notification_id,
            requested_by_type=RequestedByType.SERVICE,
            requested_by_id="billing",
        )

        assert result.status is ActionInvocationResult.NO_ELIGIBLE
        assert result.replay_id is None
        assert result.replayed_delivery_count == 0

        with session_factory() as session:
            invocation = session.scalar(select(NotificationActionInvocationModel))
            assert invocation is not None
            assert invocation.result == ActionInvocationResult.NO_ELIGIBLE.value
            assert invocation.replay_id is None
            assert invocation.replayed_delivery_count == 0
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 0

    def test_rejects_missing_notification(self, session_factory: SessionFactory) -> None:
        service = RetryServiceFixtures.service(session_factory)

        with pytest.raises(ValueError, match="notification does not exist"):
            service.retry_notification(
                notification_id=uuid4(),
                requested_by_type=RequestedByType.SERVICE,
                requested_by_id="billing",
            )
