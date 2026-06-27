from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import (
    NotificationActionInvocationModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    OutboxEventModel,
    UserModel,
)
from backend.db.repositories.notifications import NotificationRepository
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    ActionInvocationResult,
    AudienceType,
    Channel,
    DeliveryStatus,
    NotificationRequestStatus,
    RequestedByType,
    Severity,
)
from backend.services.retry_service import RetryService

pytestmark = pytest.mark.integration

SessionFactory = sessionmaker[Session]


class BarrierRetryRepository(NotificationRepository):
    def __init__(self, session: Session, barrier: Barrier) -> None:
        super().__init__(session)
        self._barrier = barrier

    def list_failed_retryable_deliveries(
        self,
        notification_id: UUID,
    ) -> list[NotificationDeliveryModel]:
        deliveries = super().list_failed_retryable_deliveries(notification_id)
        self._barrier.wait(timeout=5)
        return deliveries


class BarrierRetryUnitOfWork(SqlAlchemyUnitOfWork):
    def __init__(self, session_factory: SessionFactory, barrier: Barrier) -> None:
        super().__init__(session_factory)
        self._barrier = barrier

    def __enter__(self) -> SqlAlchemyUnitOfWork:
        super().__enter__()
        self.notifications = BarrierRetryRepository(self.session, self._barrier)
        return self


class TestPostgresRetryConcurrency:
    def test_concurrent_retry_queues_delivery_once(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
        with postgres_session_factory() as session:
            user = UserModel(email="user@example.test", display_name="User")
            notification = NotificationRequestModel(
                source_service="billing",
                message="Retry",
                severity=Severity.ERROR.value,
                audience_type=AudienceType.ALL.value,
                audience={"type": AudienceType.ALL.value},
                channels=[Channel.EMAIL.value],
                status=NotificationRequestStatus.ACCEPTED.value,
                idempotency_key="retry-concurrency",
                payload_fingerprint="retry-concurrency",
                created_at=now,
                updated_at=now,
            )
            session.add_all([user, notification])
            session.flush()
            recipient = NotificationRecipientModel(
                notification_id=notification.id,
                user_id=user.id,
                created_at=now,
            )
            session.add(recipient)
            session.flush()
            delivery = NotificationDeliveryModel(
                notification_recipient_id=recipient.id,
                notification_id=notification.id,
                user_id=user.id,
                channel=Channel.EMAIL.value,
                status=DeliveryStatus.FAILED_RETRYABLE.value,
                next_attempt_at=now,
                created_at=now,
                updated_at=now,
            )
            session.add(delivery)
            session.commit()
            notification_id = notification.id
            delivery_id = delivery.id

        barrier = Barrier(2)
        service = RetryService(
            unit_of_work_factory=lambda: BarrierRetryUnitOfWork(
                postgres_session_factory,
                barrier,
            ),
            now=lambda: now,
        )

        def retry() -> tuple[ActionInvocationResult, int]:
            result = service.retry_notification(
                notification_id=notification_id,
                requested_by_type=RequestedByType.SERVICE,
                requested_by_id="billing",
            )
            return result.status, result.replayed_delivery_count

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: retry(), range(2)))

        assert sorted(count for _, count in results) == [0, 1]
        assert {status for status, _ in results} == {
            ActionInvocationResult.QUEUED,
            ActionInvocationResult.NO_ELIGIBLE,
        }
        with postgres_session_factory() as session:
            persisted_delivery = session.get(NotificationDeliveryModel, delivery_id)
            assert persisted_delivery is not None
            assert persisted_delivery.status == DeliveryStatus.REPLAY_REQUESTED.value
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 1
            assert session.scalar(
                select(func.count(NotificationActionInvocationModel.id))
            ) == 2
