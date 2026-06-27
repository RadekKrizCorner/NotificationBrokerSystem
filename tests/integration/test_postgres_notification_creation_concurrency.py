from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import NotificationRequestModel, OutboxEventModel
from backend.db.repositories.notifications import NotificationRepository
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    AudienceType,
    Channel,
    NotificationCreateResultStatus,
    Severity,
)
from backend.domain.errors import IdempotencyConflict
from backend.domain.value_objects import AudienceSelection, NotificationCreationInput
from backend.services.notification_service import NotificationCreationService

pytestmark = pytest.mark.integration

SessionFactory = sessionmaker[Session]


class BarrierNotificationRepository(NotificationRepository):
    def __init__(self, session: Session, barrier: Barrier) -> None:
        super().__init__(session)
        self._barrier = barrier
        self._waited = False

    def find_by_explicit_idempotency_key(
        self,
        *,
        source_service: str,
        idempotency_key: str,
    ) -> NotificationRequestModel | None:
        result = super().find_by_explicit_idempotency_key(
            source_service=source_service,
            idempotency_key=idempotency_key,
        )
        if not self._waited:
            self._waited = True
            self._barrier.wait(timeout=5)
        return result


class BarrierUnitOfWork(SqlAlchemyUnitOfWork):
    def __init__(self, session_factory: SessionFactory, barrier: Barrier) -> None:
        super().__init__(session_factory)
        self._barrier = barrier

    def __enter__(self) -> SqlAlchemyUnitOfWork:
        super().__enter__()
        self.notifications = BarrierNotificationRepository(
            self.session,
            self._barrier,
        )
        return self


class TestPostgresNotificationCreationConcurrency:
    def test_returns_one_created_and_one_existing(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        barrier = Barrier(2)
        now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
        service = NotificationCreationService(
            unit_of_work_factory=lambda: BarrierUnitOfWork(
                postgres_session_factory,
                barrier,
            ),
            now=lambda: now,
            fallback_deduplication_window=timedelta(minutes=10),
        )
        request = NotificationCreationInput(
            message="Billing sync failed",
            severity=Severity.WARNING,
            audience=AudienceSelection(type=AudienceType.ALL),
            channels=(Channel.WEB,),
        )

        def create() -> NotificationCreateResultStatus:
            return service.create_notification(
                source_service="billing",
                request=request,
                idempotency_key="concurrent-key",
            ).status

        with ThreadPoolExecutor(max_workers=2) as executor:
            statuses = list(executor.map(lambda _: create(), range(2)))

        assert sorted(statuses) == [
            NotificationCreateResultStatus.CREATED,
            NotificationCreateResultStatus.EXISTING,
        ]
        with postgres_session_factory() as session:
            assert session.scalar(select(func.count(NotificationRequestModel.id))) == 1
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 1

    def test_different_concurrent_payload_returns_one_conflict(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        barrier = Barrier(2)
        now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
        service = NotificationCreationService(
            unit_of_work_factory=lambda: BarrierUnitOfWork(
                postgres_session_factory,
                barrier,
            ),
            now=lambda: now,
            fallback_deduplication_window=timedelta(minutes=10),
        )

        def create(message: str) -> NotificationCreateResultStatus:
            request = NotificationCreationInput(
                message=message,
                severity=Severity.WARNING,
                audience=AudienceSelection(type=AudienceType.ALL),
                channels=(Channel.WEB,),
            )
            return service.create_notification(
                source_service="billing",
                request=request,
                idempotency_key="concurrent-key",
            ).status

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(create, "first"),
                executor.submit(create, "second"),
            ]
            statuses: list[NotificationCreateResultStatus] = []
            errors: list[BaseException] = []
            for future in futures:
                try:
                    statuses.append(future.result())
                except BaseException as exc:
                    errors.append(exc)

        assert statuses == [NotificationCreateResultStatus.CREATED]
        assert len(errors) == 1
        assert isinstance(errors[0], IdempotencyConflict)
        with postgres_session_factory() as session:
            assert session.scalar(select(func.count(NotificationRequestModel.id))) == 1
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 1
