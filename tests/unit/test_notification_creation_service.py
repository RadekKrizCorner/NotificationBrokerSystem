from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import Base, NotificationRequestModel, OutboxEventModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    AudienceType,
    Channel,
    NotificationCreateResultStatus,
    OutboxEventStatus,
    Severity,
)
from backend.domain.errors import IdempotencyConflict
from backend.domain.value_objects import AudienceSelection, NotificationCreationInput
from backend.services.notification_service import NotificationCreationService

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


class NotificationCreationServiceFixtures:
    @staticmethod
    def service(
        session_factory: SessionFactory,
        *,
        now: datetime = datetime(2026, 6, 24, 12, 7, tzinfo=UTC),
    ) -> NotificationCreationService:
        return NotificationCreationService(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            now=lambda: now,
            fallback_deduplication_window=timedelta(minutes=10),
        )

    @staticmethod
    def request(message: str = "Billing sync failed") -> NotificationCreationInput:
        return NotificationCreationInput(
            message=message,
            severity=Severity.WARNING,
            audience=AudienceSelection(type=AudienceType.LABELS, labels=(("region", "EU"),)),
            channels=(Channel.WEB, Channel.EMAIL),
        )


class TestNotificationCreationService:
    def test_persists_request_and_outbox_event(
        self,
        session_factory: SessionFactory,
    ) -> None:
        service = NotificationCreationServiceFixtures.service(session_factory)

        result = service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key="billing-sync-1",
        )

        assert result.status is NotificationCreateResultStatus.CREATED

        with session_factory() as session:
            notification = session.get(NotificationRequestModel, result.notification_id)
            assert notification is not None
            assert notification.source_service == "billing"
            assert notification.idempotency_key == "billing-sync-1"
            assert notification.deduplication_hash is None
            assert notification.deduplication_window_start is None
            assert notification.message == "Billing sync failed"
            assert notification.severity == Severity.WARNING.value
            assert notification.audience == {"type": "labels", "labels": {"region": "EU"}}
            assert notification.channels == [Channel.WEB.value, Channel.EMAIL.value]

            outbox_event = session.scalar(select(OutboxEventModel))
            assert outbox_event is not None
            assert outbox_event.topic == "notifications.requests"
            assert outbox_event.event_type == "notification.requested"
            assert outbox_event.aggregate_type == "notification_request"
            assert outbox_event.aggregate_id == notification.id
            assert outbox_event.event_key == str(notification.id)
            assert outbox_event.payload["notification_id"] == str(notification.id)
            assert outbox_event.payload["source_service"] == "billing"
            assert outbox_event.status == OutboxEventStatus.PENDING.value

    @pytest.mark.kwparametrize(
        [
            {
                "id": "explicit-idempotency-key",
                "first_key": "same-key",
                "second_key": "same-key",
                "expected_fallback_hash": None,
            },
            {
                "id": "fallback-deterministic-hash",
                "first_key": None,
                "second_key": None,
                "expected_fallback_hash": "present",
            },
        ]
    )
    def test_returns_existing_notification_for_duplicate_request(
        self,
        session_factory: SessionFactory,
        first_key: str | None,
        second_key: str | None,
        expected_fallback_hash: str | None,
    ) -> None:
        service = NotificationCreationServiceFixtures.service(session_factory)

        first = service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key=first_key,
        )
        second = service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key=second_key,
        )

        assert first.status is NotificationCreateResultStatus.CREATED
        assert second.status is NotificationCreateResultStatus.EXISTING
        assert second.notification_id == first.notification_id

        with session_factory() as session:
            assert session.scalar(select(func.count(NotificationRequestModel.id))) == 1
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 1
            notification = session.get(NotificationRequestModel, first.notification_id)
            assert notification is not None
            if expected_fallback_hash is None:
                assert notification.deduplication_hash is None
                assert notification.deduplication_window_start is None
            else:
                assert notification.deduplication_hash is not None
                assert notification.deduplication_window_start is not None
                assert notification.deduplication_window_start.replace(tzinfo=UTC) == datetime(
                    2026,
                    6,
                    24,
                    12,
                    0,
                    tzinfo=UTC,
                )

    def test_rejects_explicit_key_reused_for_different_payload(
        self,
        session_factory: SessionFactory,
    ) -> None:
        service = NotificationCreationServiceFixtures.service(session_factory)
        service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request("first"),
            idempotency_key="same-key",
        )

        with pytest.raises(IdempotencyConflict):
            service.create_notification(
                source_service="billing",
                request=NotificationCreationServiceFixtures.request("second"),
                idempotency_key="same-key",
            )

    def test_duplicate_result_contains_persisted_counts(
        self,
        session_factory: SessionFactory,
    ) -> None:
        service = NotificationCreationServiceFixtures.service(session_factory)
        first = service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key="same-key",
        )
        with session_factory() as session:
            notification = session.get(NotificationRequestModel, first.notification_id)
            assert notification is not None
            notification.recipient_count = 3
            notification.delivery_count = 6
            session.commit()

        duplicate = service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key="same-key",
        )

        assert duplicate.status is NotificationCreateResultStatus.EXISTING
        assert duplicate.recipient_count == 3
        assert duplicate.delivery_count == 6

    def test_creates_new_fallback_row_after_window_changes(
        self,
        session_factory: SessionFactory,
    ) -> None:
        first_service = NotificationCreationServiceFixtures.service(
            session_factory,
            now=datetime(2026, 6, 24, 12, 7, tzinfo=UTC),
        )
        second_service = NotificationCreationServiceFixtures.service(
            session_factory,
            now=datetime(2026, 6, 24, 12, 11, tzinfo=UTC),
        )

        first = first_service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key=None,
        )
        second = second_service.create_notification(
            source_service="billing",
            request=NotificationCreationServiceFixtures.request(),
            idempotency_key=None,
        )

        assert first.status is NotificationCreateResultStatus.CREATED
        assert second.status is NotificationCreateResultStatus.CREATED
        assert second.notification_id != first.notification_id

        with session_factory() as session:
            assert session.scalar(select(func.count(NotificationRequestModel.id))) == 2
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 2
