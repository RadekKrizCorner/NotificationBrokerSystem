from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import (
    Base,
    DeliveryAttemptModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    ProcessedEventModel,
    UserModel,
)
from backend.db.repositories import (
    NotificationRepository,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import Channel, DeliveryStatus, Severity, WebNotificationStatus
from backend.domain.read_models import WebNotificationRow

SessionFactory = sessionmaker[Session]


class VisibleScenario(TypedDict):
    message: str
    user_id: UUID
    channel: Channel
    status: DeliveryStatus
    delivered_at: datetime | None
    read_at: datetime | None


class TestDatabaseModelStructure:
    def test_db_models_are_split_by_application_area(self) -> None:
        from backend.db.models.identity import GroupModel, UserModel
        from backend.db.models.notifications import NotificationRequestModel
        from backend.db.models.outbox import OutboxEventModel

        assert UserModel.__tablename__ == "users"
        assert GroupModel.__tablename__ == "groups"
        assert NotificationRequestModel.__tablename__ == "notification_requests"
        assert OutboxEventModel.__tablename__ == "outbox_events"

    def test_web_notification_row_is_a_domain_read_model(self) -> None:
        assert WebNotificationRow.__module__ == "backend.domain.read_models"


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


@pytest.fixture()
def db_session(session_factory: SessionFactory) -> Iterator[Session]:
    with session_factory() as session:
        yield session


class DbModelFixtures:
    @staticmethod
    def user(user_id: UUID | None = None, *, email: str | None = None) -> UserModel:
        user_id = user_id or uuid4()
        return UserModel(
            id=user_id,
            email=email or f"{user_id}@example.test",
            display_name="Demo User",
        )

    @staticmethod
    def notification(
        *,
        notification_id: UUID | None = None,
        source_service: str = "billing",
        idempotency_key: str | None = "billing-1",
        payload_fingerprint: str = "fingerprint-1",
        deduplication_hash: str | None = None,
        deduplication_window_start: datetime | None = None,
        created_at: datetime | None = None,
    ) -> NotificationRequestModel:
        return NotificationRequestModel(
            id=notification_id or uuid4(),
            source_service=source_service,
            message="Billing sync failed",
            severity=Severity.WARNING.value,
            audience_type="labels",
            audience={"type": "labels", "labels": {"region": "EU"}},
            channels=[Channel.WEB.value, Channel.EMAIL.value],
            status="accepted",
            idempotency_key=idempotency_key,
            payload_fingerprint=payload_fingerprint,
            deduplication_hash=deduplication_hash,
            deduplication_window_start=deduplication_window_start,
            created_at=created_at,
            updated_at=created_at,
        )

    @staticmethod
    def notification_from_values(
        values: Mapping[str, object],
        *,
        notification_id: UUID | None = None,
        payload_fingerprint: str = "fingerprint-1",
    ) -> NotificationRequestModel:
        return DbModelFixtures.notification(
            notification_id=notification_id,
            source_service=cast(str, values.get("source_service", "billing")),
            idempotency_key=cast(str | None, values.get("idempotency_key", "billing-1")),
            payload_fingerprint=payload_fingerprint,
            deduplication_hash=cast(str | None, values.get("deduplication_hash")),
            deduplication_window_start=cast(
                datetime | None,
                values.get("deduplication_window_start"),
            ),
        )

    @staticmethod
    def recipient(
        *,
        notification_id: UUID,
        user_id: UUID,
        recipient_id: UUID | None = None,
    ) -> NotificationRecipientModel:
        return NotificationRecipientModel(
            id=recipient_id or uuid4(),
            notification_id=notification_id,
            user_id=user_id,
        )

    @staticmethod
    def delivery(
        *,
        recipient_id: UUID,
        notification_id: UUID,
        user_id: UUID,
        channel: Channel = Channel.WEB,
        status: DeliveryStatus = DeliveryStatus.PENDING,
        delivery_id: UUID | None = None,
        delivered_at: datetime | None = None,
        read_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> NotificationDeliveryModel:
        return NotificationDeliveryModel(
            id=delivery_id or uuid4(),
            notification_recipient_id=recipient_id,
            notification_id=notification_id,
            user_id=user_id,
            channel=channel.value,
            status=status.value,
            next_attempt_at=created_at or datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
            delivered_at=delivered_at,
            read_at=read_at,
            created_at=created_at,
            updated_at=created_at,
        )

    @staticmethod
    def persist_notification_for_user(
        session: Session,
        *,
        user_id: UUID | None = None,
        notification_id: UUID | None = None,
        idempotency_key: str | None = "billing-1",
        deduplication_hash: str | None = None,
        deduplication_window_start: datetime | None = None,
        created_at: datetime | None = None,
    ) -> tuple[UserModel, NotificationRequestModel, NotificationRecipientModel]:
        user = DbModelFixtures.user(user_id)
        notification = DbModelFixtures.notification(
            notification_id=notification_id,
            idempotency_key=idempotency_key,
            deduplication_hash=deduplication_hash,
            deduplication_window_start=deduplication_window_start,
            created_at=created_at,
        )
        recipient = DbModelFixtures.recipient(notification_id=notification.id, user_id=user.id)
        session.add_all([user, notification, recipient])
        session.flush()
        return user, notification, recipient

    @staticmethod
    def seed_visible_notification_rows(session: Session) -> UserModel:
        user = DbModelFixtures.user(email="visible-user@example.test")
        other_user = DbModelFixtures.user(email="other-user@example.test")
        session.add_all([user, other_user])
        session.flush()
        DbModelFixtures._add_visible_notification_scenarios(
            session,
            user=user,
            other_user=other_user,
        )
        session.commit()
        return user

    @staticmethod
    def _add_visible_notification_scenarios(
        session: Session,
        *,
        user: UserModel,
        other_user: UserModel,
    ) -> None:
        base_time = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)
        scenarios: list[VisibleScenario] = [
            {
                "message": "Newest visible",
                "user_id": user.id,
                "channel": Channel.WEB,
                "status": DeliveryStatus.DELIVERED,
                "delivered_at": base_time + timedelta(minutes=3),
                "read_at": None,
            },
            {
                "message": "Unread older",
                "user_id": user.id,
                "channel": Channel.WEB,
                "status": DeliveryStatus.DELIVERED,
                "delivered_at": base_time + timedelta(minutes=2),
                "read_at": None,
            },
            {
                "message": "Read older",
                "user_id": user.id,
                "channel": Channel.WEB,
                "status": DeliveryStatus.DELIVERED,
                "delivered_at": base_time + timedelta(minutes=1),
                "read_at": base_time + timedelta(minutes=4),
            },
            {
                "message": "Pending web excluded",
                "user_id": user.id,
                "channel": Channel.WEB,
                "status": DeliveryStatus.PENDING,
                "delivered_at": None,
                "read_at": None,
            },
            {
                "message": "Email excluded",
                "user_id": user.id,
                "channel": Channel.EMAIL,
                "status": DeliveryStatus.DELIVERED,
                "delivered_at": base_time + timedelta(minutes=5),
                "read_at": None,
            },
            {
                "message": "Other user excluded",
                "user_id": other_user.id,
                "channel": Channel.WEB,
                "status": DeliveryStatus.DELIVERED,
                "delivered_at": base_time + timedelta(minutes=6),
                "read_at": None,
            },
        ]
        for index, scenario in enumerate(scenarios):
            DbModelFixtures._add_visible_notification(
                session,
                index=index,
                scenario=scenario,
                base_time=base_time,
            )

    @staticmethod
    def _add_visible_notification(
        session: Session,
        *,
        index: int,
        scenario: VisibleScenario,
        base_time: datetime,
    ) -> None:
        notification = DbModelFixtures.notification(
            notification_id=uuid4(),
            idempotency_key=f"visible-{index}",
            created_at=base_time - timedelta(hours=index),
        )
        notification.message = scenario["message"]
        recipient = DbModelFixtures.recipient(
            notification_id=notification.id,
            user_id=scenario["user_id"],
        )
        delivery = DbModelFixtures.delivery(
            recipient_id=recipient.id,
            notification_id=notification.id,
            user_id=scenario["user_id"],
            channel=scenario["channel"],
            status=scenario["status"],
            delivered_at=scenario["delivered_at"],
            read_at=scenario["read_at"],
            created_at=notification.created_at,
        )
        session.add_all([notification, recipient, delivery])


class TestDatabaseConstraints:
    @pytest.mark.kwparametrize(
        [
            {
                "id": "duplicate-explicit-key-same-source",
                "first": {"source_service": "billing", "idempotency_key": "same-key"},
                "second": {"source_service": "billing", "idempotency_key": "same-key"},
                "raises": True,
            },
            {
                "id": "same-explicit-key-different-source",
                "first": {"source_service": "billing", "idempotency_key": "same-key"},
                "second": {"source_service": "crm", "idempotency_key": "same-key"},
                "raises": False,
            },
            {
                "id": "duplicate-fallback-same-window",
                "first": {
                    "source_service": "billing",
                    "idempotency_key": None,
                    "deduplication_hash": "hash-1",
                    "deduplication_window_start": datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
                },
                "second": {
                    "source_service": "billing",
                    "idempotency_key": None,
                    "deduplication_hash": "hash-1",
                    "deduplication_window_start": datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
                },
                "raises": True,
            },
            {
                "id": "same-fallback-hash-different-window",
                "first": {
                    "source_service": "billing",
                    "idempotency_key": None,
                    "deduplication_hash": "hash-1",
                    "deduplication_window_start": datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
                },
                "second": {
                    "source_service": "billing",
                    "idempotency_key": None,
                    "deduplication_hash": "hash-1",
                    "deduplication_window_start": datetime(2026, 6, 24, 12, 10, tzinfo=UTC),
                },
                "raises": False,
            },
        ]
    )
    def test_notification_request_idempotency_constraints(
        self,
        db_session: Session,
        first: Mapping[str, object],
        second: Mapping[str, object],
        raises: bool,
    ) -> None:
        db_session.add(DbModelFixtures.notification_from_values(first, notification_id=uuid4()))
        db_session.commit()

        db_session.add(
            DbModelFixtures.notification_from_values(
                second,
                notification_id=uuid4(),
                payload_fingerprint="fingerprint-2",
            )
        )

        if raises:
            with pytest.raises(IntegrityError):
                db_session.commit()
            return

        db_session.commit()
        assert db_session.scalar(select(func.count(NotificationRequestModel.id))) == 2

    @pytest.mark.kwparametrize(
        [
            {
                "id": "fallback-hash-missing",
                "deduplication_hash": None,
                "deduplication_window_start": datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
            },
            {
                "id": "fallback-window-missing",
                "deduplication_hash": "hash-1",
                "deduplication_window_start": None,
            },
        ]
    )
    def test_fallback_idempotency_requires_hash_and_window(
        self,
        db_session: Session,
        deduplication_hash: str | None,
        deduplication_window_start: datetime | None,
    ) -> None:
        db_session.add(
            DbModelFixtures.notification(
                idempotency_key=None,
                deduplication_hash=deduplication_hash,
                deduplication_window_start=deduplication_window_start,
            )
        )

        with pytest.raises(IntegrityError):
            db_session.commit()

    @pytest.mark.kwparametrize(
        [
            {
                "id": "duplicate-recipient",
                "factory": "recipient",
                "raises": True,
            },
            {
                "id": "duplicate-delivery-channel",
                "factory": "delivery_same_channel",
                "raises": True,
            },
            {
                "id": "different-delivery-channel",
                "factory": "delivery_different_channel",
                "raises": False,
            },
            {
                "id": "duplicate-delivery-attempt-number",
                "factory": "attempt",
                "raises": True,
            },
            {
                "id": "duplicate-processed-event-consumer",
                "factory": "processed_event_same_consumer",
                "raises": True,
            },
            {
                "id": "same-event-different-consumer",
                "factory": "processed_event_different_consumer",
                "raises": False,
            },
        ]
    )
    def test_fanout_and_worker_uniqueness_constraints(
        self,
        db_session: Session,
        factory: str,
        raises: bool,
    ) -> None:
        user, notification, recipient = DbModelFixtures.persist_notification_for_user(db_session)
        delivery = DbModelFixtures.delivery(
            recipient_id=recipient.id,
            notification_id=notification.id,
            user_id=user.id,
            channel=Channel.WEB,
        )
        event_id = uuid4()
        db_session.add_all(
            [
                delivery,
                DeliveryAttemptModel(
                    id=uuid4(),
                    delivery_id=delivery.id,
                    attempt_number=1,
                    status=DeliveryStatus.FAILED_RETRYABLE.value,
                    started_at=datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
                ),
                ProcessedEventModel(
                    event_id=event_id,
                    consumer_name="worker",
                ),
            ]
        )
        db_session.commit()

        if factory == "recipient":
            db_session.add(
                DbModelFixtures.recipient(notification_id=notification.id, user_id=user.id)
            )
        elif factory == "delivery_same_channel":
            db_session.add(
                DbModelFixtures.delivery(
                    recipient_id=recipient.id,
                    notification_id=notification.id,
                    user_id=user.id,
                    channel=Channel.WEB,
                )
            )
        elif factory == "delivery_different_channel":
            db_session.add(
                DbModelFixtures.delivery(
                    recipient_id=recipient.id,
                    notification_id=notification.id,
                    user_id=user.id,
                    channel=Channel.EMAIL,
                )
            )
        elif factory == "attempt":
            db_session.add(
                DeliveryAttemptModel(
                    id=uuid4(),
                    delivery_id=delivery.id,
                    attempt_number=1,
                    status=DeliveryStatus.DELIVERED.value,
                    started_at=datetime(2026, 6, 24, 12, 1, tzinfo=UTC),
                )
            )
        elif factory == "processed_event_same_consumer":
            db_session.add(ProcessedEventModel(event_id=event_id, consumer_name="worker"))
        elif factory == "processed_event_different_consumer":
            db_session.add(ProcessedEventModel(event_id=event_id, consumer_name="outbox"))
        else:
            raise AssertionError(f"unknown factory {factory}")

        if raises:
            with pytest.raises(IntegrityError):
                db_session.commit()
            return

        db_session.commit()


class TestNotificationRepository:
    @pytest.mark.kwparametrize(
        [
            {
                "id": "all",
                "status_filter": WebNotificationStatus.ALL,
                "expected_messages": [
                    "Newest visible",
                    "Unread older",
                    "Read older",
                ],
            },
            {
                "id": "unread",
                "status_filter": WebNotificationStatus.UNREAD,
                "expected_messages": [
                    "Newest visible",
                    "Unread older",
                ],
            },
            {
                "id": "read",
                "status_filter": WebNotificationStatus.READ,
                "expected_messages": [
                    "Read older",
                ],
            },
        ]
    )
    def test_lists_visible_web_notifications(
        self,
        db_session: Session,
        status_filter: WebNotificationStatus,
        expected_messages: list[str],
    ) -> None:
        user = DbModelFixtures.seed_visible_notification_rows(db_session)
        repository = NotificationRepository(db_session)

        rows = repository.list_web_notifications_for_user(
            user_id=user.id,
            status=status_filter,
            limit=10,
        )

        assert [row.message for row in rows] == expected_messages
        assert all(isinstance(row.severity, Severity) for row in rows)

    def test_uses_delivered_at_id_cursor(self, db_session: Session) -> None:
        user = DbModelFixtures.seed_visible_notification_rows(db_session)
        repository = NotificationRepository(db_session)
        first_page = repository.list_web_notifications_for_user(
            user_id=user.id,
            status=WebNotificationStatus.ALL,
            limit=1,
        )

        assert [row.message for row in first_page] == ["Newest visible"]

        second_page = repository.list_web_notifications_for_user(
            user_id=user.id,
            status=WebNotificationStatus.ALL,
            limit=10,
            after=(first_page[0].delivered_at, first_page[0].id),
        )

        assert [row.message for row in second_page] == ["Unread older", "Read older"]


class TestSqlAlchemyUnitOfWork:
    def test_commits_and_rolls_back(self, session_factory: SessionFactory) -> None:
        committed_user_id = uuid4()
        with SqlAlchemyUnitOfWork(session_factory) as uow:
            uow.session.add(DbModelFixtures.user(committed_user_id))
            uow.commit()

        with session_factory() as session:
            assert session.get(UserModel, committed_user_id) is not None

        rolled_back_user_id = uuid4()
        with pytest.raises(RuntimeError), SqlAlchemyUnitOfWork(session_factory) as uow:
            uow.session.add(DbModelFixtures.user(rolled_back_user_id))
            raise RuntimeError("force rollback")

        with session_factory() as session:
            assert session.get(UserModel, rolled_back_user_id) is None
