from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import (
    Base,
    GroupModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    UserGroupModel,
    UserLabelModel,
    UserModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import AudienceType, Channel, DeliveryStatus, Severity
from backend.services.notification_fanout_service import NotificationFanoutService

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


class NotificationFanoutFixtures:
    @staticmethod
    def service(session_factory: SessionFactory) -> NotificationFanoutService:
        return NotificationFanoutService(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            now=lambda: datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
        )

    @staticmethod
    def user(
        *,
        email: str,
        active: bool = True,
        user_id: UUID | None = None,
    ) -> UserModel:
        return UserModel(
            id=user_id or uuid4(),
            email=email,
            display_name=email,
            active=active,
        )

    @staticmethod
    def notification(
        *,
        audience_type: AudienceType,
        audience: dict[str, object],
        channels: tuple[Channel, ...] = (Channel.WEB, Channel.EMAIL),
    ) -> NotificationRequestModel:
        return NotificationRequestModel(
            id=uuid4(),
            source_service="billing",
            message="Billing sync failed",
            severity=Severity.WARNING.value,
            audience_type=audience_type.value,
            audience=audience,
            channels=[channel.value for channel in channels],
            status="accepted",
            idempotency_key=str(uuid4()),
            payload_fingerprint="fingerprint",
        )

    @staticmethod
    def seed_users_and_notification(
        session_factory: SessionFactory,
        *,
        audience_type: AudienceType,
        audience: dict[str, object],
        channels: tuple[Channel, ...] = (Channel.WEB, Channel.EMAIL),
    ) -> UUID:
        with session_factory() as session:
            admin = NotificationFanoutFixtures.user(email="admin@example.test")
            eu = NotificationFanoutFixtures.user(email="eu@example.test")
            us = NotificationFanoutFixtures.user(email="us@example.test")
            inactive = NotificationFanoutFixtures.user(
                email="inactive@example.test",
                active=False,
            )
            group = GroupModel(name="Administrators")
            notification = NotificationFanoutFixtures.notification(
                audience_type=audience_type,
                audience=audience,
                channels=channels,
            )
            session.add_all([admin, eu, us, inactive, group, notification])
            session.flush()
            session.add_all(
                [
                    UserGroupModel(user_id=admin.id, group_id=group.id),
                    UserGroupModel(user_id=inactive.id, group_id=group.id),
                    UserLabelModel(user_id=eu.id, key="region", value="EU"),
                    UserLabelModel(user_id=eu.id, key="tier", value="gold"),
                    UserLabelModel(user_id=admin.id, key="region", value="EU"),
                    UserLabelModel(user_id=admin.id, key="tier", value="silver"),
                    UserLabelModel(user_id=us.id, key="region", value="US"),
                    UserLabelModel(user_id=inactive.id, key="region", value="EU"),
                    UserLabelModel(user_id=inactive.id, key="tier", value="gold"),
                ]
            )
            session.commit()
            return notification.id


class TestNotificationFanoutService:
    @pytest.mark.kwparametrize(
        [
            {
                "id": "all-active-users",
                "audience_type": AudienceType.ALL,
                "audience": {"type": AudienceType.ALL.value},
                "expected_emails": ["admin@example.test", "eu@example.test", "us@example.test"],
            },
            {
                "id": "group-users",
                "audience_type": AudienceType.GROUP,
                "audience": {"type": AudienceType.GROUP.value, "group": "Administrators"},
                "expected_emails": ["admin@example.test"],
            },
            {
                "id": "label-users",
                "audience_type": AudienceType.LABELS,
                "audience": {
                    "type": AudienceType.LABELS.value,
                    "labels": {"region": "EU", "tier": "gold"},
                },
                "expected_emails": ["eu@example.test"],
            },
        ]
    )
    def test_resolves_audience_and_creates_deliveries(
        self,
        session_factory: SessionFactory,
        audience_type: AudienceType,
        audience: dict[str, object],
        expected_emails: list[str],
    ) -> None:
        notification_id = NotificationFanoutFixtures.seed_users_and_notification(
            session_factory,
            audience_type=audience_type,
            audience=audience,
        )
        service = NotificationFanoutFixtures.service(session_factory)

        result = service.fanout_notification(notification_id)

        assert result.recipient_count == len(expected_emails)
        assert result.delivery_count == len(expected_emails) * 2

        with session_factory() as session:
            recipients = session.scalars(
                select(UserModel.email)
                .join(
                    NotificationRecipientModel,
                    NotificationRecipientModel.user_id == UserModel.id,
                )
                .where(NotificationRecipientModel.notification_id == notification_id)
                .order_by(UserModel.email)
            ).all()
            deliveries = session.scalars(
                select(NotificationDeliveryModel).where(
                    NotificationDeliveryModel.notification_id == notification_id
                )
            ).all()
            notification = session.get(NotificationRequestModel, notification_id)

            assert recipients == expected_emails
            assert notification is not None
            assert notification.recipient_count == len(expected_emails)
            assert notification.delivery_count == len(expected_emails) * 2
            assert {delivery.channel for delivery in deliveries} == {
                Channel.WEB.value,
                Channel.EMAIL.value,
            }
            assert {delivery.status for delivery in deliveries} == {DeliveryStatus.PENDING.value}
            assert all(
                delivery.next_attempt_at.replace(tzinfo=UTC) == result.next_attempt_at
                for delivery in deliveries
            )

    def test_is_idempotent_when_reprocessed(
        self,
        session_factory: SessionFactory,
    ) -> None:
        notification_id = NotificationFanoutFixtures.seed_users_and_notification(
            session_factory,
            audience_type=AudienceType.GROUP,
            audience={"type": AudienceType.GROUP.value, "group": "Administrators"},
            channels=(Channel.WEB,),
        )
        service = NotificationFanoutFixtures.service(session_factory)

        first = service.fanout_notification(notification_id)
        second = service.fanout_notification(notification_id)

        assert first.recipient_count == 1
        assert first.delivery_count == 1
        assert second.recipient_count == 1
        assert second.delivery_count == 1

        with session_factory() as session:
            assert session.scalar(select(func.count(NotificationRecipientModel.id))) == 1
            assert session.scalar(select(func.count(NotificationDeliveryModel.id))) == 1

    def test_allows_empty_audience_result(
        self,
        session_factory: SessionFactory,
    ) -> None:
        notification_id = NotificationFanoutFixtures.seed_users_and_notification(
            session_factory,
            audience_type=AudienceType.GROUP,
            audience={"type": AudienceType.GROUP.value, "group": "Missing"},
        )
        service = NotificationFanoutFixtures.service(session_factory)

        result = service.fanout_notification(notification_id)

        assert result.recipient_count == 0
        assert result.delivery_count == 0

        with session_factory() as session:
            notification = session.get(NotificationRequestModel, notification_id)
            assert notification is not None
            assert notification.recipient_count == 0
            assert notification.delivery_count == 0
