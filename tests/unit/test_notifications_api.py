from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.core.config import Settings
from backend.db.models import (
    Base,
    NotificationActionInvocationModel,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    OutboxEventModel,
    UserModel,
)
from backend.domain.enums import (
    ActionInvocationResult,
    ActionType,
    AudienceType,
    Channel,
    DeliveryStatus,
    RequestedByType,
    Severity,
)
from backend.main import create_app

SessionFactory = sessionmaker[Session]
JWT_SECRET = "test-secret-long-enough-for-hs256"


class ApiTestTokens:
    @staticmethod
    def service(*, scopes: list[str] | None = None, subject: str = "billing") -> str:
        issued_at = datetime.now(UTC)
        payload = {
            "sub": subject,
            "type": "service",
            "scopes": scopes or ["notifications:write"],
            "iat": issued_at,
            "exp": issued_at + timedelta(minutes=5),
            "iss": "notification-center",
            "aud": "notification-center-api",
        }
        return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

    @staticmethod
    def user(subject: str = "user-1", *, scopes: list[str] | None = None) -> str:
        issued_at = datetime.now(UTC)
        payload = {
            "sub": subject,
            "type": "user",
            "scopes": scopes if scopes is not None else ["notifications:read"],
            "iat": issued_at,
            "exp": issued_at + timedelta(minutes=5),
            "iss": "notification-center",
            "aud": "notification-center-api",
        }
        return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


class NotificationApiFixtures:
    @staticmethod
    def notification_payload() -> dict[str, object]:
        return {
            "message": "Billing sync failed",
            "severity": "warning",
            "audience": {
                "type": "labels",
                "labels": {"region": "EU"},
            },
            "channels": ["web", "email"],
            "idempotency_key": "billing-sync-1",
        }

    @staticmethod
    def seed_web_notifications(session_factory: SessionFactory) -> UUID:
        with session_factory() as session:
            return NotificationApiFixtures._seed_web_notifications(session)

    @staticmethod
    def web_delivery_id_by_message(session_factory: SessionFactory, message: str) -> UUID:
        with session_factory() as session:
            delivery_id = session.scalar(
                select(NotificationDeliveryModel.id)
                .join(
                    NotificationRequestModel,
                    NotificationRequestModel.id == NotificationDeliveryModel.notification_id,
                )
                .where(NotificationRequestModel.message == message)
            )
            assert delivery_id is not None
            return delivery_id

    @staticmethod
    def seed_retryable_web_notification(
        session_factory: SessionFactory,
        *,
        source_service: str = "billing",
        email_status: DeliveryStatus = DeliveryStatus.FAILED_RETRYABLE,
    ) -> tuple[UUID, UUID, UUID]:
        with session_factory() as session:
            user = UserModel(email="retry-user@example.test", display_name="Retry User")
            session.add(user)
            session.flush()
            base_time = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)
            notification = NotificationRequestModel(
                source_service=source_service,
                message="Retryable notification",
                severity=Severity.ERROR.value,
                audience_type=AudienceType.ALL.value,
                audience={"type": AudienceType.ALL.value},
                channels=[Channel.WEB.value, Channel.EMAIL.value],
                status="accepted",
                idempotency_key=f"{source_service}-retryable",
                payload_fingerprint=f"{source_service}-retryable-fingerprint",
                created_at=base_time,
                updated_at=base_time,
            )
            session.add(notification)
            session.flush()
            recipient = NotificationRecipientModel(
                notification_id=notification.id,
                user_id=user.id,
            )
            session.add(recipient)
            session.flush()
            web_delivery = NotificationDeliveryModel(
                notification_recipient_id=recipient.id,
                notification_id=notification.id,
                user_id=user.id,
                channel=Channel.WEB.value,
                status=DeliveryStatus.DELIVERED.value,
                next_attempt_at=base_time,
                delivered_at=base_time + timedelta(minutes=1),
                created_at=base_time,
                updated_at=base_time,
            )
            email_delivery = NotificationDeliveryModel(
                notification_recipient_id=recipient.id,
                notification_id=notification.id,
                user_id=user.id,
                channel=Channel.EMAIL.value,
                status=email_status.value,
                next_attempt_at=base_time,
                delivered_at=(
                    base_time + timedelta(minutes=2)
                    if email_status is DeliveryStatus.DELIVERED
                    else None
                ),
                created_at=base_time,
                updated_at=base_time,
            )
            session.add_all([web_delivery, email_delivery])
            session.commit()
            return user.id, notification.id, web_delivery.id

    @staticmethod
    def _seed_web_notifications(session: Session) -> UUID:
        user = UserModel(email="user@example.test", display_name="User")
        other_user = UserModel(email="other@example.test", display_name="Other")
        session.add_all([user, other_user])
        session.flush()
        base_time = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)
        scenarios = [
            (
                "Newest unread",
                user.id,
                DeliveryStatus.DELIVERED,
                base_time + timedelta(minutes=3),
                None,
            ),
            (
                "Read middle",
                user.id,
                DeliveryStatus.DELIVERED,
                base_time + timedelta(minutes=2),
                base_time + timedelta(minutes=4),
            ),
            (
                "Older unread",
                user.id,
                DeliveryStatus.DELIVERED,
                base_time + timedelta(minutes=1),
                None,
            ),
            ("Pending hidden", user.id, DeliveryStatus.PENDING, None, None),
            (
                "Other user hidden",
                other_user.id,
                DeliveryStatus.DELIVERED,
                base_time + timedelta(minutes=5),
                None,
            ),
        ]
        for index, (message, scenario_user_id, status, delivered_at, read_at) in enumerate(
            scenarios
        ):
            NotificationApiFixtures._add_web_notification(
                session,
                index=index,
                message=message,
                user_id=scenario_user_id,
                status=status,
                delivered_at=delivered_at,
                read_at=read_at,
                base_time=base_time,
            )
        session.commit()
        return user.id

    @staticmethod
    def _add_web_notification(
        session: Session,
        *,
        index: int,
        message: str,
        user_id: UUID,
        status: DeliveryStatus,
        delivered_at: datetime | None,
        read_at: datetime | None,
        base_time: datetime,
    ) -> None:
        notification = NotificationRequestModel(
            source_service="billing",
            message=message,
            severity=Severity.WARNING.value,
            audience_type=AudienceType.ALL.value,
            audience={"type": AudienceType.ALL.value},
            channels=[Channel.WEB.value],
            status="accepted",
            idempotency_key=f"visible-{index}",
            payload_fingerprint=f"fingerprint-{index}",
            created_at=base_time - timedelta(hours=index),
            updated_at=base_time - timedelta(hours=index),
        )
        session.add(notification)
        session.flush()
        recipient = NotificationRecipientModel(
            notification_id=notification.id,
            user_id=user_id,
        )
        session.add(recipient)
        session.flush()
        session.add(
            NotificationDeliveryModel(
                notification_recipient_id=recipient.id,
                notification_id=notification.id,
                user_id=user_id,
                channel=Channel.WEB.value,
                status=status.value,
                next_attempt_at=base_time,
                delivered_at=delivered_at,
                read_at=read_at,
                created_at=notification.created_at,
                updated_at=notification.updated_at,
            )
        )


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
def client(session_factory: SessionFactory) -> TestClient:
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        jwt_secret=JWT_SECRET,
        fallback_deduplication_window_seconds=600,
    )
    app = create_app(
        settings=settings,
        session_factory=session_factory,
        now=lambda: datetime(2026, 6, 24, 12, 7, tzinfo=UTC),
    )
    return TestClient(app)


class TestCreateNotificationApi:
    def test_rejects_token_missing_registered_claims(self, client: TestClient) -> None:
        token = jwt.encode(
            {
                "sub": "billing",
                "type": "service",
                "scopes": ["notifications:write"],
            },
            JWT_SECRET,
            algorithm="HS256",
        )

        response = client.post(
            "/notifications",
            json=NotificationApiFixtures.notification_payload(),
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401

    def test_rejects_request_body_over_limit(self, client: TestClient) -> None:
        token = ApiTestTokens.service()

        response = client.post(
            "/notifications",
            content=b'{"message":"' + (b"x" * 70_000) + b'"}',
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

        assert response.status_code == 413

    def test_accepts_service_notification(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        response = client.post(
            "/notifications",
            json=NotificationApiFixtures.notification_payload(),
            headers={"Authorization": f"Bearer {ApiTestTokens.service()}"},
        )

        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "accepted"
        assert body["deduplicated"] is False
        assert body["recipient_count"] == 0
        assert body["delivery_count"] == 0

        with session_factory() as session:
            notification = session.scalar(select(NotificationRequestModel))
            assert notification is not None
            assert str(notification.id) == body["notification_id"]
            assert notification.source_service == "billing"
            assert notification.idempotency_key == "billing-sync-1"
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 1

    def test_returns_existing_for_duplicate_idempotency_key(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        headers = {"Authorization": f"Bearer {ApiTestTokens.service()}"}
        first = client.post(
            "/notifications",
            json=NotificationApiFixtures.notification_payload(),
            headers=headers,
        )
        second = client.post(
            "/notifications",
            json=NotificationApiFixtures.notification_payload(),
            headers=headers,
        )

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json()["deduplicated"] is True
        assert second.json()["notification_id"] == first.json()["notification_id"]

        with session_factory() as session:
            assert session.scalar(select(func.count(NotificationRequestModel.id))) == 1
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 1

    @pytest.mark.kwparametrize(
        [
            {
                "id": "missing-token",
                "headers": {},
                "expected_status": 401,
            },
            {
                "id": "user-token",
                "headers": {"Authorization": f"Bearer {ApiTestTokens.user()}"},
                "expected_status": 403,
            },
            {
                "id": "missing-scope",
                "headers": {
                    "Authorization": (
                        f"Bearer {ApiTestTokens.service(scopes=['notifications:read'])}"
                    ),
                },
                "expected_status": 403,
            },
        ]
    )
    def test_requires_service_write_scope(
        self,
        client: TestClient,
        headers: dict[str, str],
        expected_status: int,
    ) -> None:
        response = client.post(
            "/notifications",
            json=NotificationApiFixtures.notification_payload(),
            headers=headers,
        )

        assert response.status_code == expected_status


class TestUserNotificationApi:
    def test_lists_visible_web_notifications(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        user_id = NotificationApiFixtures.seed_web_notifications(session_factory)

        response = client.get(
            "/me/notifications",
            params={"status": "unread", "limit": 10},
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(user_id))}"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["next_cursor"] is None
        assert [item["message"] for item in body["items"]] == ["Newest unread", "Older unread"]
        assert all(item["severity"] == Severity.WARNING.value for item in body["items"])

    def test_uses_cursor_pagination(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        user_id = NotificationApiFixtures.seed_web_notifications(session_factory)
        headers = {"Authorization": f"Bearer {ApiTestTokens.user(str(user_id))}"}

        first = client.get("/me/notifications", params={"limit": 1}, headers=headers)
        assert first.status_code == 200
        assert [item["message"] for item in first.json()["items"]] == ["Newest unread"]
        assert first.json()["next_cursor"] is not None

        second = client.get(
            "/me/notifications",
            params={"limit": 10, "cursor": first.json()["next_cursor"]},
            headers=headers,
        )

        assert second.status_code == 200
        assert [item["message"] for item in second.json()["items"]] == [
            "Read middle",
            "Older unread",
        ]
        assert second.json()["next_cursor"] is None

    @pytest.mark.kwparametrize(
        [
            {
                "id": "missing-token",
                "headers": {},
                "expected_status": 401,
            },
            {
                "id": "service-token",
                "headers": {"Authorization": f"Bearer {ApiTestTokens.service()}"},
                "expected_status": 403,
            },
            {
                "id": "missing-scope",
                "headers": {
                    "Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()), scopes=[])}",
                },
                "expected_status": 403,
            },
        ]
    )
    def test_requires_user_read_scope(
        self,
        client: TestClient,
        headers: dict[str, str],
        expected_status: int,
    ) -> None:
        response = client.get("/me/notifications", headers=headers)

        assert response.status_code == expected_status

    def test_rejects_invalid_cursor(self, client: TestClient) -> None:
        response = client.get(
            "/me/notifications",
            params={"cursor": "invalid"},
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()))}"},
        )

        assert response.status_code == 400

    def test_marks_visible_web_notification_read_idempotently(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        user_id = NotificationApiFixtures.seed_web_notifications(session_factory)
        delivery_id = NotificationApiFixtures.web_delivery_id_by_message(
            session_factory,
            "Older unread",
        )
        headers = {"Authorization": f"Bearer {ApiTestTokens.user(str(user_id))}"}

        first = client.post(f"/me/notifications/{delivery_id}/read", headers=headers)
        second = client.post(f"/me/notifications/{delivery_id}/read", headers=headers)

        assert first.status_code == 200
        assert second.status_code == 200
        assert first.json() == second.json()
        assert first.json()["id"] == str(delivery_id)
        assert first.json()["read_at"] == "2026-06-24T12:07:00+00:00"

        with session_factory() as session:
            delivery = session.get(NotificationDeliveryModel, delivery_id)
            assert delivery is not None
            assert delivery.read_at is not None

    def test_mark_read_hides_other_users_web_notification(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        NotificationApiFixtures.seed_web_notifications(session_factory)
        delivery_id = NotificationApiFixtures.web_delivery_id_by_message(
            session_factory,
            "Older unread",
        )

        response = client.post(
            f"/me/notifications/{delivery_id}/read",
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()))}"},
        )

        assert response.status_code == 404

    def test_mark_read_rejects_hidden_pending_web_notification(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        user_id = NotificationApiFixtures.seed_web_notifications(session_factory)
        delivery_id = NotificationApiFixtures.web_delivery_id_by_message(
            session_factory,
            "Pending hidden",
        )

        response = client.post(
            f"/me/notifications/{delivery_id}/read",
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(user_id))}"},
        )

        assert response.status_code == 404

    @pytest.mark.kwparametrize(
        [
            {
                "id": "missing-token",
                "headers": {},
                "expected_status": 401,
            },
            {
                "id": "service-token",
                "headers": {"Authorization": f"Bearer {ApiTestTokens.service()}"},
                "expected_status": 403,
            },
            {
                "id": "missing-scope",
                "headers": {
                    "Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()), scopes=[])}",
                },
                "expected_status": 403,
            },
        ]
    )
    def test_mark_read_requires_user_read_scope(
        self,
        client: TestClient,
        session_factory: SessionFactory,
        headers: dict[str, str],
        expected_status: int,
    ) -> None:
        NotificationApiFixtures.seed_web_notifications(session_factory)
        delivery_id = NotificationApiFixtures.web_delivery_id_by_message(
            session_factory,
            "Older unread",
        )

        response = client.post(f"/me/notifications/{delivery_id}/read", headers=headers)

        assert response.status_code == expected_status

    def test_user_retry_replays_failed_sibling_delivery(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        user_id, _notification_id, web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(session_factory)
        )

        response = client.post(
            f"/me/notifications/{web_delivery_id}/actions/retry",
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(user_id))}"},
        )

        assert response.status_code == 202
        body = response.json()
        assert body["status"] == ActionInvocationResult.QUEUED.value
        assert body["replay_id"] is not None
        assert body["replayed_delivery_count"] == 1

        with session_factory() as session:
            email_delivery = session.scalar(
                select(NotificationDeliveryModel).where(
                    NotificationDeliveryModel.channel == Channel.EMAIL.value
                )
            )
            web_delivery = session.get(NotificationDeliveryModel, web_delivery_id)
            invocation = session.scalar(select(NotificationActionInvocationModel))
            outbox_event = session.scalar(select(OutboxEventModel))

            assert email_delivery is not None
            assert email_delivery.status == DeliveryStatus.REPLAY_REQUESTED.value
            assert str(email_delivery.replay_id) == body["replay_id"]
            assert web_delivery is not None
            assert web_delivery.status == DeliveryStatus.DELIVERED.value
            assert web_delivery.replay_id is None
            assert invocation is not None
            assert invocation.web_delivery_id == web_delivery_id
            assert invocation.requested_by_type == RequestedByType.USER.value
            assert invocation.requested_by_id == str(user_id)
            assert invocation.action_type == ActionType.RETRY.value
            assert outbox_event is not None
            assert outbox_event.payload["delivery_ids"] == [str(email_delivery.id)]

    def test_user_retry_hides_other_users_web_notification(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        _user_id, _notification_id, web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(session_factory)
        )

        response = client.post(
            f"/me/notifications/{web_delivery_id}/actions/retry",
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()))}"},
        )

        assert response.status_code == 404

    def test_user_retry_records_no_eligible_for_delivered_sibling_delivery(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        user_id, _notification_id, web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(
                session_factory,
                email_status=DeliveryStatus.DELIVERED,
            )
        )

        response = client.post(
            f"/me/notifications/{web_delivery_id}/actions/retry",
            headers={"Authorization": f"Bearer {ApiTestTokens.user(str(user_id))}"},
        )

        assert response.status_code == 200
        assert response.json() == {
            "replay_id": None,
            "status": ActionInvocationResult.NO_ELIGIBLE.value,
            "replayed_delivery_count": 0,
        }

        with session_factory() as session:
            invocation = session.scalar(select(NotificationActionInvocationModel))
            assert invocation is not None
            assert invocation.result == ActionInvocationResult.NO_ELIGIBLE.value
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 0

    @pytest.mark.kwparametrize(
        [
            {
                "id": "missing-token",
                "headers": {},
                "expected_status": 401,
            },
            {
                "id": "service-token",
                "headers": {"Authorization": f"Bearer {ApiTestTokens.service()}"},
                "expected_status": 403,
            },
            {
                "id": "missing-scope",
                "headers": {
                    "Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()), scopes=[])}",
                },
                "expected_status": 403,
            },
        ]
    )
    def test_user_retry_requires_user_read_scope(
        self,
        client: TestClient,
        session_factory: SessionFactory,
        headers: dict[str, str],
        expected_status: int,
    ) -> None:
        _user_id, _notification_id, web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(session_factory)
        )

        response = client.post(
            f"/me/notifications/{web_delivery_id}/actions/retry",
            headers=headers,
        )

        assert response.status_code == expected_status


class TestServiceRetryNotificationApi:
    def test_same_source_service_retries_notification(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        _user_id, notification_id, _web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(session_factory)
        )

        response = client.post(
            f"/notifications/{notification_id}/retry",
            headers={"Authorization": f"Bearer {ApiTestTokens.service(subject='billing')}"},
        )

        assert response.status_code == 202
        body = response.json()
        assert body["status"] == ActionInvocationResult.QUEUED.value
        assert body["replay_id"] is not None
        assert body["replayed_delivery_count"] == 1

    def test_retry_any_service_scope_retries_other_source_notification(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        _user_id, notification_id, _web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(session_factory)
        )

        response = client.post(
            f"/notifications/{notification_id}/retry",
            headers={
                "Authorization": (
                    "Bearer "
                    f"{ApiTestTokens.service(subject='ops', scopes=['notifications:retry:any'])}"
                ),
            },
        )

        assert response.status_code == 202
        assert response.json()["status"] == ActionInvocationResult.QUEUED.value

    def test_service_retry_records_no_eligible_without_outbox_event(
        self,
        client: TestClient,
        session_factory: SessionFactory,
    ) -> None:
        _user_id, notification_id, _web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(
                session_factory,
                email_status=DeliveryStatus.DELIVERED,
            )
        )

        response = client.post(
            f"/notifications/{notification_id}/retry",
            headers={"Authorization": f"Bearer {ApiTestTokens.service(subject='billing')}"},
        )

        assert response.status_code == 200
        assert response.json() == {
            "replay_id": None,
            "status": ActionInvocationResult.NO_ELIGIBLE.value,
            "replayed_delivery_count": 0,
        }

        with session_factory() as session:
            invocation = session.scalar(select(NotificationActionInvocationModel))
            assert invocation is not None
            assert invocation.result == ActionInvocationResult.NO_ELIGIBLE.value
            assert session.scalar(select(func.count(OutboxEventModel.id))) == 0

    def test_service_retry_rejects_missing_notification(self, client: TestClient) -> None:
        response = client.post(
            f"/notifications/{uuid4()}/retry",
            headers={"Authorization": f"Bearer {ApiTestTokens.service()}"},
        )

        assert response.status_code == 404

    @pytest.mark.kwparametrize(
        [
            {
                "id": "missing-token",
                "headers": {},
                "expected_status": 401,
            },
            {
                "id": "user-token",
                "headers": {"Authorization": f"Bearer {ApiTestTokens.user(str(uuid4()))}"},
                "expected_status": 403,
            },
            {
                "id": "different-source",
                "headers": {"Authorization": f"Bearer {ApiTestTokens.service(subject='crm')}"},
                "expected_status": 403,
            },
            {
                "id": "missing-scope",
                "headers": {
                    "Authorization": (
                        f"Bearer {ApiTestTokens.service(scopes=['notifications:read'])}"
                    ),
                },
                "expected_status": 403,
            },
        ]
    )
    def test_service_retry_enforces_authorization(
        self,
        client: TestClient,
        session_factory: SessionFactory,
        headers: dict[str, str],
        expected_status: int,
    ) -> None:
        _user_id, notification_id, _web_delivery_id = (
            NotificationApiFixtures.seed_retryable_web_notification(session_factory)
        )

        response = client.post(f"/notifications/{notification_id}/retry", headers=headers)

        assert response.status_code == expected_status
