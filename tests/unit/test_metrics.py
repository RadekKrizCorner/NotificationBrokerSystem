from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app_factory import BackendApplicationFactory
from backend.core.config import Settings
from backend.core.metrics import PrometheusMetrics
from backend.db.models import (
    Base,
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    OutboxEventModel,
    UserModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    AudienceType,
    Channel,
    DeliveryStatus,
    NotificationRequestStatus,
    OutboxEventStatus,
    Severity,
)
from workers.outbox.publisher import OutboxPublisher

SessionFactory = sessionmaker[Session]


class RecordingEventPublisher:
    def publish(self, *, topic: str, key: str, payload: Mapping[str, object]) -> None:
        return None


class MetricsFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def settings() -> Settings:
        return Settings(
            database_url="sqlite+pysqlite:///:memory:",
            jwt_secret="test-secret-long-enough",
        )

    @staticmethod
    def session_factory() -> SessionFactory:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine, expire_on_commit=False)

    @staticmethod
    def seed_outbox_event(
        session_factory: SessionFactory,
        *,
        created_at: datetime,
        next_attempt_at: datetime,
        status: OutboxEventStatus = OutboxEventStatus.PENDING,
    ) -> None:
        with session_factory() as session:
            event_id = uuid4()
            session.add(
                OutboxEventModel(
                    id=event_id,
                    topic="notifications.requests",
                    event_type="notification.requested",
                    aggregate_type="notification_request",
                    aggregate_id=uuid4(),
                    event_key=str(event_id),
                    payload={"notification_id": str(event_id)},
                    status=status.value,
                    next_attempt_at=next_attempt_at,
                    created_at=created_at,
                    updated_at=created_at,
                )
            )
            session.commit()

    @staticmethod
    def seed_pipeline_statuses(session_factory: SessionFactory) -> None:
        with session_factory() as session:
            user = UserModel(
                email="metrics-user@example.test",
                display_name="Metrics User",
            )
            notification = NotificationRequestModel(
                source_service="metrics-test",
                message="Metrics test notification",
                severity=Severity.INFO.value,
                audience_type=AudienceType.ALL.value,
                audience={"type": AudienceType.ALL.value},
                channels=[Channel.WEB.value, Channel.EMAIL.value],
                status=NotificationRequestStatus.ACCEPTED.value,
                idempotency_key="metrics-test-1",
                payload_fingerprint="a" * 64,
                created_at=MetricsFixtures.now,
                updated_at=MetricsFixtures.now,
            )
            session.add_all([user, notification])
            session.flush()

            recipient = NotificationRecipientModel(
                notification_id=notification.id,
                user_id=user.id,
                created_at=MetricsFixtures.now,
            )
            session.add(recipient)
            session.flush()

            session.add_all(
                [
                    NotificationDeliveryModel(
                        notification_recipient_id=recipient.id,
                        notification_id=notification.id,
                        user_id=user.id,
                        channel=Channel.WEB.value,
                        status=DeliveryStatus.PENDING.value,
                        next_attempt_at=MetricsFixtures.now,
                        created_at=MetricsFixtures.now,
                        updated_at=MetricsFixtures.now,
                    ),
                    NotificationDeliveryModel(
                        notification_recipient_id=recipient.id,
                        notification_id=notification.id,
                        user_id=user.id,
                        channel=Channel.EMAIL.value,
                        status=DeliveryStatus.DELIVERED.value,
                        next_attempt_at=MetricsFixtures.now,
                        delivered_at=MetricsFixtures.now,
                        created_at=MetricsFixtures.now,
                        updated_at=MetricsFixtures.now,
                    ),
                ]
            )
            session.commit()


class TestPrometheusMetrics:
    def test_renders_http_and_outbox_metrics(self) -> None:
        metrics = PrometheusMetrics()

        metrics.record_http_request(
            method="GET",
            path="/notifications",
            status_code=202,
            duration_seconds=0.25,
        )
        metrics.record_outbox_event(
            event_type="notification.requested",
            status=OutboxEventStatus.PUBLISHED.value,
        )
        metrics.set_outbox_oldest_pending_seconds(42.0)

        body = metrics.render().decode("utf-8")

        assert (
            'notification_http_requests_total{method="GET",path="/notifications",status="202"} '
            "1.0"
        ) in body
        assert "notification_http_request_duration_seconds_bucket" in body
        assert (
            'notification_outbox_events_total{event_type="notification.requested",'
            'status="published"} 1.0'
        ) in body
        assert "notification_outbox_oldest_pending_seconds 42.0" in body

    def test_renders_backend_process_metrics(self) -> None:
        metrics = PrometheusMetrics()

        body = metrics.render().decode("utf-8")

        assert "notification_backend_process_cpu_seconds_total" in body
        assert "notification_backend_process_resident_memory_bytes" in body

    def test_api_exposes_metrics_endpoint_and_records_request_metrics(self) -> None:
        app = BackendApplicationFactory(
            settings=MetricsFixtures.settings(),
            session_factory=MetricsFixtures.session_factory(),
            metrics=PrometheusMetrics(),
        ).create()
        client = TestClient(app)

        response = client.get("/openapi.json")
        metrics_response = client.get("/metrics")

        assert response.status_code == 200
        assert metrics_response.status_code == 200
        assert "text/plain" in metrics_response.headers["content-type"]
        assert (
            'notification_http_requests_total{method="GET",path="/openapi.json",status="200"} '
            "1.0"
        ) in metrics_response.text

    def test_api_metrics_endpoint_refreshes_pipeline_status_gauges(self) -> None:
        session_factory = MetricsFixtures.session_factory()
        MetricsFixtures.seed_pipeline_statuses(session_factory)
        MetricsFixtures.seed_outbox_event(
            session_factory,
            created_at=MetricsFixtures.now - timedelta(seconds=30),
            next_attempt_at=MetricsFixtures.now - timedelta(seconds=1),
            status=OutboxEventStatus.PENDING,
        )
        MetricsFixtures.seed_outbox_event(
            session_factory,
            created_at=MetricsFixtures.now,
            next_attempt_at=MetricsFixtures.now,
            status=OutboxEventStatus.PUBLISHED,
        )
        app = BackendApplicationFactory(
            settings=MetricsFixtures.settings(),
            session_factory=session_factory,
            now=lambda: MetricsFixtures.now,
            metrics=PrometheusMetrics(),
        ).create()
        client = TestClient(app)

        response = client.get("/metrics")

        assert response.status_code == 200
        assert 'notification_requests_by_status{status="accepted"} 1.0' in response.text
        assert 'notification_outbox_events_by_status{status="pending"} 1.0' in response.text
        assert 'notification_outbox_events_by_status{status="published"} 1.0' in response.text
        assert 'notification_deliveries_by_status{status="pending"} 1.0' in response.text
        assert 'notification_deliveries_by_status{status="delivered"} 1.0' in response.text
        assert (
            'notification_deliveries_by_channel_status{channel="web",status="pending"} 1.0'
            in response.text
        )
        assert (
            'notification_deliveries_by_channel_status{channel="email",status="delivered"} 1.0'
            in response.text
        )
        assert "notification_outbox_oldest_pending_seconds 30.0" in response.text

    def test_outbox_publisher_records_event_status_and_backlog_age(self) -> None:
        session_factory = MetricsFixtures.session_factory()
        metrics = PrometheusMetrics()
        MetricsFixtures.seed_outbox_event(
            session_factory,
            created_at=MetricsFixtures.now,
            next_attempt_at=MetricsFixtures.now - timedelta(seconds=1),
        )
        MetricsFixtures.seed_outbox_event(
            session_factory,
            created_at=MetricsFixtures.now - timedelta(minutes=5),
            next_attempt_at=MetricsFixtures.now,
        )
        publisher = OutboxPublisher(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            event_publisher=RecordingEventPublisher(),
            now=lambda: MetricsFixtures.now,
            worker_id="metrics-worker",
            lease_duration=timedelta(seconds=30),
            retry_delay=timedelta(seconds=60),
            max_attempts=3,
            metrics=metrics,
        )

        result = publisher.publish_due_events(limit=1)

        body = metrics.render().decode("utf-8")
        assert result.published_count == 1
        assert (
            'notification_outbox_events_total{event_type="notification.requested",'
            'status="published"} 1.0'
        ) in body
        assert "notification_outbox_oldest_pending_seconds 300.0" in body
