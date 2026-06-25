from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import (
    NotificationDeliveryModel,
    NotificationRecipientModel,
    NotificationRequestModel,
    OutboxEventModel,
    ProcessedEventModel,
    UserModel,
)
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import (
    AudienceType,
    Channel,
    NotificationRequestStatus,
    OutboxEventStatus,
    Severity,
)
from backend.services.notification_fanout_service import NotificationFanoutService
from workers.kafka.consumer import AioKafkaConsumerClient, KafkaNotificationConsumer
from workers.kafka.publisher import AioKafkaProducerClient, KafkaEventPublisher
from workers.notifications.consumer import NotificationConsumerWorker
from workers.notifications.requested import NotificationRequestedHandler
from workers.outbox.publisher import OutboxPublisher

SessionFactory = sessionmaker[Session]


class RedpandaNotificationConsumerFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def topic() -> str:
        return f"notification-consumer-test-{uuid4().hex}"

    @staticmethod
    def seed_notification(session_factory: SessionFactory, *, topic: str) -> UUID:
        with session_factory() as session:
            notification = NotificationRequestModel(
                source_service="billing",
                message="Consumer integration notification",
                severity=Severity.WARNING.value,
                audience_type=AudienceType.ALL.value,
                audience={"type": AudienceType.ALL.value},
                channels=[Channel.WEB.value, Channel.EMAIL.value],
                status=NotificationRequestStatus.ACCEPTED.value,
                idempotency_key=str(uuid4()),
                payload_fingerprint=str(uuid4()),
                created_at=RedpandaNotificationConsumerFixtures.now,
                updated_at=RedpandaNotificationConsumerFixtures.now,
            )
            users = [
                UserModel(email="consumer-1@example.test", display_name="Consumer 1"),
                UserModel(email="consumer-2@example.test", display_name="Consumer 2"),
            ]
            session.add_all([notification, *users])
            session.flush()
            session.add(
                OutboxEventModel(
                    topic=topic,
                    event_type="notification.requested",
                    aggregate_type="notification_request",
                    aggregate_id=notification.id,
                    event_key=str(notification.id),
                    payload={
                        "notification_id": str(notification.id),
                        "source_service": "billing",
                    },
                    status=OutboxEventStatus.PENDING.value,
                    next_attempt_at=RedpandaNotificationConsumerFixtures.now,
                    created_at=RedpandaNotificationConsumerFixtures.now,
                    updated_at=RedpandaNotificationConsumerFixtures.now,
                )
            )
            session.commit()
            return notification.id


@pytest.mark.integration
class TestRedpandaNotificationConsumer:
    def test_notification_requested_event_is_consumed_and_fanned_out(
        self,
        postgres_session_factory: SessionFactory,
        kafka_bootstrap_servers: str,
    ) -> None:
        topic = RedpandaNotificationConsumerFixtures.topic()
        notification_id = RedpandaNotificationConsumerFixtures.seed_notification(
            postgres_session_factory,
            topic=topic,
        )
        producer_client = AioKafkaProducerClient(
            bootstrap_servers=kafka_bootstrap_servers,
            client_id="notification-consumer-producer-test",
        )
        consumer_client = AioKafkaConsumerClient(
            topic=topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=f"notification-consumer-test-{uuid4().hex}",
            client_id="notification-consumer-test",
        )
        try:
            publisher = OutboxPublisher(
                unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(postgres_session_factory),
                event_publisher=KafkaEventPublisher(producer=producer_client),
                now=lambda: RedpandaNotificationConsumerFixtures.now,
                worker_id="redpanda-outbox-worker",
                lease_duration=timedelta(seconds=30),
                retry_delay=timedelta(seconds=60),
                max_attempts=3,
            )
            fanout_service = NotificationFanoutService(
                unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(postgres_session_factory),
                now=lambda: RedpandaNotificationConsumerFixtures.now,
            )
            worker = NotificationConsumerWorker(
                event_consumer=KafkaNotificationConsumer(raw_consumer=consumer_client),
                handler=NotificationRequestedHandler(fanout_service=fanout_service),
                unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(postgres_session_factory),
                consumer_name="notification-requested-consumer",
                now=lambda: RedpandaNotificationConsumerFixtures.now,
                batch_size=1,
                poll_timeout_seconds=15,
            )

            publish_result = publisher.publish_due_events(limit=10)
            consume_result = worker.run_once()
        finally:
            producer_client.close()
            consumer_client.close()

        assert publish_result.published_count == 1
        assert consume_result.received_count == 1
        assert consume_result.processed_count == 1
        assert consume_result.committed_count == 1

        with postgres_session_factory() as session:
            assert session.scalar(select(func.count(NotificationRecipientModel.id))) == 2
            assert session.scalar(select(func.count(NotificationDeliveryModel.id))) == 4
            processed_event = session.get(
                ProcessedEventModel,
                (notification_id, "notification-requested-consumer"),
            )
            assert processed_event is not None
