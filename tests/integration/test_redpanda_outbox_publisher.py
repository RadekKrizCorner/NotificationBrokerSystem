import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import OutboxEventModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import OutboxEventStatus
from workers.kafka.publisher import AioKafkaProducerClient, KafkaEventPublisher
from workers.outbox.publisher import OutboxPublisher

SessionFactory = sessionmaker[Session]


class RedpandaOutboxFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def topic() -> str:
        return f"notification-events-test-{uuid4().hex}"

    @staticmethod
    def seed_event(session_factory: SessionFactory, *, topic: str) -> UUID:
        event_id = uuid4()
        with session_factory() as session:
            session.add(
                OutboxEventModel(
                    id=event_id,
                    topic=topic,
                    event_type="notification.requested",
                    aggregate_type="notification_request",
                    aggregate_id=uuid4(),
                    event_key=str(event_id),
                    payload={"notification_id": str(event_id), "source_service": "billing"},
                    status=OutboxEventStatus.PENDING.value,
                    next_attempt_at=RedpandaOutboxFixtures.now,
                    created_at=RedpandaOutboxFixtures.now,
                    updated_at=RedpandaOutboxFixtures.now,
                )
            )
            session.commit()
        return event_id


@pytest.mark.integration
class TestRedpandaOutboxPublisher:
    def test_outbox_publisher_publishes_persisted_event_to_redpanda(
        self,
        postgres_session_factory: SessionFactory,
        kafka_bootstrap_servers: str,
    ) -> None:
        topic = RedpandaOutboxFixtures.topic()
        event_id = RedpandaOutboxFixtures.seed_event(postgres_session_factory, topic=topic)
        producer_client = AioKafkaProducerClient(
            bootstrap_servers=kafka_bootstrap_servers,
            client_id="notification-center-test",
        )
        try:
            publisher = OutboxPublisher(
                unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(postgres_session_factory),
                event_publisher=KafkaEventPublisher(producer=producer_client),
                now=lambda: RedpandaOutboxFixtures.now,
                worker_id="redpanda-test-worker",
                lease_duration=timedelta(seconds=30),
                retry_delay=timedelta(seconds=60),
                max_attempts=3,
            )

            result = publisher.publish_due_events(limit=10)
            message = asyncio.run(
                RedpandaMessageConsumer.consume_one(
                    bootstrap_servers=kafka_bootstrap_servers,
                    topic=topic,
                )
            )
        finally:
            producer_client.close()

        assert result.claimed_count == 1
        assert result.published_count == 1
        assert message["key"] == str(event_id)
        assert message["payload"] == {
            "notification_id": str(event_id),
            "source_service": "billing",
        }

        with postgres_session_factory() as session:
            event = session.get(OutboxEventModel, event_id)
            assert event is not None
            assert event.status == OutboxEventStatus.PUBLISHED.value


class RedpandaMessageConsumer:
    @staticmethod
    async def consume_one(*, bootstrap_servers: str, topic: str) -> dict[str, object]:
        aiokafka_module = cast(Any, import_module("aiokafka"))
        consumer = aiokafka_module.AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=f"notification-center-test-{uuid4().hex}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            record = await asyncio.wait_for(consumer.getone(), timeout=15)
            key = cast(bytes, record.key).decode("utf-8")
            payload = cast(Mapping[str, object], json.loads(cast(bytes, record.value)))
            return {"key": key, "payload": payload}
        finally:
            await consumer.stop()
