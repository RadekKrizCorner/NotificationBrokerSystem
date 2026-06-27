import json
from datetime import UTC, datetime
from uuid import uuid4

from workers.kafka.consumer import (
    InvalidNotificationKafkaMessage,
    KafkaNotificationConsumer,
    KafkaRawMessage,
)


class RecordingRawKafkaConsumer:
    def __init__(self, messages: list[KafkaRawMessage]) -> None:
        self.messages = messages
        self.commits = 0
        self.poll_timeouts: list[float] = []

    def consume_one(self, *, timeout_seconds: float) -> KafkaRawMessage | None:
        self.poll_timeouts.append(timeout_seconds)
        if not self.messages:
            return None
        return self.messages.pop(0)

    def commit(self) -> None:
        self.commits += 1


class TestKafkaNotificationConsumer:
    def test_consume_one_decodes_json_payload_and_key(self) -> None:
        event_id = uuid4()
        occurred_at = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
        raw_consumer = RecordingRawKafkaConsumer(
            [
                KafkaRawMessage(
                    key=b"notification-1",
                    value=json.dumps(
                        {
                            "schema_version": 1,
                            "event_id": str(event_id),
                            "event_type": "notification.requested",
                            "occurred_at": occurred_at.isoformat(),
                            "data": {"notification_id": "notification-1"},
                        }
                    ).encode("utf-8"),
                )
            ]
        )
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        message = consumer.consume_one(timeout_seconds=2.5)

        assert message is not None
        assert message.key == "notification-1"
        assert message.payload == {"notification_id": "notification-1"}
        assert message.event_id == event_id
        assert message.event_type == "notification.requested"
        assert message.occurred_at == occurred_at
        assert raw_consumer.poll_timeouts == [2.5]

    def test_consume_one_returns_none_when_no_message_arrives(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer([])
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        message = consumer.consume_one(timeout_seconds=1.0)

        assert message is None

    def test_consume_one_classifies_non_json_payload_for_dead_lettering(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer(
            [KafkaRawMessage(key=b"notification-1", value=b"not-json")]
        )
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        message = consumer.consume_one(timeout_seconds=1.0)

        assert isinstance(message, InvalidNotificationKafkaMessage)
        assert message.error_code == "invalid_json"
        assert message.raw_message.value == b"not-json"

    def test_consume_one_rejects_unsupported_schema_version(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer(
            [
                KafkaRawMessage(
                    key=b"notification-1",
                    value=json.dumps({"schema_version": 99}).encode(),
                )
            ]
        )
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        message = consumer.consume_one(timeout_seconds=1.0)

        assert isinstance(message, InvalidNotificationKafkaMessage)
        assert message.error_code == "unsupported_schema_version"

    def test_commit_delegates_to_raw_consumer(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer([])
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        consumer.commit()

        assert raw_consumer.commits == 1
