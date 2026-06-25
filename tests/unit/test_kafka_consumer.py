import json

import pytest

from workers.kafka.consumer import KafkaNotificationConsumer, KafkaRawMessage


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
        raw_consumer = RecordingRawKafkaConsumer(
            [
                KafkaRawMessage(
                    key=b"notification-1",
                    value=json.dumps({"notification_id": "notification-1"}).encode("utf-8"),
                )
            ]
        )
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        message = consumer.consume_one(timeout_seconds=2.5)

        assert message is not None
        assert message.key == "notification-1"
        assert message.payload == {"notification_id": "notification-1"}
        assert raw_consumer.poll_timeouts == [2.5]

    def test_consume_one_returns_none_when_no_message_arrives(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer([])
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        message = consumer.consume_one(timeout_seconds=1.0)

        assert message is None

    def test_consume_one_rejects_non_json_payload(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer(
            [KafkaRawMessage(key=b"notification-1", value=b"not-json")]
        )
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        with pytest.raises(ValueError, match="valid JSON"):
            consumer.consume_one(timeout_seconds=1.0)

    def test_commit_delegates_to_raw_consumer(self) -> None:
        raw_consumer = RecordingRawKafkaConsumer([])
        consumer = KafkaNotificationConsumer(raw_consumer=raw_consumer)

        consumer.commit()

        assert raw_consumer.commits == 1
