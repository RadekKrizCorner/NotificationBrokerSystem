import json
from collections.abc import Mapping

import pytest

from workers.kafka.publisher import KafkaEventPublisher


class RecordingKafkaProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []

    def send_and_wait(self, *, topic: str, key: bytes, value: bytes) -> None:
        self.sent.append((topic, key, value))


class FailingKafkaProducer:
    def send_and_wait(self, *, topic: str, key: bytes, value: bytes) -> None:
        raise RuntimeError("kafka unavailable")


class TestKafkaEventPublisher:
    @pytest.mark.kwparametrize(
        [
            {
                "id": "flat-payload",
                "payload": {"notification_id": "n-1", "source_service": "billing"},
            },
            {
                "id": "nested-payload",
                "payload": {
                    "notification_id": "n-1",
                    "metadata": {"region": "EU", "attempt": 2},
                    "channels": ["web", "email"],
                    "urgent": False,
                    "note": None,
                },
            },
        ]
    )
    def test_publish_serializes_json_payload_and_forwards_topic_key(
        self,
        payload: Mapping[str, object],
    ) -> None:
        producer = RecordingKafkaProducer()
        publisher = KafkaEventPublisher(producer=producer)

        publisher.publish(
            topic="notifications.requests",
            key="notification-1",
            payload=payload,
        )

        assert len(producer.sent) == 1
        topic, key, value = producer.sent[0]
        assert topic == "notifications.requests"
        assert key == b"notification-1"
        assert json.loads(value.decode("utf-8")) == payload
        assert value == json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def test_publish_propagates_producer_failure(self) -> None:
        publisher = KafkaEventPublisher(producer=FailingKafkaProducer())

        with pytest.raises(RuntimeError, match="kafka unavailable"):
            publisher.publish(
                topic="notifications.requests",
                key="notification-1",
                payload={"notification_id": "n-1"},
            )

    def test_publish_rejects_non_json_payload_without_sending(self) -> None:
        producer = RecordingKafkaProducer()
        publisher = KafkaEventPublisher(producer=producer)

        with pytest.raises(TypeError):
            publisher.publish(
                topic="notifications.requests",
                key="notification-1",
                payload={"bad": object()},
            )

        assert producer.sent == []
