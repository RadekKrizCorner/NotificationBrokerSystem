import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from threading import Thread
from typing import Any, Protocol, cast
from uuid import UUID


@dataclass(frozen=True, slots=True)
class KafkaRawMessage:
    key: bytes | None
    value: bytes


@dataclass(frozen=True, slots=True)
class NotificationKafkaMessage:
    key: str
    event_id: UUID
    event_type: str
    occurred_at: datetime
    payload: Mapping[str, object]
    raw_message: KafkaRawMessage


@dataclass(frozen=True, slots=True)
class InvalidNotificationKafkaMessage:
    raw_message: KafkaRawMessage
    error_code: str
    error_message: str


class EnvelopeDecodeError(ValueError):
    def __init__(self, *, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class RawKafkaConsumerClient(Protocol):
    def consume_one(self, *, timeout_seconds: float) -> KafkaRawMessage | None:
        pass

    def commit(self) -> None:
        pass


class AsyncKafkaRecord(Protocol):
    key: bytes | None
    value: bytes


class AsyncKafkaConsumer(Protocol):
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def getone(self) -> AsyncKafkaRecord:
        pass

    async def commit(self) -> None:
        pass


class AsyncKafkaConsumerFactory(Protocol):
    def __call__(
        self,
        topic: str,
        *,
        bootstrap_servers: str,
        group_id: str,
        client_id: str,
        auto_offset_reset: str,
        enable_auto_commit: bool,
    ) -> AsyncKafkaConsumer:
        pass


class KafkaNotificationConsumer:
    _supported_event_types = frozenset({"notification.requested", "notification.replay_requested"})

    def __init__(self, *, raw_consumer: RawKafkaConsumerClient) -> None:
        self._raw_consumer = raw_consumer

    def consume_one(
        self,
        *,
        timeout_seconds: float,
    ) -> NotificationKafkaMessage | InvalidNotificationKafkaMessage | None:
        raw_message = self._raw_consumer.consume_one(timeout_seconds=timeout_seconds)
        if raw_message is None:
            return None

        try:
            return self._decode_message(raw_message)
        except EnvelopeDecodeError as exc:
            return InvalidNotificationKafkaMessage(
                raw_message=raw_message,
                error_code=exc.code,
                error_message=str(exc),
            )

    def commit(self) -> None:
        self._raw_consumer.commit()

    def _decode_message(
        self,
        raw_message: KafkaRawMessage,
    ) -> NotificationKafkaMessage:
        envelope = self._decode_payload(raw_message.value)

        schema_version = envelope.get("schema_version")
        if schema_version != 1:
            raise EnvelopeDecodeError(
                code="unsupported_schema_version",
                message="Kafka notification schema_version must be 1",
            )

        raw_event_id = envelope.get("event_id")
        try:
            event_id = UUID(raw_event_id) if isinstance(raw_event_id, str) else None
        except ValueError as exc:
            raise EnvelopeDecodeError(
                code="invalid_event_id",
                message="Kafka notification event_id must be a UUID",
            ) from exc
        if event_id is None:
            raise EnvelopeDecodeError(
                code="invalid_event_id",
                message="Kafka notification event_id must be a UUID",
            )

        event_type = envelope.get("event_type")
        if not isinstance(event_type, str) or event_type not in self._supported_event_types:
            raise EnvelopeDecodeError(
                code="unsupported_event_type",
                message="Kafka notification event_type is unsupported",
            )

        raw_occurred_at = envelope.get("occurred_at")
        try:
            occurred_at = (
                datetime.fromisoformat(raw_occurred_at)
                if isinstance(raw_occurred_at, str)
                else None
            )
        except ValueError as exc:
            raise EnvelopeDecodeError(
                code="invalid_occurred_at",
                message="Kafka notification occurred_at must be ISO-8601",
            ) from exc
        if occurred_at is None or occurred_at.tzinfo is None:
            raise EnvelopeDecodeError(
                code="invalid_occurred_at",
                message="Kafka notification occurred_at must include a timezone",
            )

        data = envelope.get("data")
        if not isinstance(data, dict):
            raise EnvelopeDecodeError(
                code="invalid_data",
                message="Kafka notification data must be an object",
            )

        return NotificationKafkaMessage(
            key=self._decode_key(raw_message.key, event_id=event_id),
            event_id=event_id,
            event_type=event_type,
            occurred_at=occurred_at,
            payload=cast(Mapping[str, object], data),
            raw_message=raw_message,
        )

    def _decode_payload(self, value: bytes) -> Mapping[str, object]:
        try:
            decoded = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EnvelopeDecodeError(
                code="invalid_json",
                message="Kafka notification payload must be valid JSON",
            ) from exc

        if not isinstance(decoded, dict):
            raise EnvelopeDecodeError(
                code="invalid_envelope",
                message="Kafka notification envelope must be an object",
            )
        return cast(Mapping[str, object], decoded)

    def _decode_key(self, key: bytes | None, *, event_id: UUID) -> str:
        if key is None:
            return str(event_id)
        try:
            return key.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise EnvelopeDecodeError(
                code="invalid_key",
                message="Kafka notification key must be UTF-8",
            ) from exc


class AioKafkaConsumerClient:
    def __init__(
        self,
        *,
        topic: str,
        bootstrap_servers: str,
        group_id: str,
        client_id: str,
        consumer_factory: AsyncKafkaConsumerFactory | None = None,
    ) -> None:
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._client_id = client_id
        self._consumer_factory = consumer_factory or self._default_consumer_factory
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._consumer: AsyncKafkaConsumer | None = None
        self._closed = False

    def consume_one(self, *, timeout_seconds: float) -> KafkaRawMessage | None:
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._consume_one(timeout_seconds=timeout_seconds),
            loop,
        )
        return future.result()

    def commit(self) -> None:
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(self._commit(), loop)
        future.result()

    def close(self) -> None:
        if self._closed:
            return

        loop = self._loop
        if loop is not None:
            consumer = self._consumer
            if consumer is not None:
                stop_future = asyncio.run_coroutine_threadsafe(consumer.stop(), loop)
                stop_future.result()
            loop.call_soon_threadsafe(loop.stop)
            thread = self._thread
            if thread is not None:
                thread.join(timeout=5)
            loop.close()

        self._closed = True

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._closed:
            raise RuntimeError("Kafka consumer client is closed")
        if self._loop is not None:
            return self._loop

        loop = asyncio.new_event_loop()
        thread = Thread(target=self._run_loop, args=(loop,), daemon=True)
        thread.start()
        self._loop = loop
        self._thread = thread
        return loop

    def _run_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def _consume_one(self, *, timeout_seconds: float) -> KafkaRawMessage | None:
        consumer = await self._consumer_instance()
        try:
            record = await asyncio.wait_for(consumer.getone(), timeout=timeout_seconds)
        except TimeoutError:
            return None
        return KafkaRawMessage(key=record.key, value=record.value)

    async def _commit(self) -> None:
        consumer = await self._consumer_instance()
        await consumer.commit()

    async def _consumer_instance(self) -> AsyncKafkaConsumer:
        if self._consumer is None:
            consumer = self._consumer_factory(
                self._topic,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                client_id=self._client_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            await consumer.start()
            self._consumer = consumer
        return self._consumer

    def _default_consumer_factory(
        self,
        topic: str,
        *,
        bootstrap_servers: str,
        group_id: str,
        client_id: str,
        auto_offset_reset: str,
        enable_auto_commit: bool,
    ) -> AsyncKafkaConsumer:
        aiokafka_module = cast(Any, import_module("aiokafka"))
        consumer = aiokafka_module.AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=client_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
        )
        return cast(AsyncKafkaConsumer, consumer)
