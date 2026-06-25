import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from threading import Thread
from typing import Any, Protocol, cast


@dataclass(frozen=True, slots=True)
class KafkaRawMessage:
    key: bytes | None
    value: bytes


@dataclass(frozen=True, slots=True)
class NotificationKafkaMessage:
    key: str
    payload: Mapping[str, object]


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
    def __init__(self, *, raw_consumer: RawKafkaConsumerClient) -> None:
        self._raw_consumer = raw_consumer

    def consume_one(self, *, timeout_seconds: float) -> NotificationKafkaMessage | None:
        raw_message = self._raw_consumer.consume_one(timeout_seconds=timeout_seconds)
        if raw_message is None:
            return None

        payload = self._decode_payload(raw_message.value)
        return NotificationKafkaMessage(
            key=self._decode_key(raw_message.key, payload=payload),
            payload=payload,
        )

    def commit(self) -> None:
        self._raw_consumer.commit()

    def _decode_payload(self, value: bytes) -> Mapping[str, object]:
        try:
            decoded = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Kafka notification payload must be valid JSON") from exc

        if not isinstance(decoded, dict):
            raise ValueError("Kafka notification payload must be a JSON object")
        return cast(Mapping[str, object], decoded)

    def _decode_key(self, key: bytes | None, *, payload: Mapping[str, object]) -> str:
        if key is not None:
            return key.decode("utf-8")

        notification_id = payload.get("notification_id")
        if isinstance(notification_id, str):
            return notification_id
        raise ValueError("Kafka notification message key is required")


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
