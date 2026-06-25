import asyncio
import json
from collections.abc import Mapping
from importlib import import_module
from threading import Thread
from typing import Any, Protocol, cast


class KafkaProducerClient(Protocol):
    def send_and_wait(self, *, topic: str, key: bytes, value: bytes) -> None:
        pass


class AsyncKafkaProducer(Protocol):
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_and_wait(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
    ) -> object:
        pass


class AsyncKafkaProducerFactory(Protocol):
    def __call__(self, *, bootstrap_servers: str, client_id: str) -> AsyncKafkaProducer:
        pass


class KafkaEventPublisher:
    def __init__(self, *, producer: KafkaProducerClient) -> None:
        self._producer = producer

    def publish(self, *, topic: str, key: str, payload: Mapping[str, object]) -> None:
        encoded_key = key.encode("utf-8")
        encoded_payload = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self._producer.send_and_wait(topic=topic, key=encoded_key, value=encoded_payload)


class AioKafkaProducerClient:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
        producer_factory: AsyncKafkaProducerFactory | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._producer_factory = producer_factory or self._default_producer_factory
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._producer: AsyncKafkaProducer | None = None
        self._closed = False

    def send_and_wait(self, *, topic: str, key: bytes, value: bytes) -> None:
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._send_and_wait(topic=topic, key=key, value=value),
            loop,
        )
        future.result()

    def close(self) -> None:
        if self._closed:
            return

        loop = self._loop
        if loop is not None:
            producer = self._producer
            if producer is not None:
                stop_future = asyncio.run_coroutine_threadsafe(producer.stop(), loop)
                stop_future.result()
            loop.call_soon_threadsafe(loop.stop)
            thread = self._thread
            if thread is not None:
                thread.join(timeout=5)
            loop.close()

        self._closed = True

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._closed:
            raise RuntimeError("Kafka producer client is closed")
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

    async def _send_and_wait(self, *, topic: str, key: bytes, value: bytes) -> None:
        producer = await self._producer_instance()
        await producer.send_and_wait(topic, key=key, value=value)

    async def _producer_instance(self) -> AsyncKafkaProducer:
        if self._producer is None:
            producer = self._producer_factory(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
            )
            await producer.start()
            self._producer = producer
        return self._producer

    def _default_producer_factory(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
    ) -> AsyncKafkaProducer:
        aiokafka_module = cast(Any, import_module("aiokafka"))
        producer = aiokafka_module.AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
        )
        return cast(AsyncKafkaProducer, producer)
