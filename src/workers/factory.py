from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy.orm import Session, sessionmaker

from backend.core.config import Settings
from backend.core.metrics import PrometheusMetrics
from backend.db.session import make_engine, make_session_factory
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import Channel
from backend.domain.results import DeliveryWorkerResult, OutboxPublisherResult
from backend.services.notification_fanout_service import NotificationFanoutService
from workers.delivery.base import DeliveryAdapter
from workers.delivery.email import EmailDeliveryAdapter
from workers.delivery.web import WebDeliveryAdapter
from workers.delivery.worker import DeliveryWorker
from workers.kafka.consumer import AioKafkaConsumerClient, KafkaNotificationConsumer
from workers.kafka.publisher import AioKafkaProducerClient, KafkaEventPublisher
from workers.notifications.consumer import (
    NotificationConsumerWorker,
    NotificationEventConsumer,
)
from workers.notifications.requested import NotificationRequestedHandler
from workers.outbox.publisher import EventPublisher, OutboxPublisher
from workers.workload.generator import (
    NotificationRestClient,
    UrllibNotificationRestClient,
    WorkloadGenerator,
    WorkloadNotificationRequestFactory,
    WorkloadServiceTokenFactory,
)


class OutboxPublisherWorker:
    def __init__(self, *, publisher: OutboxPublisher, batch_size: int) -> None:
        self._publisher = publisher
        self._batch_size = batch_size

    def run_once(self) -> OutboxPublisherResult:
        return self._publisher.publish_due_events(limit=self._batch_size)


class OutboxWorkerFactory:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session] | None = None,
        event_publisher: EventPublisher | None = None,
        now: Callable[[], datetime] | None = None,
        metrics: PrometheusMetrics | None = None,
    ) -> None:
        self.settings = settings
        self._session_factory = session_factory
        self._event_publisher = event_publisher
        self._now = now
        self._metrics = metrics

    @classmethod
    def from_env(cls) -> OutboxWorkerFactory:
        return cls(settings=Settings())

    def create_outbox_publisher(self) -> OutboxPublisher:
        session_factory = self._resolved_session_factory()
        return OutboxPublisher(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            event_publisher=self._resolved_event_publisher(),
            now=self._now or self._default_now,
            worker_id=self.settings.outbox_worker_id,
            lease_duration=timedelta(seconds=self.settings.outbox_lease_seconds),
            retry_delay=timedelta(seconds=self.settings.outbox_retry_delay_seconds),
            max_attempts=self.settings.outbox_max_attempts,
            metrics=self._resolved_metrics(),
        )

    def create_outbox_worker(self) -> OutboxPublisherWorker:
        return OutboxPublisherWorker(
            publisher=self.create_outbox_publisher(),
            batch_size=self.settings.outbox_publish_batch_size,
        )

    def _resolved_session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is not None:
            return self._session_factory
        engine = make_engine(self.settings.database_url)
        return make_session_factory(engine)

    def _resolved_event_publisher(self) -> EventPublisher:
        if self._event_publisher is not None:
            return self._event_publisher
        producer = AioKafkaProducerClient(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            client_id=self.settings.kafka_client_id,
        )
        return KafkaEventPublisher(producer=producer)

    def _resolved_metrics(self) -> PrometheusMetrics:
        if self._metrics is not None:
            return self._metrics
        self._metrics = PrometheusMetrics()
        return self._metrics

    def _default_now(self) -> datetime:
        return datetime.now(UTC)


class DeliveryWorkerProcess:
    def __init__(self, *, worker: DeliveryWorker, batch_size: int) -> None:
        self._worker = worker
        self._batch_size = batch_size

    def run_once(self) -> DeliveryWorkerResult:
        return self._worker.process_due_deliveries(limit=self._batch_size)


class DeliveryWorkerFactory:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session] | None = None,
        adapters: Mapping[Channel, DeliveryAdapter] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self._session_factory = session_factory
        self._adapters = adapters
        self._now = now

    @classmethod
    def from_env(cls) -> DeliveryWorkerFactory:
        return cls(settings=Settings())

    def create_web_delivery_processor(self) -> DeliveryWorker:
        return self._create_delivery_processor(
            channels=(Channel.WEB,),
            worker_id=self.settings.web_delivery_worker_id,
        )

    def create_email_delivery_processor(self) -> DeliveryWorker:
        return self._create_delivery_processor(
            channels=(Channel.EMAIL,),
            worker_id=self.settings.email_delivery_worker_id,
        )

    def create_web_delivery_worker(self) -> DeliveryWorkerProcess:
        return DeliveryWorkerProcess(
            worker=self.create_web_delivery_processor(),
            batch_size=self.settings.delivery_batch_size,
        )

    def create_email_delivery_worker(self) -> DeliveryWorkerProcess:
        return DeliveryWorkerProcess(
            worker=self.create_email_delivery_processor(),
            batch_size=self.settings.delivery_batch_size,
        )

    def _create_delivery_processor(
        self,
        *,
        channels: tuple[Channel, ...],
        worker_id: str,
    ) -> DeliveryWorker:
        session_factory = self._resolved_session_factory()
        return DeliveryWorker(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            adapters=self._resolved_adapters(channels=channels),
            channels=channels,
            now=self._now or self._default_now,
            worker_id=worker_id,
            lease_duration=timedelta(seconds=self.settings.delivery_lease_seconds),
            retry_delay=timedelta(seconds=self.settings.delivery_retry_delay_seconds),
        )

    def _resolved_session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is not None:
            return self._session_factory
        engine = make_engine(self.settings.database_url)
        return make_session_factory(engine)

    def _resolved_adapters(
        self,
        *,
        channels: tuple[Channel, ...],
    ) -> Mapping[Channel, DeliveryAdapter]:
        if self._adapters is not None:
            return {
                channel: adapter
                for channel, adapter in self._adapters.items()
                if channel in channels
            }
        adapters: Mapping[Channel, DeliveryAdapter] = {
            Channel.WEB: WebDeliveryAdapter(),
            Channel.EMAIL: EmailDeliveryAdapter.for_mailpit(
                host=self.settings.smtp_host,
                port=self.settings.smtp_port,
                from_address=self.settings.smtp_from_address,
                timeout_seconds=self.settings.smtp_timeout_seconds,
            ),
        }
        return {channel: adapters[channel] for channel in channels}

    def _default_now(self) -> datetime:
        return datetime.now(UTC)


class NotificationConsumerWorkerFactory:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session] | None = None,
        event_consumer: NotificationEventConsumer | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self._session_factory = session_factory
        self._event_consumer = event_consumer
        self._now = now

    @classmethod
    def from_env(cls) -> NotificationConsumerWorkerFactory:
        return cls(settings=Settings())

    def create_notification_consumer_worker(self) -> NotificationConsumerWorker:
        session_factory = self._resolved_session_factory()
        now = self._now or self._default_now
        fanout_service = NotificationFanoutService(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            now=now,
            max_recipients=self.settings.fanout_max_recipients,
            max_deliveries=self.settings.fanout_max_deliveries,
        )
        return NotificationConsumerWorker(
            event_consumer=self._resolved_event_consumer(),
            handler=NotificationRequestedHandler(fanout_service=fanout_service),
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            consumer_name=self.settings.notification_consumer_name,
            now=now,
            batch_size=self.settings.notification_consumer_batch_size,
            poll_timeout_seconds=self.settings.notification_consumer_poll_timeout_seconds,
        )

    def _resolved_session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is not None:
            return self._session_factory
        engine = make_engine(self.settings.database_url)
        return make_session_factory(engine)

    def _resolved_event_consumer(self) -> NotificationEventConsumer:
        if self._event_consumer is not None:
            return self._event_consumer
        raw_consumer = AioKafkaConsumerClient(
            topic=self.settings.notification_consumer_topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id=self.settings.notification_consumer_group_id,
            client_id=self.settings.notification_consumer_client_id,
        )
        return KafkaNotificationConsumer(raw_consumer=raw_consumer)

    def _default_now(self) -> datetime:
        return datetime.now(UTC)


class WorkloadGeneratorFactory:
    def __init__(
        self,
        *,
        settings: Settings,
        rest_client: NotificationRestClient | None = None,
        run_id: str | None = None,
    ) -> None:
        self.settings = settings
        self._rest_client = rest_client
        self._run_id = run_id

    @classmethod
    def from_env(cls) -> WorkloadGeneratorFactory:
        return cls(settings=Settings())

    def create_workload_generator(self) -> WorkloadGenerator:
        run_id = self._run_id or self.settings.workload_run_id or uuid4().hex
        return WorkloadGenerator(
            api_base_url=self.settings.workload_api_base_url,
            request_timeout_seconds=self.settings.workload_request_timeout_seconds,
            rest_client=self._resolved_rest_client(),
            request_factory=WorkloadNotificationRequestFactory(run_id=run_id),
            token_factory=WorkloadServiceTokenFactory(
                source_service=self.settings.workload_source_service,
                jwt_secret=self.settings.jwt_secret,
                jwt_algorithm=self.settings.jwt_algorithm,
                jwt_issuer=self.settings.jwt_issuer,
                jwt_audience=self.settings.jwt_audience,
                token_ttl_seconds=self.settings.jwt_token_ttl_seconds,
            ),
        )

    def _resolved_rest_client(self) -> NotificationRestClient:
        if self._rest_client is not None:
            return self._rest_client
        return UrllibNotificationRestClient()
