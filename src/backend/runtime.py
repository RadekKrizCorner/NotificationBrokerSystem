from __future__ import annotations

import logging
from argparse import ArgumentParser
from collections.abc import Callable, Sequence
from time import sleep as default_sleep
from typing import Protocol

import uvicorn

from backend.core.config import Settings
from backend.seeding.demo_data import DemoDataSeederFactory
from workers.factory import (
    DeliveryWorkerFactory,
    NotificationConsumerWorkerFactory,
    OutboxWorkerFactory,
    WorkloadGeneratorFactory,
)

logger = logging.getLogger(__name__)


class OneShotWorker(Protocol):
    def run_once(self) -> object:
        pass


class ApiRunner(Protocol):
    def __call__(self, app_path: str, *, host: str, port: int) -> None:
        pass


class WorkerRuntime(Protocol):
    def run_forever(self) -> None:
        pass


SettingsFactory = Callable[[], Settings]
WorkerFactoryBuilder = Callable[[Settings], object]
RuntimeFactory = Callable[..., WorkerRuntime]


class PollingWorkerRuntime:
    def __init__(
        self,
        *,
        run_once: Callable[[], object],
        poll_interval_seconds: float,
        error_backoff_initial_seconds: float = 1.0,
        error_backoff_max_seconds: float = 30.0,
        sleep: Callable[[float], None] = default_sleep,
    ) -> None:
        if error_backoff_initial_seconds <= 0:
            raise ValueError("error backoff initial value must be positive")
        if error_backoff_max_seconds < error_backoff_initial_seconds:
            raise ValueError("error backoff maximum must be at least the initial value")
        self._run_once = run_once
        self._poll_interval_seconds = poll_interval_seconds
        self._error_backoff_initial_seconds = error_backoff_initial_seconds
        self._error_backoff_max_seconds = error_backoff_max_seconds
        self._current_error_backoff_seconds = error_backoff_initial_seconds
        self._sleep = sleep

    def run_once(self) -> object:
        return self._run_once()

    def run_poll_cycle(self) -> object | None:
        try:
            result = self.run_once()
        except Exception:
            logger.exception("worker poll cycle failed")
            delay = self._current_error_backoff_seconds
            self._current_error_backoff_seconds = min(
                delay * 2,
                self._error_backoff_max_seconds,
            )
            self._sleep(delay)
            return None

        self._current_error_backoff_seconds = self._error_backoff_initial_seconds
        self._sleep(self._poll_interval_seconds)
        return result

    def run_forever(self) -> None:
        while True:
            self.run_poll_cycle()


class NotificationCenterCli:
    def __init__(
        self,
        *,
        settings_factory: SettingsFactory = Settings,
        api_runner: ApiRunner | None = None,
        outbox_worker_factory: WorkerFactoryBuilder | None = None,
        delivery_worker_factory: WorkerFactoryBuilder | None = None,
        notification_consumer_worker_factory: WorkerFactoryBuilder | None = None,
        demo_data_seeder_factory: WorkerFactoryBuilder | None = None,
        workload_generator_factory: WorkerFactoryBuilder | None = None,
        runtime_factory: RuntimeFactory = PollingWorkerRuntime,
    ) -> None:
        self._settings_factory = settings_factory
        self._api_runner = api_runner or self._run_uvicorn
        self._outbox_worker_factory = outbox_worker_factory or self._create_outbox_factory
        self._delivery_worker_factory = delivery_worker_factory or self._create_delivery_factory
        self._notification_consumer_worker_factory = (
            notification_consumer_worker_factory or self._create_notification_consumer_factory
        )
        self._demo_data_seeder_factory = (
            demo_data_seeder_factory or self._create_demo_data_seeder_factory
        )
        self._workload_generator_factory = (
            workload_generator_factory or self._create_workload_generator_factory
        )
        self._runtime_factory = runtime_factory

    def run(self, argv: Sequence[str] | None = None) -> int:
        args = self._parser().parse_args(argv)
        settings = self._settings_factory()

        if args.role == "api":
            self._api_runner("backend.main:app", host=settings.api_host, port=settings.api_port)
            return 0

        if args.role == "outbox-publisher":
            factory = self._outbox_worker_factory(settings)
            worker = self._outbox_worker(factory)
            self._run_worker(
                worker=worker,
                once=args.once,
                poll_interval_seconds=settings.outbox_poll_interval_seconds,
            )
            return 0

        if args.role == "web-delivery-worker":
            factory = self._delivery_worker_factory(settings)
            worker = self._web_delivery_worker(factory)
            self._run_worker(
                worker=worker,
                once=args.once,
                poll_interval_seconds=settings.delivery_poll_interval_seconds,
            )
            return 0

        if args.role == "email-delivery-worker":
            factory = self._delivery_worker_factory(settings)
            worker = self._email_delivery_worker(factory)
            self._run_worker(
                worker=worker,
                once=args.once,
                poll_interval_seconds=settings.delivery_poll_interval_seconds,
            )
            return 0

        if args.role == "notification-consumer":
            factory = self._notification_consumer_worker_factory(settings)
            worker = self._notification_consumer_worker(factory)
            self._run_worker(
                worker=worker,
                once=args.once,
                poll_interval_seconds=settings.notification_consumer_poll_interval_seconds,
            )
            return 0

        if args.role == "seed-demo-data":
            factory = self._demo_data_seeder_factory(settings)
            seeder = self._demo_data_seeder(factory)
            seeder.run_once()
            return 0

        if args.role == "workload-generator":
            factory = self._workload_generator_factory(settings)
            worker = self._workload_generator(factory)
            self._run_worker(
                worker=worker,
                once=args.once,
                poll_interval_seconds=settings.workload_interval_seconds,
            )
            return 0

        raise ValueError(f"unsupported role {args.role}")

    def _parser(self) -> ArgumentParser:
        parser = ArgumentParser(prog="notification-center")
        subparsers = parser.add_subparsers(dest="role", required=True)
        subparsers.add_parser("api")

        outbox_parser = subparsers.add_parser("outbox-publisher")
        outbox_parser.add_argument("--once", action="store_true")

        web_delivery_parser = subparsers.add_parser("web-delivery-worker")
        web_delivery_parser.add_argument("--once", action="store_true")

        email_delivery_parser = subparsers.add_parser("email-delivery-worker")
        email_delivery_parser.add_argument("--once", action="store_true")

        notification_consumer_parser = subparsers.add_parser("notification-consumer")
        notification_consumer_parser.add_argument("--once", action="store_true")

        subparsers.add_parser("seed-demo-data")

        workload_generator_parser = subparsers.add_parser("workload-generator")
        workload_generator_parser.add_argument("--once", action="store_true")
        return parser

    def _run_worker(
        self,
        *,
        worker: OneShotWorker,
        once: bool,
        poll_interval_seconds: float,
    ) -> None:
        if once:
            worker.run_once()
            return

        runtime = self._runtime_factory(
            run_once=worker.run_once,
            poll_interval_seconds=poll_interval_seconds,
        )
        runtime.run_forever()

    def _outbox_worker(self, factory: object) -> OneShotWorker:
        if not hasattr(factory, "create_outbox_worker"):
            raise TypeError("outbox worker factory must expose create_outbox_worker")
        worker = factory.create_outbox_worker()
        return self._ensure_one_shot_worker(worker)

    def _web_delivery_worker(self, factory: object) -> OneShotWorker:
        if not hasattr(factory, "create_web_delivery_worker"):
            raise TypeError("delivery worker factory must expose create_web_delivery_worker")
        worker = factory.create_web_delivery_worker()
        return self._ensure_one_shot_worker(worker)

    def _email_delivery_worker(self, factory: object) -> OneShotWorker:
        if not hasattr(factory, "create_email_delivery_worker"):
            raise TypeError("delivery worker factory must expose create_email_delivery_worker")
        worker = factory.create_email_delivery_worker()
        return self._ensure_one_shot_worker(worker)

    def _notification_consumer_worker(self, factory: object) -> OneShotWorker:
        if not hasattr(factory, "create_notification_consumer_worker"):
            raise TypeError(
                "notification consumer worker factory must expose "
                "create_notification_consumer_worker"
            )
        worker = factory.create_notification_consumer_worker()
        return self._ensure_one_shot_worker(worker)

    def _demo_data_seeder(self, factory: object) -> OneShotWorker:
        if not hasattr(factory, "create_demo_data_seeder"):
            raise TypeError("demo data seeder factory must expose create_demo_data_seeder")
        seeder = factory.create_demo_data_seeder()
        return self._ensure_one_shot_worker(seeder)

    def _workload_generator(self, factory: object) -> OneShotWorker:
        if not hasattr(factory, "create_workload_generator"):
            raise TypeError("workload generator factory must expose create_workload_generator")
        worker = factory.create_workload_generator()
        return self._ensure_one_shot_worker(worker)

    def _ensure_one_shot_worker(self, worker: object) -> OneShotWorker:
        if not hasattr(worker, "run_once"):
            raise TypeError("worker must expose run_once")
        return worker

    def _run_uvicorn(self, app_path: str, *, host: str, port: int) -> None:
        uvicorn.run(app_path, host=host, port=port)

    def _create_outbox_factory(self, settings: Settings) -> OutboxWorkerFactory:
        return OutboxWorkerFactory(settings=settings)

    def _create_delivery_factory(self, settings: Settings) -> DeliveryWorkerFactory:
        return DeliveryWorkerFactory(settings=settings)

    def _create_notification_consumer_factory(
        self,
        settings: Settings,
    ) -> NotificationConsumerWorkerFactory:
        return NotificationConsumerWorkerFactory(settings=settings)

    def _create_demo_data_seeder_factory(self, settings: Settings) -> DemoDataSeederFactory:
        return DemoDataSeederFactory(settings=settings)

    def _create_workload_generator_factory(self, settings: Settings) -> WorkloadGeneratorFactory:
        return WorkloadGeneratorFactory(settings=settings)


def main(argv: Sequence[str] | None = None) -> int:
    return NotificationCenterCli().run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
