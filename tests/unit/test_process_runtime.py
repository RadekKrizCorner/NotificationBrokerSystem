from typing import Any

import pytest

from backend.core.config import Settings
from backend.runtime import NotificationCenterCli, PollingWorkerRuntime


class RecordingWorker:
    def __init__(self) -> None:
        self.run_count = 0

    def run_once(self) -> str:
        self.run_count += 1
        return "processed"


class RecordingSleeper:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.calls.append(seconds)


class RecordingApiRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def run(self, app_path: str, *, host: str, port: int) -> None:
        self.calls.append({"app_path": app_path, "host": host, "port": port})


class RecordingOutboxWorkerFactory:
    def __init__(self, settings: Settings, worker: RecordingWorker) -> None:
        self.settings = settings
        self.worker = worker

    def create_outbox_worker(self) -> RecordingWorker:
        return self.worker


class RecordingDeliveryWorkerFactory:
    def __init__(
        self,
        settings: Settings,
        *,
        web_worker: RecordingWorker,
        email_worker: RecordingWorker,
    ) -> None:
        self.settings = settings
        self.web_worker = web_worker
        self.email_worker = email_worker

    def create_web_delivery_worker(self) -> RecordingWorker:
        return self.web_worker

    def create_email_delivery_worker(self) -> RecordingWorker:
        return self.email_worker


class RecordingNotificationConsumerWorkerFactory:
    def __init__(self, settings: Settings, worker: RecordingWorker) -> None:
        self.settings = settings
        self.worker = worker

    def create_notification_consumer_worker(self) -> RecordingWorker:
        return self.worker


class RecordingDemoDataSeederFactory:
    def __init__(self, settings: Settings, worker: RecordingWorker) -> None:
        self.settings = settings
        self.worker = worker

    def create_demo_data_seeder(self) -> RecordingWorker:
        return self.worker


class RecordingWorkloadGeneratorFactory:
    def __init__(self, settings: Settings, worker: RecordingWorker) -> None:
        self.settings = settings
        self.worker = worker

    def create_workload_generator(self) -> RecordingWorker:
        return self.worker


class RecordingRuntime:
    def __init__(
        self,
        *,
        run_once: Any,
        poll_interval_seconds: float,
    ) -> None:
        self.run_once = run_once
        self.poll_interval_seconds = poll_interval_seconds
        self.run_forever_count = 0

    def run_forever(self) -> None:
        self.run_forever_count += 1


class RecordingRuntimeFactory:
    def __init__(self) -> None:
        self.runtimes: list[RecordingRuntime] = []

    def create(self, *, run_once: Any, poll_interval_seconds: float) -> RecordingRuntime:
        runtime = RecordingRuntime(
            run_once=run_once,
            poll_interval_seconds=poll_interval_seconds,
        )
        self.runtimes.append(runtime)
        return runtime


class ProcessRuntimeFixtures:
    @staticmethod
    def settings() -> Settings:
        return Settings(
            jwt_secret="test-secret-long-enough",
            api_host="127.0.0.1",
            api_port=9001,
            outbox_poll_interval_seconds=2.5,
            delivery_poll_interval_seconds=3.5,
            notification_consumer_poll_interval_seconds=4.5,
            workload_notifications_per_window=100,
            workload_window_seconds=300.0,
        )


class TestPollingWorkerRuntime:
    def test_run_once_delegates_to_worker_without_sleeping(self) -> None:
        worker = RecordingWorker()
        sleeper = RecordingSleeper()
        runtime = PollingWorkerRuntime(
            run_once=worker.run_once,
            poll_interval_seconds=1.5,
            sleep=sleeper.sleep,
        )

        result = runtime.run_once()

        assert result == "processed"
        assert worker.run_count == 1
        assert sleeper.calls == []

    def test_run_poll_cycle_sleeps_after_work(self) -> None:
        worker = RecordingWorker()
        sleeper = RecordingSleeper()
        runtime = PollingWorkerRuntime(
            run_once=worker.run_once,
            poll_interval_seconds=1.5,
            sleep=sleeper.sleep,
        )

        result = runtime.run_poll_cycle()

        assert result == "processed"
        assert worker.run_count == 1
        assert sleeper.calls == [1.5]


class TestNotificationCenterCli:
    def test_api_role_runs_uvicorn_app_from_settings(self) -> None:
        settings = ProcessRuntimeFixtures.settings()
        api_runner = RecordingApiRunner()
        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            api_runner=api_runner.run,
        )

        exit_code = cli.run(["api"])

        assert exit_code == 0
        assert api_runner.calls == [
            {
                "app_path": "backend.main:app",
                "host": "127.0.0.1",
                "port": 9001,
            }
        ]

    def test_outbox_publisher_once_runs_one_batch(self) -> None:
        settings = ProcessRuntimeFixtures.settings()
        outbox_worker = RecordingWorker()

        def factory_builder(factory_settings: Settings) -> RecordingOutboxWorkerFactory:
            return RecordingOutboxWorkerFactory(factory_settings, outbox_worker)

        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            outbox_worker_factory=factory_builder,
        )

        exit_code = cli.run(["outbox-publisher", "--once"])

        assert exit_code == 0
        assert outbox_worker.run_count == 1

    @pytest.mark.kwparametrize(
        [
            {
                "id": "web",
                "role": "web-delivery-worker",
                "expected_worker": "web",
            },
            {
                "id": "email",
                "role": "email-delivery-worker",
                "expected_worker": "email",
            },
        ]
    )
    def test_channel_delivery_worker_once_runs_one_batch(
        self,
        role: str,
        expected_worker: str,
    ) -> None:
        settings = ProcessRuntimeFixtures.settings()
        web_worker = RecordingWorker()
        email_worker = RecordingWorker()

        def factory_builder(factory_settings: Settings) -> RecordingDeliveryWorkerFactory:
            return RecordingDeliveryWorkerFactory(
                factory_settings,
                web_worker=web_worker,
                email_worker=email_worker,
            )

        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            delivery_worker_factory=factory_builder,
        )

        exit_code = cli.run([role, "--once"])

        assert exit_code == 0
        assert web_worker.run_count == (1 if expected_worker == "web" else 0)
        assert email_worker.run_count == (1 if expected_worker == "email" else 0)

    def test_generic_delivery_worker_role_is_not_supported(self) -> None:
        cli = NotificationCenterCli(settings_factory=ProcessRuntimeFixtures.settings)

        with pytest.raises(SystemExit):
            cli.run(["delivery-worker", "--once"])

    def test_notification_consumer_once_runs_one_batch(self) -> None:
        settings = ProcessRuntimeFixtures.settings()
        notification_consumer_worker = RecordingWorker()

        def factory_builder(
            factory_settings: Settings,
        ) -> RecordingNotificationConsumerWorkerFactory:
            return RecordingNotificationConsumerWorkerFactory(
                factory_settings,
                notification_consumer_worker,
            )

        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            notification_consumer_worker_factory=factory_builder,
        )

        exit_code = cli.run(["notification-consumer", "--once"])

        assert exit_code == 0
        assert notification_consumer_worker.run_count == 1

    def test_seed_demo_data_role_runs_seeder_once(self) -> None:
        settings = ProcessRuntimeFixtures.settings()
        seeder = RecordingWorker()

        def factory_builder(factory_settings: Settings) -> RecordingDemoDataSeederFactory:
            return RecordingDemoDataSeederFactory(factory_settings, seeder)

        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            demo_data_seeder_factory=factory_builder,
        )

        exit_code = cli.run(["seed-demo-data"])

        assert exit_code == 0
        assert seeder.run_count == 1

    def test_workload_generator_once_runs_one_request(self) -> None:
        settings = ProcessRuntimeFixtures.settings()
        workload_generator = RecordingWorker()

        def factory_builder(factory_settings: Settings) -> RecordingWorkloadGeneratorFactory:
            return RecordingWorkloadGeneratorFactory(factory_settings, workload_generator)

        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            workload_generator_factory=factory_builder,
        )

        exit_code = cli.run(["workload-generator", "--once"])

        assert exit_code == 0
        assert workload_generator.run_count == 1

    def test_workload_generator_polling_uses_configured_rate_interval(self) -> None:
        settings = ProcessRuntimeFixtures.settings()
        workload_generator = RecordingWorker()
        runtime_factory = RecordingRuntimeFactory()

        def factory_builder(factory_settings: Settings) -> RecordingWorkloadGeneratorFactory:
            return RecordingWorkloadGeneratorFactory(factory_settings, workload_generator)

        cli = NotificationCenterCli(
            settings_factory=lambda: settings,
            workload_generator_factory=factory_builder,
            runtime_factory=runtime_factory.create,
        )

        exit_code = cli.run(["workload-generator"])

        assert exit_code == 0
        assert len(runtime_factory.runtimes) == 1
        assert runtime_factory.runtimes[0].poll_interval_seconds == 3.0
        assert runtime_factory.runtimes[0].run_forever_count == 1
