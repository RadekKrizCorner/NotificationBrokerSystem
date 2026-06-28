from typing import cast

import jwt
import pytest

from workers.workload.generator import (
    WorkloadGenerator,
    WorkloadNotificationRequestFactory,
    WorkloadServiceTokenFactory,
)


class RecordingNotificationRestClient:
    def __init__(self, *, status_code: int = 202, exception: Exception | None = None) -> None:
        self.status_code = status_code
        self.exception = exception
        self.calls: list[dict[str, object]] = []

    def post_notification(
        self,
        *,
        url: str,
        bearer_token: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> int:
        self.calls.append(
            {
                "url": url,
                "bearer_token": bearer_token,
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self.exception is not None:
            raise self.exception
        return self.status_code


class WorkloadGeneratorFixtures:
    @staticmethod
    def request_factory() -> WorkloadNotificationRequestFactory:
        return WorkloadNotificationRequestFactory(run_id="run-123")

    @staticmethod
    def token_factory() -> WorkloadServiceTokenFactory:
        return WorkloadServiceTokenFactory(
            source_service="demo-workload-generator",
            jwt_secret="test-secret-long-enough-for-hs256",
            jwt_algorithm="HS256",
            jwt_issuer="notification-center",
            jwt_audience="notification-center-api",
            token_ttl_seconds=300,
        )

    @staticmethod
    def generator(
        client: RecordingNotificationRestClient,
        *,
        api_base_url: str = "http://api:8000/",
    ) -> WorkloadGenerator:
        return WorkloadGenerator(
            api_base_url=api_base_url,
            request_timeout_seconds=5.5,
            rest_client=client,
            request_factory=WorkloadGeneratorFixtures.request_factory(),
            token_factory=WorkloadGeneratorFixtures.token_factory(),
        )


class TestWorkloadNotificationRequestFactory:
    def test_create_uses_low_fanout_demo_audience(self) -> None:
        factory = WorkloadGeneratorFixtures.request_factory()

        payloads = [factory.create(sequence) for sequence in range(1, 6)]

        audiences = [cast(dict[str, object], payload["audience"]) for payload in payloads]

        assert {audience["group"] for audience in audiences} == {"Administrators"}

    @pytest.mark.kwparametrize(
        [
            {
                "id": "first-admin-web",
                "sequence": 1,
                "severity": "info",
                "audience": {"type": "group", "group": "Administrators"},
                "channels": ["web"],
                "message": "Demo workload notification 000001 for group=Administrators",
            },
            {
                "id": "second-admin-email",
                "sequence": 2,
                "severity": "warning",
                "audience": {"type": "group", "group": "Administrators"},
                "channels": ["email"],
                "message": "Demo workload notification 000002 for group=Administrators",
            },
            {
                "id": "third-admin-both",
                "sequence": 3,
                "severity": "error",
                "audience": {"type": "group", "group": "Administrators"},
                "channels": ["web", "email"],
                "message": "Demo workload notification 000003 for group=Administrators",
            },
            {
                "id": "fourth-admin-web",
                "sequence": 4,
                "severity": "critical",
                "audience": {"type": "group", "group": "Administrators"},
                "channels": ["web"],
                "message": "Demo workload notification 000004 for group=Administrators",
            },
            {
                "id": "fifth-admin-email",
                "sequence": 5,
                "severity": "info",
                "audience": {"type": "group", "group": "Administrators"},
                "channels": ["email"],
                "message": "Demo workload notification 000005 for group=Administrators",
            },
        ]
    )
    def test_create_cycles_deterministic_notification_variants(
        self,
        sequence: int,
        severity: str,
        audience: dict[str, object],
        channels: list[str],
        message: str,
    ) -> None:
        factory = WorkloadGeneratorFixtures.request_factory()

        payload = factory.create(sequence)

        assert payload == {
            "message": message,
            "severity": severity,
            "audience": audience,
            "channels": channels,
            "idempotency_key": f"workload:run-123:{sequence}",
        }


class TestWorkloadServiceTokenFactory:
    def test_create_bearer_token_uses_service_principal_and_write_scope(self) -> None:
        token_factory = WorkloadGeneratorFixtures.token_factory()

        token = token_factory.create_bearer_token()

        payload = jwt.decode(
            token,
            "test-secret-long-enough-for-hs256",
            algorithms=["HS256"],
            audience="notification-center-api",
            issuer="notification-center",
        )
        assert payload["sub"] == "demo-workload-generator"
        assert payload["type"] == "service"
        assert payload["scopes"] == ["notifications:write"]
        assert payload["exp"] - payload["iat"] == 300


class TestWorkloadGenerator:
    def test_run_once_sends_one_authenticated_post_to_notifications_endpoint(self) -> None:
        client = RecordingNotificationRestClient()
        generator = WorkloadGeneratorFixtures.generator(client)

        result = generator.run_once()

        assert result.sequence == 1
        assert result.status_code == 202
        assert result.success is True
        assert len(client.calls) == 1
        assert client.calls[0]["url"] == "http://api:8000/notifications"
        assert client.calls[0]["timeout_seconds"] == 5.5
        assert client.calls[0]["payload"] == {
            "message": "Demo workload notification 000001 for group=Administrators",
            "severity": "info",
            "audience": {"type": "group", "group": "Administrators"},
            "channels": ["web"],
            "idempotency_key": "workload:run-123:1",
        }
        token_payload = jwt.decode(
            str(client.calls[0]["bearer_token"]),
            "test-secret-long-enough-for-hs256",
            algorithms=["HS256"],
            audience="notification-center-api",
            issuer="notification-center",
        )
        assert token_payload["sub"] == "demo-workload-generator"

    def test_run_once_advances_sequence_for_next_notification(self) -> None:
        client = RecordingNotificationRestClient()
        generator = WorkloadGeneratorFixtures.generator(client)

        first = generator.run_once()
        second = generator.run_once()

        assert first.sequence == 1
        assert second.sequence == 2
        assert client.calls[0]["payload"] != client.calls[1]["payload"]
        assert client.calls[1]["payload"] == {
            "message": "Demo workload notification 000002 for group=Administrators",
            "severity": "warning",
            "audience": {"type": "group", "group": "Administrators"},
            "channels": ["email"],
            "idempotency_key": "workload:run-123:2",
        }

    def test_run_once_returns_failed_result_when_rest_call_fails(self) -> None:
        client = RecordingNotificationRestClient(exception=RuntimeError("api unavailable"))
        generator = WorkloadGeneratorFixtures.generator(client)

        result = generator.run_once()

        assert result.sequence == 1
        assert result.status_code is None
        assert result.success is False
        assert result.error_message == "api unavailable"

    def test_run_once_returns_failed_result_when_api_rejects_request(self) -> None:
        client = RecordingNotificationRestClient(status_code=500)
        generator = WorkloadGeneratorFixtures.generator(client)

        result = generator.run_once()

        assert result.sequence == 1
        assert result.status_code == 500
        assert result.success is False
        assert result.error_message == "unexpected status code 500"
