from __future__ import annotations

import json
import logging
from typing import Protocol
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import jwt

from backend.domain.results import WorkloadGeneratorResult

logger = logging.getLogger(__name__)


class NotificationRestClient(Protocol):
    def post_notification(
        self,
        *,
        url: str,
        bearer_token: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> int:
        pass


class UrllibNotificationRestClient:
    def post_notification(
        self,
        *,
        url: str,
        bearer_token: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> int:
        request = Request(
            url,
            data=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return int(response.status)
        except HTTPError as exc:
            return int(exc.code)


class WorkloadNotificationRequestFactory:
    severities = ("info", "warning", "error", "critical")
    channel_variants = (("web",), ("email",), ("web", "email"))

    def __init__(self, *, run_id: str) -> None:
        self._run_id = run_id

    def create(self, sequence: int) -> dict[str, object]:
        if sequence < 1:
            raise ValueError("sequence must be positive")

        index = sequence - 1
        audience, audience_description = self._audience(index)
        return {
            "message": (
                f"Demo workload notification {sequence:06d} for {audience_description}"
            ),
            "severity": self.severities[index % len(self.severities)],
            "audience": audience,
            "channels": list(self.channel_variants[index % len(self.channel_variants)]),
            "idempotency_key": f"workload:{self._run_id}:{sequence}",
        }

    def _audience(self, index: int) -> tuple[dict[str, object], str]:
        return {"type": "group", "group": "Administrators"}, "group=Administrators"


class WorkloadServiceTokenFactory:
    def __init__(
        self,
        *,
        source_service: str,
        jwt_secret: str,
        jwt_algorithm: str,
    ) -> None:
        self._source_service = source_service
        self._jwt_secret = jwt_secret
        self._jwt_algorithm = jwt_algorithm

    def create_bearer_token(self) -> str:
        token = jwt.encode(
            {
                "sub": self._source_service,
                "type": "service",
                "scopes": ["notifications:write"],
            },
            self._jwt_secret,
            algorithm=self._jwt_algorithm,
        )
        if isinstance(token, bytes):
            return token.decode("utf-8")
        return token


class WorkloadGenerator:
    def __init__(
        self,
        *,
        api_base_url: str,
        request_timeout_seconds: float,
        rest_client: NotificationRestClient,
        request_factory: WorkloadNotificationRequestFactory,
        token_factory: WorkloadServiceTokenFactory,
        initial_sequence: int = 1,
    ) -> None:
        self._notifications_url = f"{api_base_url.rstrip('/')}/notifications"
        self._request_timeout_seconds = request_timeout_seconds
        self._rest_client = rest_client
        self._request_factory = request_factory
        self._token_factory = token_factory
        self._next_sequence = initial_sequence

    def run_once(self) -> WorkloadGeneratorResult:
        sequence = self._next_sequence
        payload = self._request_factory.create(sequence)
        bearer_token = self._token_factory.create_bearer_token()

        status_code: int | None = None
        error_message: str | None = None
        try:
            status_code = self._rest_client.post_notification(
                url=self._notifications_url,
                bearer_token=bearer_token,
                payload=payload,
                timeout_seconds=self._request_timeout_seconds,
            )
            if status_code != 202:
                error_message = f"unexpected status code {status_code}"
        except Exception as exc:
            error_message = str(exc)

        self._next_sequence += 1
        success = error_message is None
        if not success:
            logger.warning(
                "workload notification request failed",
                extra={
                    "sequence": sequence,
                    "status_code": status_code,
                    "error_message": error_message,
                },
            )

        return WorkloadGeneratorResult(
            sequence=sequence,
            status_code=status_code,
            success=success,
            error_message=error_message,
        )
