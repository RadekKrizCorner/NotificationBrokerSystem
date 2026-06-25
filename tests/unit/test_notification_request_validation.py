import pytest
from pydantic import ValidationError

from backend.api.schemas.notification_requests import CreateNotificationRequest
from backend.domain.enums import Channel, Severity


class NotificationRequestValidationFixtures:
    @staticmethod
    def valid_payload() -> dict[str, object]:
        return {
            "message": "Billing sync failed",
            "severity": "warning",
            "audience": {
                "type": "labels",
                "labels": {
                    "region": "EU",
                    "tier": "enterprise",
                },
            },
            "channels": ["email", "web"],
            "idempotency_key": "billing-sync-failed-2026-06-23",
        }


class TestCreateNotificationRequestValidation:
    def test_preserves_supported_payload(self) -> None:
        request = CreateNotificationRequest.model_validate(
            NotificationRequestValidationFixtures.valid_payload()
        )

        assert request.message == "Billing sync failed"
        assert request.severity is Severity.WARNING
        assert request.channels == (Channel.EMAIL, Channel.WEB)
        assert request.audience.type == "labels"
        assert request.audience.labels == {"region": "EU", "tier": "enterprise"}
        assert request.idempotency_key == "billing-sync-failed-2026-06-23"

    @pytest.mark.kwparametrize(
        [
            {
                "id": "empty-message",
                "patch": {"message": ""},
                "expected_fragment": "message",
            },
            {
                "id": "message-leading-whitespace",
                "patch": {"message": " Billing sync failed"},
                "expected_fragment": "message",
            },
            {
                "id": "message-trailing-whitespace",
                "patch": {"message": "Billing sync failed "},
                "expected_fragment": "message",
            },
            {
                "id": "overlong-message",
                "patch": {"message": "x" * 2001},
                "expected_fragment": "message",
            },
            {
                "id": "invalid-severity",
                "patch": {"severity": "urgent"},
                "expected_fragment": "severity",
            },
            {
                "id": "uppercase-severity",
                "patch": {"severity": "WARNING"},
                "expected_fragment": "severity",
            },
            {
                "id": "duplicate-channels",
                "patch": {"channels": ["web", "web"]},
                "expected_fragment": "channels",
            },
            {
                "id": "empty-channels",
                "patch": {"channels": []},
                "expected_fragment": "channels",
            },
            {
                "id": "unsupported-channel",
                "patch": {"channels": ["sms"]},
                "expected_fragment": "channels",
            },
            {
                "id": "uppercase-channel",
                "patch": {"channels": ["WEB"]},
                "expected_fragment": "channels",
            },
            {
                "id": "unknown-top-level-field",
                "patch": {"actions": ["retry"]},
                "expected_fragment": "actions",
            },
            {
                "id": "malformed-audience",
                "patch": {"audience": {"labels": {"region": "EU"}}},
                "expected_fragment": "audience",
            },
            {
                "id": "all-audience-explicit-null-group",
                "patch": {"audience": {"type": "all", "group": None}},
                "expected_fragment": "audience",
            },
            {
                "id": "all-audience-explicit-null-labels",
                "patch": {"audience": {"type": "all", "labels": None}},
                "expected_fragment": "audience",
            },
            {
                "id": "group-audience-explicit-null-group",
                "patch": {"audience": {"type": "group", "group": None}},
                "expected_fragment": "audience",
            },
            {
                "id": "group-audience-explicit-null-labels",
                "patch": {
                    "audience": {
                        "type": "group",
                        "group": "Administrators",
                        "labels": None,
                    }
                },
                "expected_fragment": "audience",
            },
            {
                "id": "labels-audience-explicit-null-labels",
                "patch": {"audience": {"type": "labels", "labels": None}},
                "expected_fragment": "audience",
            },
            {
                "id": "labels-audience-explicit-null-group",
                "patch": {
                    "audience": {
                        "type": "labels",
                        "labels": {"region": "EU"},
                        "group": None,
                    }
                },
                "expected_fragment": "audience",
            },
            {
                "id": "empty-labels",
                "patch": {"audience": {"type": "labels", "labels": {}}},
                "expected_fragment": "labels",
            },
            {
                "id": "group-whitespace",
                "patch": {"audience": {"type": "group", "group": " Administrators "}},
                "expected_fragment": "group",
            },
            {
                "id": "label-key-whitespace",
                "patch": {"audience": {"type": "labels", "labels": {" region": "EU"}}},
                "expected_fragment": "label",
            },
            {
                "id": "label-value-whitespace",
                "patch": {"audience": {"type": "labels", "labels": {"region": " EU"}}},
                "expected_fragment": "label",
            },
            {
                "id": "blank-idempotency-key",
                "patch": {"idempotency_key": ""},
                "expected_fragment": "idempotency_key",
            },
            {
                "id": "idempotency-key-invalid-character",
                "patch": {"idempotency_key": "bad key"},
                "expected_fragment": "idempotency_key",
            },
            {
                "id": "idempotency-key-too-long",
                "patch": {"idempotency_key": "a" * 129},
                "expected_fragment": "idempotency_key",
            },
        ]
    )
    def test_validation_matrix(
        self,
        patch: dict[str, object],
        expected_fragment: str,
    ) -> None:
        payload = NotificationRequestValidationFixtures.valid_payload()
        payload.update(patch)

        with pytest.raises(ValidationError) as exc_info:
            CreateNotificationRequest.model_validate(payload)

        assert expected_fragment in str(exc_info.value)

    @pytest.mark.kwparametrize(
        [
            {
                "id": "all-users",
                "audience": {"type": "all"},
                "expected": {"type": "all", "group": None, "labels": None},
            },
            {
                "id": "group",
                "audience": {"type": "group", "group": "Administrators"},
                "expected": {"type": "group", "group": "Administrators", "labels": None},
            },
            {
                "id": "labels",
                "audience": {"type": "labels", "labels": {"tier": "enterprise", "region": "EU"}},
                "expected": {
                    "type": "labels",
                    "group": None,
                    "labels": {"region": "EU", "tier": "enterprise"},
                },
            },
        ]
    )
    def test_audience_selector_matrix(
        self,
        audience: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        payload = NotificationRequestValidationFixtures.valid_payload()
        payload["audience"] = audience

        request = CreateNotificationRequest.model_validate(payload)

        assert request.audience.model_dump() == expected
