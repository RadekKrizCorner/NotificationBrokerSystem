from datetime import UTC, datetime, timedelta

import pytest

from backend.domain.enums import AudienceType, Channel, Severity
from backend.domain.value_objects import AudienceSelection, NotificationCreationInput
from backend.services.intake.idempotency import (
    canonical_creation_payload,
    deduplication_window_start,
    fallback_deduplication_hash,
    payload_fingerprint,
)


class IdempotencyFixtures:
    @staticmethod
    def creation(
        *,
        message: str = "Billing sync failed",
        severity: Severity = Severity.WARNING,
        audience: AudienceSelection | None = None,
        channels: tuple[Channel, ...] = (Channel.WEB, Channel.EMAIL),
    ) -> NotificationCreationInput:
        return NotificationCreationInput(
            message=message,
            severity=severity,
            audience=audience
            or AudienceSelection(
                type=AudienceType.LABELS,
                labels=(
                    ("tier", "enterprise"),
                    ("region", "EU"),
                ),
            ),
            channels=channels,
        )


class TestIdempotency:
    def test_payload_fingerprint_is_stable_for_same_creation_input(self) -> None:
        first = IdempotencyFixtures.creation()
        second = IdempotencyFixtures.creation()

        assert payload_fingerprint(first) == payload_fingerprint(second)

    def test_payload_fingerprint_changes_when_payload_changes(self) -> None:
        first = IdempotencyFixtures.creation()
        second = IdempotencyFixtures.creation(message="Billing sync recovered")

        assert payload_fingerprint(first) != payload_fingerprint(second)

    def test_canonical_creation_payload_sorts_channels_and_label_keys(self) -> None:
        request = IdempotencyFixtures.creation(channels=(Channel.WEB, Channel.EMAIL))

        canonical = canonical_creation_payload(request, source_service="billing")

        assert canonical == {
            "audience": {
                "labels": {
                    "region": "EU",
                    "tier": "enterprise",
                },
                "type": "labels",
            },
            "channels": ["email", "web"],
            "message": "Billing sync failed",
            "severity": "warning",
            "source_service": "billing",
        }

    def test_duplicate_label_keys_are_rejected_before_canonicalization(self) -> None:
        with pytest.raises(ValueError, match="duplicate label"):
            IdempotencyFixtures.creation(
                audience=AudienceSelection(
                    type=AudienceType.LABELS,
                    labels=(
                        ("region", "EU"),
                        ("region", "US"),
                    ),
                )
            )

    @pytest.mark.kwparametrize(
        [
            {
                "id": "bucket-start",
                "now": datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC),
                "expected": datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC),
            },
            {
                "id": "inside-bucket",
                "now": datetime(2026, 6, 23, 12, 9, 59, tzinfo=UTC),
                "expected": datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC),
            },
            {
                "id": "next-bucket",
                "now": datetime(2026, 6, 23, 12, 10, 0, tzinfo=UTC),
                "expected": datetime(2026, 6, 23, 12, 10, 0, tzinfo=UTC),
            },
        ]
    )
    def test_deduplication_window_matrix(
        self,
        now: datetime,
        expected: datetime,
    ) -> None:
        assert deduplication_window_start(now, window=timedelta(minutes=10)) == expected

    @pytest.mark.kwparametrize(
        [
            {
                "id": "naive-now",
                "now": datetime(2026, 6, 23, 12, 0, 0),
                "window": timedelta(minutes=10),
            },
            {
                "id": "subsecond-window",
                "now": datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC),
                "window": timedelta(milliseconds=500),
            },
            {
                "id": "fractional-window",
                "now": datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC),
                "window": timedelta(seconds=1, microseconds=1),
            },
        ]
    )
    def test_deduplication_window_rejects_ambiguous_inputs(
        self,
        now: datetime,
        window: timedelta,
    ) -> None:
        with pytest.raises(ValueError):
            deduplication_window_start(now, window=window)

    def test_fallback_deduplication_hash_is_stable_inside_window(self) -> None:
        request = IdempotencyFixtures.creation()
        first_seen_at = datetime(2026, 6, 23, 12, 1, tzinfo=UTC)
        second_seen_at = datetime(2026, 6, 23, 12, 9, tzinfo=UTC)

        assert fallback_deduplication_hash(
            request,
            source_service="billing",
            now=first_seen_at,
            window=timedelta(minutes=10),
        ) == fallback_deduplication_hash(
            request,
            source_service="billing",
            now=second_seen_at,
            window=timedelta(minutes=10),
        )

    def test_fallback_deduplication_hash_changes_outside_window(self) -> None:
        request = IdempotencyFixtures.creation()

        assert fallback_deduplication_hash(
            request,
            source_service="billing",
            now=datetime(2026, 6, 23, 12, 9, tzinfo=UTC),
            window=timedelta(minutes=10),
        ) != fallback_deduplication_hash(
            request,
            source_service="billing",
            now=datetime(2026, 6, 23, 12, 10, tzinfo=UTC),
            window=timedelta(minutes=10),
        )
