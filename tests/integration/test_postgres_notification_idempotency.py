from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import NotificationRequestModel
from backend.domain.enums import AudienceType, Channel, NotificationRequestStatus, Severity

SessionFactory = sessionmaker[Session]


class PostgresNotificationFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def notification(
        *,
        idempotency_key: str | None,
        deduplication_hash: str | None,
        deduplication_window_start: datetime | None,
    ) -> NotificationRequestModel:
        return NotificationRequestModel(
            source_service="billing",
            message="Billing sync failed",
            severity=Severity.WARNING.value,
            audience_type=AudienceType.ALL.value,
            audience={"type": AudienceType.ALL.value},
            channels=[Channel.WEB.value],
            status=NotificationRequestStatus.ACCEPTED.value,
            idempotency_key=idempotency_key,
            payload_fingerprint=str(uuid4()),
            deduplication_hash=deduplication_hash,
            deduplication_window_start=deduplication_window_start,
            created_at=PostgresNotificationFixtures.now,
            updated_at=PostgresNotificationFixtures.now,
        )


@pytest.mark.integration
class TestPostgresNotificationIdempotency:
    @pytest.mark.kwparametrize(
        [
            {
                "id": "explicit-idempotency-key",
                "idempotency_key": "billing-sync-1",
                "deduplication_hash": None,
                "deduplication_window_start": None,
            },
            {
                "id": "fallback-deduplication-window",
                "idempotency_key": None,
                "deduplication_hash": "same-deterministic-hash",
                "deduplication_window_start": PostgresNotificationFixtures.now,
            },
        ]
    )
    def test_database_rejects_duplicate_notification_identity(
        self,
        postgres_session_factory: SessionFactory,
        idempotency_key: str | None,
        deduplication_hash: str | None,
        deduplication_window_start: datetime | None,
    ) -> None:
        with postgres_session_factory() as session:
            session.add(
                PostgresNotificationFixtures.notification(
                    idempotency_key=idempotency_key,
                    deduplication_hash=deduplication_hash,
                    deduplication_window_start=deduplication_window_start,
                )
            )
            session.commit()

            session.add(
                PostgresNotificationFixtures.notification(
                    idempotency_key=idempotency_key,
                    deduplication_hash=deduplication_hash,
                    deduplication_window_start=deduplication_window_start,
                )
            )
            with pytest.raises(IntegrityError):
                session.commit()
