from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import OutboxEventModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import OutboxEventStatus

SessionFactory = sessionmaker[Session]


class PostgresOutboxFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)

    @staticmethod
    def seed_event(
        session_factory: SessionFactory,
        *,
        status: OutboxEventStatus = OutboxEventStatus.PENDING,
        next_attempt_at: datetime | None = None,
        lease_expires_at: datetime | None = None,
        claimed_by: str | None = None,
    ) -> UUID:
        event_id = uuid4()
        with session_factory() as session:
            session.add(
                OutboxEventModel(
                    id=event_id,
                    topic="notifications.requests",
                    event_type="notification.requested",
                    aggregate_type="notification_request",
                    aggregate_id=uuid4(),
                    event_key=str(event_id),
                    payload={"notification_id": str(event_id), "source_service": "billing"},
                    status=status.value,
                    next_attempt_at=next_attempt_at or PostgresOutboxFixtures.now,
                    lease_expires_at=lease_expires_at,
                    claimed_by=claimed_by,
                    created_at=PostgresOutboxFixtures.now,
                    updated_at=PostgresOutboxFixtures.now,
                )
            )
            session.commit()
        return event_id


@pytest.mark.integration
class TestPostgresOutboxRepository:
    def test_locked_outbox_row_is_skipped_by_second_worker(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        event_id = PostgresOutboxFixtures.seed_event(postgres_session_factory)

        with SqlAlchemyUnitOfWork(postgres_session_factory) as first_uow:
            first_claimed = first_uow.outbox.claim_due_events(
                now=PostgresOutboxFixtures.now,
                limit=1,
                worker_id="worker-1",
                lease_expires_at=PostgresOutboxFixtures.now + timedelta(seconds=30),
            )
            first_claimed_ids = [event.id for event in first_claimed]

            with SqlAlchemyUnitOfWork(postgres_session_factory) as second_uow:
                second_claimed = second_uow.outbox.claim_due_events(
                    now=PostgresOutboxFixtures.now,
                    limit=1,
                    worker_id="worker-2",
                    lease_expires_at=PostgresOutboxFixtures.now + timedelta(seconds=30),
                )
                second_claimed_ids = [event.id for event in second_claimed]
                second_uow.rollback()

            first_uow.rollback()

        assert first_claimed_ids == [event_id]
        assert second_claimed_ids == []

    def test_expired_outbox_lease_can_be_reclaimed(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        event_id = PostgresOutboxFixtures.seed_event(
            postgres_session_factory,
            status=OutboxEventStatus.PUBLISHING,
            lease_expires_at=PostgresOutboxFixtures.now - timedelta(seconds=1),
            claimed_by="stale-worker",
        )

        with SqlAlchemyUnitOfWork(postgres_session_factory) as uow:
            claimed = uow.outbox.claim_due_events(
                now=PostgresOutboxFixtures.now,
                limit=1,
                worker_id="worker-2",
                lease_expires_at=PostgresOutboxFixtures.now + timedelta(seconds=30),
            )
            uow.commit()

        assert [event.id for event in claimed] == [event_id]

        with postgres_session_factory() as session:
            event = session.get(OutboxEventModel, event_id)
            assert event is not None
            assert event.status == OutboxEventStatus.PUBLISHING.value
            assert event.claimed_by == "worker-2"


@pytest.mark.integration
class TestPostgresMigrations:
    def test_migration_uses_postgres_jsonb_for_outbox_payload(
        self,
        postgres_engine: Engine,
    ) -> None:
        with postgres_engine.connect() as connection:
            payload_type = connection.scalar(
                text(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = 'outbox_events'
                    AND column_name = 'payload'
                    """
                )
            )

        assert payload_type == "jsonb"

    def test_migration_creates_partial_idempotency_indexes(
        self,
        postgres_engine: Engine,
    ) -> None:
        with postgres_engine.connect() as connection:
            index_rows = connection.execute(
                text(
                    """
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                    AND tablename = 'notification_requests'
                    """
                )
            ).all()

        partial_indexes = {row.indexname for row in index_rows if "WHERE" in row.indexdef}
        assert {
            "uq_notification_requests_source_idempotency_key",
            "uq_notification_requests_source_deduplication",
        }.issubset(partial_indexes)
