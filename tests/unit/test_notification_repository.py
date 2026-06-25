from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import Mock

from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.orm import Session

from backend.db.repositories.notifications import NotificationRepository
from backend.domain.enums import Channel


class RepositoryFixtures:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)


class TestNotificationRepository:
    def test_claim_due_deliveries_uses_skip_locked_for_parallel_workers(self) -> None:
        session = Mock(spec=Session)
        session.scalars.return_value = []
        repository = NotificationRepository(cast(Session, session))

        repository.claim_due_deliveries(
            now=RepositoryFixtures.now,
            limit=100,
            worker_id="worker-1",
            lease_expires_at=RepositoryFixtures.now + timedelta(seconds=30),
        )

        statement = session.scalars.call_args.args[0]
        dialect_factory = cast(Any, PGDialect_psycopg)
        compiled = statement.compile(dialect=dialect_factory())

        assert "FOR UPDATE SKIP LOCKED" in str(compiled)

    def test_claim_due_deliveries_filters_by_channels_when_configured(self) -> None:
        session = Mock(spec=Session)
        session.scalars.return_value = []
        repository = NotificationRepository(cast(Session, session))

        repository.claim_due_deliveries(
            now=RepositoryFixtures.now,
            limit=100,
            worker_id="web-worker-1",
            lease_expires_at=RepositoryFixtures.now + timedelta(seconds=30),
            channels=(Channel.WEB,),
        )

        statement = session.scalars.call_args.args[0]
        dialect_factory = cast(Any, PGDialect_psycopg)
        compiled = statement.compile(
            dialect=dialect_factory(),
            compile_kwargs={"literal_binds": True},
        )

        assert "notification_deliveries.channel IN ('web')" in str(compiled)
