from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import Base, ProducerQuotaModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.errors import ProducerQuotaExceeded
from backend.services.quota_service import ProducerQuotaService

SessionFactory = sessionmaker[Session]


@pytest.fixture()
def session_factory() -> Iterator[SessionFactory]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


class TestProducerQuotaService:
    def test_rejects_request_after_fixed_window_limit(
        self,
        session_factory: SessionFactory,
    ) -> None:
        service = ProducerQuotaService(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            now=lambda: datetime(2026, 6, 27, 12, 0, 30, tzinfo=UTC),
            limit=1,
            window=timedelta(minutes=1),
        )

        service.consume(source_service="billing")
        with pytest.raises(ProducerQuotaExceeded) as error:
            service.consume(source_service="billing")

        assert error.value.retry_after_seconds == 30
        with session_factory() as session:
            assert session.scalar(select(func.count(ProducerQuotaModel.source_service))) == 1
            quota = session.scalar(select(ProducerQuotaModel))
            assert quota is not None
            assert quota.request_count == 2

    def test_uses_independent_windows(self, session_factory: SessionFactory) -> None:
        current = [datetime(2026, 6, 27, 12, 0, 59, tzinfo=UTC)]
        service = ProducerQuotaService(
            unit_of_work_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
            now=lambda: current[0],
            limit=1,
            window=timedelta(minutes=1),
        )
        service.consume(source_service="billing")
        current[0] = datetime(2026, 6, 27, 12, 1, tzinfo=UTC)

        service.consume(source_service="billing")
