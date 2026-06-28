from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import ProducerQuotaModel
from backend.db.unit_of_work import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

SessionFactory = sessionmaker[Session]


class TestPostgresProducerQuotaRepository:
    def test_concurrent_increments_are_atomic(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        window_start = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

        def increment() -> int:
            with SqlAlchemyUnitOfWork(postgres_session_factory) as uow:
                count = uow.producer_quotas.increment(
                    source_service="billing",
                    window_start=window_start,
                )
                uow.commit()
                return count

        with ThreadPoolExecutor(max_workers=8) as executor:
            counts = list(executor.map(lambda _: increment(), range(20)))

        assert sorted(counts) == list(range(1, 21))
        with postgres_session_factory() as session:
            quota = session.scalar(
                select(ProducerQuotaModel).where(
                    ProducerQuotaModel.source_service == "billing",
                    ProducerQuotaModel.window_start == window_start,
                )
            )
            assert quota is not None
            assert quota.request_count == 20
