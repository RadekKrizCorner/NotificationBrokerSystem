from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from math import ceil

from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.errors import ProducerQuotaExceeded

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class ProducerQuotaService:
    def __init__(
        self,
        *,
        unit_of_work_factory: UnitOfWorkFactory,
        now: Callable[[], datetime],
        limit: int,
        window: timedelta,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if window.total_seconds() <= 0 or not window.total_seconds().is_integer():
            raise ValueError("window must be a positive whole-second duration")
        self._unit_of_work_factory = unit_of_work_factory
        self._now = now
        self._limit = limit
        self._window = window

    def consume(self, *, source_service: str) -> None:
        now = self._aware_utc_now()
        window_seconds = int(self._window.total_seconds())
        epoch_seconds = int(now.timestamp())
        window_start_seconds = epoch_seconds - (epoch_seconds % window_seconds)
        window_start = datetime.fromtimestamp(window_start_seconds, tz=UTC)

        with self._unit_of_work_factory() as uow:
            request_count = uow.producer_quotas.increment(
                source_service=source_service,
                window_start=window_start,
            )
            uow.commit()

        if request_count > self._limit:
            retry_at = window_start + self._window
            raise ProducerQuotaExceeded(
                retry_after_seconds=max(
                    ceil((retry_at - now).total_seconds()),
                    1,
                )
            )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)
