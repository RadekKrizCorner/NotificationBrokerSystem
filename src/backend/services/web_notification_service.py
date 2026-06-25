from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from uuid import UUID

from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.domain.enums import WebNotificationStatus
from backend.domain.read_models import WebNotificationRow
from backend.domain.results import MarkWebNotificationReadResult

UnitOfWorkFactory = Callable[[], AbstractContextManager[SqlAlchemyUnitOfWork]]


class WebNotificationService:
    def __init__(
        self,
        *,
        unit_of_work_factory: UnitOfWorkFactory,
        now: Callable[[], datetime],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now = now

    def list_for_user(
        self,
        *,
        user_id: UUID,
        status: WebNotificationStatus,
        limit: int,
        after: tuple[datetime, UUID] | None,
    ) -> list[WebNotificationRow]:
        with self._unit_of_work_factory() as uow:
            return uow.notifications.list_web_notifications_for_user(
                user_id=user_id,
                status=status,
                limit=limit,
                after=after,
            )

    def mark_read(
        self,
        *,
        user_id: UUID,
        web_notification_id: UUID,
    ) -> MarkWebNotificationReadResult:
        read_at = self._aware_utc_now()
        with self._unit_of_work_factory() as uow:
            delivery = uow.notifications.mark_visible_web_notification_read(
                delivery_id=web_notification_id,
                user_id=user_id,
                read_at=read_at,
            )
            if delivery is None:
                raise ValueError("web notification does not exist")
            uow.commit()
            return MarkWebNotificationReadResult(
                web_notification_id=delivery.id,
                read_at=self._as_aware_utc(delivery.read_at or read_at),
            )

    def _aware_utc_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(UTC)

    def _as_aware_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
