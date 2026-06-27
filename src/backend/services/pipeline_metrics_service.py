from collections.abc import Callable
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.core.metrics import PrometheusMetrics
from backend.db.models import (
    NotificationDeliveryModel,
    NotificationRequestModel,
    OutboxEventModel,
)
from backend.db.repositories.outbox import OutboxRepository
from backend.domain.enums import (
    Channel,
    DeliveryStatus,
    NotificationRequestStatus,
    OutboxEventStatus,
)


class PipelineMetricsRefresher:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        metrics: PrometheusMetrics,
        now: Callable[[], datetime],
    ) -> None:
        self._session_factory = session_factory
        self._metrics = metrics
        self._now = now

    def refresh(self) -> None:
        with self._session_factory() as session:
            self._refresh_notification_request_counts(session)
            self._refresh_outbox_event_counts(session)
            self._refresh_delivery_counts(session)
            self._metrics.set_outbox_oldest_pending_seconds(
                OutboxRepository(session).oldest_publishable_event_age_seconds(
                    now=self._now(),
                )
            )

    def _refresh_notification_request_counts(self, session: Session) -> None:
        counts = self._notification_request_counts(session)
        for status in NotificationRequestStatus:
            self._metrics.set_notification_request_status_count(
                status=status.value,
                count=counts.get(status.value, 0),
            )

    def _refresh_outbox_event_counts(self, session: Session) -> None:
        counts = self._outbox_event_counts(session)
        for status in OutboxEventStatus:
            self._metrics.set_outbox_event_status_count(
                status=status.value,
                count=counts.get(status.value, 0),
            )

    def _refresh_delivery_counts(self, session: Session) -> None:
        counts = self._delivery_counts(session)
        for status in DeliveryStatus:
            self._metrics.set_delivery_status_count(
                status=status.value,
                count=counts.get(status.value, 0),
            )
        channel_counts = self._delivery_channel_counts(session)
        for channel in Channel:
            for status in DeliveryStatus:
                self._metrics.set_delivery_channel_status_count(
                    channel=channel.value,
                    status=status.value,
                    count=channel_counts.get((channel.value, status.value), 0),
                )

    def _notification_request_counts(self, session: Session) -> dict[str, int]:
        rows = session.execute(
            select(
                NotificationRequestModel.status, func.count(NotificationRequestModel.id)
            ).group_by(NotificationRequestModel.status)
        )
        return {str(status): int(count) for status, count in rows}

    def _outbox_event_counts(self, session: Session) -> dict[str, int]:
        rows = session.execute(
            select(OutboxEventModel.status, func.count(OutboxEventModel.id)).group_by(
                OutboxEventModel.status
            )
        )
        return {str(status): int(count) for status, count in rows}

    def _delivery_counts(self, session: Session) -> dict[str, int]:
        rows = session.execute(
            select(
                NotificationDeliveryModel.status, func.count(NotificationDeliveryModel.id)
            ).group_by(NotificationDeliveryModel.status)
        )
        return {str(status): int(count) for status, count in rows}

    def _delivery_channel_counts(self, session: Session) -> dict[tuple[str, str], int]:
        rows = session.execute(
            select(
                NotificationDeliveryModel.channel,
                NotificationDeliveryModel.status,
                func.count(NotificationDeliveryModel.id),
            ).group_by(NotificationDeliveryModel.channel, NotificationDeliveryModel.status)
        )
        return {(str(channel), str(status)): int(count) for channel, status, count in rows}
