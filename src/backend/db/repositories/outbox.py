from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from backend.db.models import OutboxEventModel
from backend.domain.enums import OutboxEventStatus


class OutboxRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, event: OutboxEventModel) -> None:
        self._session.add(event)

    def claim_due_events(
        self,
        *,
        now: datetime,
        limit: int,
        worker_id: str,
        lease_expires_at: datetime,
    ) -> list[OutboxEventModel]:
        if limit <= 0:
            return []

        statement = (
            select(OutboxEventModel)
            .where(self._publishable_event_predicate(now))
            .order_by(
                OutboxEventModel.next_attempt_at,
                OutboxEventModel.created_at,
                OutboxEventModel.id,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        events = list(self._session.scalars(statement))
        for event in events:
            event.status = OutboxEventStatus.PUBLISHING.value
            event.claimed_by = worker_id
            event.claim_token = uuid4()
            event.lease_expires_at = lease_expires_at
            event.updated_at = now
        return events

    def get_claimed_event_for_update(
        self,
        *,
        event_id: UUID,
        claim_token: UUID,
    ) -> OutboxEventModel | None:
        statement = (
            select(OutboxEventModel)
            .where(
                OutboxEventModel.id == event_id,
                OutboxEventModel.status == OutboxEventStatus.PUBLISHING.value,
                OutboxEventModel.claim_token == claim_token,
            )
            .with_for_update()
        )
        return self._session.scalar(statement)

    def oldest_publishable_event_age_seconds(self, *, now: datetime) -> float:
        statement = select(func.min(OutboxEventModel.created_at)).where(
            self._publishable_event_predicate(now)
        )
        oldest_created_at = self._session.scalar(statement)
        if oldest_created_at is None:
            return 0.0

        compatible_now = self._compatible_now(now=now, value=oldest_created_at)
        return max((compatible_now - oldest_created_at).total_seconds(), 0.0)

    def mark_published(self, event: OutboxEventModel, *, published_at: datetime) -> None:
        event.status = OutboxEventStatus.PUBLISHED.value
        event.published_at = published_at
        event.next_attempt_at = published_at
        event.claimed_by = None
        event.claim_token = None
        event.lease_expires_at = None
        event.last_error = None
        event.updated_at = published_at

    def mark_publish_failed(
        self,
        event: OutboxEventModel,
        *,
        failed_at: datetime,
        retry_at: datetime,
        error_message: str,
        max_attempts: int,
    ) -> OutboxEventStatus:
        event.attempts = (event.attempts or 0) + 1
        event.last_error = error_message
        event.claimed_by = None
        event.claim_token = None
        event.lease_expires_at = None
        event.updated_at = failed_at

        if event.attempts >= max_attempts:
            event.status = OutboxEventStatus.FAILED_TERMINAL.value
            event.next_attempt_at = failed_at
            return OutboxEventStatus.FAILED_TERMINAL

        event.status = OutboxEventStatus.FAILED_RETRYABLE.value
        event.next_attempt_at = retry_at
        return OutboxEventStatus.FAILED_RETRYABLE

    def _publishable_event_predicate(self, now: datetime) -> ColumnElement[bool]:
        ready_to_publish = and_(
            OutboxEventModel.status.in_(
                [
                    OutboxEventStatus.PENDING.value,
                    OutboxEventStatus.FAILED_RETRYABLE.value,
                ]
            ),
            OutboxEventModel.next_attempt_at <= now,
        )
        expired_publishing_lease = and_(
            OutboxEventModel.status == OutboxEventStatus.PUBLISHING.value,
            OutboxEventModel.lease_expires_at < now,
        )
        return or_(ready_to_publish, expired_publishing_lease)

    def _compatible_now(self, *, now: datetime, value: datetime) -> datetime:
        if value.tzinfo is None and now.tzinfo is not None:
            return now.replace(tzinfo=None)
        if value.tzinfo is not None and now.tzinfo is None:
            return now.replace(tzinfo=value.tzinfo)
        return now
