from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.db.models import ProcessedEventModel


class ProcessedEventRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def exists(self, *, event_id: UUID, consumer_name: str) -> bool:
        same_event = ProcessedEventModel.event_id == event_id
        same_consumer = ProcessedEventModel.consumer_name == consumer_name

        statement = select(ProcessedEventModel.event_id).where(same_event, same_consumer)
        return self._session.scalar(statement) is not None

    def add(self, *, event_id: UUID, consumer_name: str, processed_at: datetime) -> None:
        self._session.add(
            ProcessedEventModel(
                event_id=event_id,
                consumer_name=consumer_name,
                processed_at=processed_at,
            )
        )
