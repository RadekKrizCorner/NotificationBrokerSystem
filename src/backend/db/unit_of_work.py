from types import TracebackType

from sqlalchemy.orm import Session, sessionmaker

from backend.db.repositories import (
    NotificationRepository,
    OutboxRepository,
    ProcessedEventRepository,
    ProducerQuotaRepository,
    UserRepository,
)


class SqlAlchemyUnitOfWork:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self.session: Session
        self.notifications: NotificationRepository
        self.outbox: OutboxRepository
        self.processed_events: ProcessedEventRepository
        self.producer_quotas: ProducerQuotaRepository
        self.users: UserRepository

    def __enter__(self) -> SqlAlchemyUnitOfWork:
        self.session = self._session_factory()
        self.notifications = NotificationRepository(self.session)
        self.outbox = OutboxRepository(self.session)
        self.processed_events = ProcessedEventRepository(self.session)
        self.producer_quotas = ProducerQuotaRepository(self.session)
        self.users = UserRepository(self.session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.rollback()
        self.session.close()

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()
