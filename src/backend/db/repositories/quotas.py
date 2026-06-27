from datetime import datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from backend.db.models import ProducerQuotaModel


class ProducerQuotaRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def increment(
        self,
        *,
        source_service: str,
        window_start: datetime,
    ) -> int:
        bind = self._session.get_bind()
        if bind.dialect.name == "postgresql":
            insert_statement: Any = postgresql_insert(ProducerQuotaModel)
        elif bind.dialect.name == "sqlite":
            insert_statement = sqlite_insert(ProducerQuotaModel)
        else:
            raise RuntimeError(
                f"producer quotas do not support {bind.dialect.name}"
            )

        statement = (
            insert_statement.values(
                source_service=source_service,
                window_start=window_start,
                request_count=1,
            )
            .on_conflict_do_update(
                index_elements=[
                    ProducerQuotaModel.source_service,
                    ProducerQuotaModel.window_start,
                ],
                set_={
                    "request_count": ProducerQuotaModel.request_count + 1,
                },
            )
            .returning(ProducerQuotaModel.request_count)
        )
        request_count = self._session.scalar(statement)
        if request_count is None:
            raise RuntimeError("quota increment did not return a count")
        return int(request_count)
