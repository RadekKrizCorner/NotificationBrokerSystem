from datetime import datetime

from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.db.models.base import Base


class ProducerQuotaModel(Base):
    __tablename__ = "producer_quotas"

    source_service: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
    )
    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    request_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
