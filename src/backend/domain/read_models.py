from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from backend.domain.enums import Severity


@dataclass(frozen=True, slots=True)
class WebNotificationRow:
    id: UUID
    notification_id: UUID
    message: str
    severity: Severity
    read_at: datetime | None
    delivered_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WebNotificationCursor:
    delivered_at: datetime
    delivery_id: UUID
