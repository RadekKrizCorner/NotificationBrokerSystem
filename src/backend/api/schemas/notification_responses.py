from uuid import UUID

from pydantic import BaseModel, ConfigDict

from backend.domain.enums import ActionInvocationResult, Severity


class CreateNotificationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    notification_id: UUID
    status: str
    recipient_count: int
    delivery_count: int
    deduplicated: bool


class WebNotificationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    notification_id: UUID
    message: str
    severity: Severity
    read_at: str | None
    delivered_at: str
    created_at: str


class WebNotificationListResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    items: list[WebNotificationResponse]
    next_cursor: str | None


class MarkWebNotificationReadResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    read_at: str


class RetryNotificationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    replay_id: UUID | None
    status: ActionInvocationResult
    replayed_delivery_count: int
