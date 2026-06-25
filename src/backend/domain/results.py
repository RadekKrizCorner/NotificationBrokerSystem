from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from backend.domain.enums import (
    ActionInvocationResult,
    DeliveryOutcomeStatus,
    NotificationCreateResultStatus,
)


@dataclass(frozen=True, slots=True)
class NotificationCreateResult:
    notification_id: UUID
    status: NotificationCreateResultStatus


@dataclass(frozen=True, slots=True)
class NotificationFanoutResult:
    notification_id: UUID
    recipient_count: int
    delivery_count: int
    next_attempt_at: datetime


@dataclass(frozen=True, slots=True)
class RetryResult:
    notification_id: UUID
    status: ActionInvocationResult
    replay_id: UUID | None
    replayed_delivery_count: int
    requested_at: datetime


@dataclass(frozen=True, slots=True)
class MarkWebNotificationReadResult:
    web_notification_id: UUID
    read_at: datetime


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    status: DeliveryOutcomeStatus
    provider_message_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryWorkerResult:
    claimed_count: int
    processed_count: int


@dataclass(frozen=True, slots=True)
class OutboxPublisherResult:
    claimed_count: int
    published_count: int
    failed_retryable_count: int
    failed_terminal_count: int


@dataclass(frozen=True, slots=True)
class NotificationConsumerResult:
    received_count: int
    processed_count: int
    duplicate_count: int
    committed_count: int


@dataclass(frozen=True, slots=True)
class WorkloadGeneratorResult:
    sequence: int
    status_code: int | None
    success: bool
    error_message: str | None = None
