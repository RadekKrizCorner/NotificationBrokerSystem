from enum import StrEnum


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Channel(StrEnum):
    WEB = "web"
    EMAIL = "email"


class AudienceType(StrEnum):
    ALL = "all"
    GROUP = "group"
    LABELS = "labels"


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    REPLAY_REQUESTED = "replay_requested"
    DELIVERED = "delivered"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"


class DeliveryOutcomeStatus(StrEnum):
    DELIVERED = "delivered"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"


class WebNotificationStatus(StrEnum):
    ALL = "all"
    UNREAD = "unread"
    READ = "read"


class NotificationCreateResultStatus(StrEnum):
    CREATED = "created"
    EXISTING = "existing"


class NotificationRequestStatus(StrEnum):
    ACCEPTED = "accepted"


class OutboxEventStatus(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"


class ActionInvocationResult(StrEnum):
    QUEUED = "queued"
    NO_ELIGIBLE = "no_eligible"


class ActionType(StrEnum):
    RETRY = "retry"


class RequestedByType(StrEnum):
    SERVICE = "service"
    USER = "user"
