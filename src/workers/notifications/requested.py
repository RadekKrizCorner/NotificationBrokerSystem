from collections.abc import Mapping
from typing import Protocol
from uuid import UUID

from backend.domain.results import NotificationFanoutResult


class FanoutService(Protocol):
    def fanout_notification(self, notification_id: UUID) -> NotificationFanoutResult:
        pass


class NotificationRequestedHandler:
    def __init__(self, *, fanout_service: FanoutService) -> None:
        self._fanout_service = fanout_service

    def handle(self, payload: Mapping[str, object]) -> NotificationFanoutResult:
        raw_notification_id = payload.get("notification_id")
        if not isinstance(raw_notification_id, str):
            raise ValueError("notification_id is required")
        return self._fanout_service.fanout_notification(UUID(raw_notification_id))
