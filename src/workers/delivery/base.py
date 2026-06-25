from typing import Protocol

from backend.db.models import NotificationDeliveryModel, NotificationRequestModel, UserModel
from backend.domain.results import DeliveryOutcome


class DeliveryAdapter(Protocol):
    def deliver(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> DeliveryOutcome:
        pass
