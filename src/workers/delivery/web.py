from backend.db.models import NotificationDeliveryModel, NotificationRequestModel, UserModel
from backend.domain.enums import DeliveryOutcomeStatus
from backend.domain.results import DeliveryOutcome


class WebDeliveryAdapter:
    def deliver(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> DeliveryOutcome:
        return DeliveryOutcome(status=DeliveryOutcomeStatus.DELIVERED)
