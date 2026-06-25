from email.message import EmailMessage
from smtplib import SMTPResponseException
from types import TracebackType
from uuid import uuid4

import pytest

from backend.db.models import NotificationDeliveryModel, NotificationRequestModel, UserModel
from backend.domain.enums import (
    AudienceType,
    Channel,
    DeliveryOutcomeStatus,
    DeliveryStatus,
    Severity,
)
from workers.delivery.email import EmailDeliveryAdapter


class FakeSMTP:
    def __init__(self) -> None:
        self.messages: list[EmailMessage] = []

    def __enter__(self) -> "FakeSMTP":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def send_message(self, message: EmailMessage) -> None:
        self.messages.append(message)


class FailingSMTP(FakeSMTP):
    def __init__(self, code: int) -> None:
        super().__init__()
        self.code = code

    def send_message(self, message: EmailMessage) -> None:
        raise SMTPResponseException(self.code, b"smtp rejected")


class EmailDeliveryFixtures:
    @staticmethod
    def notification() -> NotificationRequestModel:
        return NotificationRequestModel(
            source_service="billing",
            message="Billing sync failed",
            severity=Severity.ERROR.value,
            audience_type=AudienceType.ALL.value,
            audience={"type": "all"},
            channels=[Channel.EMAIL.value],
            status="accepted",
            idempotency_key="email-delivery",
            payload_fingerprint="fingerprint",
        )

    @staticmethod
    def delivery() -> NotificationDeliveryModel:
        return NotificationDeliveryModel(
            notification_recipient_id=uuid4(),
            notification_id=uuid4(),
            user_id=uuid4(),
            channel=Channel.EMAIL.value,
            status=DeliveryStatus.PENDING.value,
        )

    @staticmethod
    def user() -> UserModel:
        return UserModel(
            email="user@example.test",
            display_name="User Example",
        )


class TestEmailDeliveryAdapter:
    def test_sends_notification_email(self) -> None:
        smtp = FakeSMTP()
        adapter = EmailDeliveryAdapter(
            smtp_factory=lambda: smtp,
            from_address="notifications@example.test",
        )

        outcome = adapter.deliver(
            delivery=EmailDeliveryFixtures.delivery(),
            notification=EmailDeliveryFixtures.notification(),
            user=EmailDeliveryFixtures.user(),
        )

        assert outcome.status is DeliveryOutcomeStatus.DELIVERED
        assert outcome.provider_message_id is not None
        [message] = smtp.messages
        assert message["From"] == "notifications@example.test"
        assert message["To"] == "user@example.test"
        assert message["Subject"] == "[error] Billing sync failed"
        assert "Billing sync failed" in message.get_content()

    @pytest.mark.kwparametrize(
        [
            {
                "id": "temporary-smtp",
                "smtp_code": 421,
                "expected_status": DeliveryOutcomeStatus.FAILED_RETRYABLE,
            },
            {
                "id": "permanent-smtp",
                "smtp_code": 550,
                "expected_status": DeliveryOutcomeStatus.FAILED_TERMINAL,
            },
        ]
    )
    def test_classifies_smtp_response_errors(
        self,
        smtp_code: int,
        expected_status: DeliveryOutcomeStatus,
    ) -> None:
        adapter = EmailDeliveryAdapter(
            smtp_factory=lambda: FailingSMTP(smtp_code),
            from_address="notifications@example.test",
        )

        outcome = adapter.deliver(
            delivery=EmailDeliveryFixtures.delivery(),
            notification=EmailDeliveryFixtures.notification(),
            user=EmailDeliveryFixtures.user(),
        )

        assert outcome.status is expected_status
        assert outcome.error_code == str(smtp_code)
        assert outcome.error_message == "smtp rejected"
