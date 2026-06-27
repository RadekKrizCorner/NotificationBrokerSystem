from email.message import EmailMessage
from pathlib import Path
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

    def __enter__(self) -> FakeSMTP:
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
            id=uuid4(),
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
        plain_body = message.get_body(preferencelist=("plain",))
        html_body = message.get_body(preferencelist=("html",))
        assert plain_body is not None
        assert html_body is not None
        assert "Billing sync failed" in plain_body.get_content()
        assert "Billing sync failed" in html_body.get_content()

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

    def test_renders_multiline_plain_and_autoescaped_html_with_stable_message_id(
        self,
    ) -> None:
        smtp = FakeSMTP()
        adapter = EmailDeliveryAdapter(
            smtp_factory=lambda: smtp,
            from_address="notifications@example.test",
        )
        delivery = EmailDeliveryFixtures.delivery()
        notification = EmailDeliveryFixtures.notification()
        notification.message = "First line\nSecond line <script>alert(1)</script>"

        first = adapter.deliver(
            delivery=delivery,
            notification=notification,
            user=EmailDeliveryFixtures.user(),
        )
        second = adapter.deliver(
            delivery=delivery,
            notification=notification,
            user=EmailDeliveryFixtures.user(),
        )

        assert first.status is DeliveryOutcomeStatus.DELIVERED
        assert first.provider_message_id == second.provider_message_id
        [first_message, second_message] = smtp.messages
        assert first_message["Message-ID"] == second_message["Message-ID"]
        assert "\n" not in str(first_message["Subject"])
        plain_body = first_message.get_body(preferencelist=("plain",))
        html_body = first_message.get_body(preferencelist=("html",))
        assert plain_body is not None
        assert html_body is not None
        assert "<script>alert(1)</script>" in plain_body.get_content()
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html_body.get_content()
        assert "<script>alert(1)</script>" not in html_body.get_content()

    def test_uses_templates_from_configured_directory(
        self,
        tmp_path: Path,
    ) -> None:
        (tmp_path / "subject.j2").write_text("Custom {{ user.display_name }}")
        (tmp_path / "plain.txt.j2").write_text("Plain: {{ notification.message }}")
        (tmp_path / "html.html.j2").write_text("<b>{{ notification.message }}</b>")
        smtp = FakeSMTP()
        adapter = EmailDeliveryAdapter(
            smtp_factory=lambda: smtp,
            from_address="notifications@example.test",
            template_directory=tmp_path,
        )
        delivery = EmailDeliveryFixtures.delivery()
        outcome = adapter.deliver(
            delivery=delivery,
            notification=EmailDeliveryFixtures.notification(),
            user=EmailDeliveryFixtures.user(),
        )

        assert outcome.status is DeliveryOutcomeStatus.DELIVERED
        [message] = smtp.messages
        assert message["Subject"] == "Custom User Example"
        plain_body = message.get_body(preferencelist=("plain",))
        html_body = message.get_body(preferencelist=("html",))
        assert plain_body is not None and plain_body.get_content().startswith("Plain:")
        assert html_body is not None and "<b>Billing sync failed</b>" in html_body.get_content()

    def test_missing_template_is_a_terminal_delivery_failure(self, tmp_path: Path) -> None:
        smtp = FakeSMTP()
        adapter = EmailDeliveryAdapter(
            smtp_factory=lambda: smtp,
            from_address="notifications@example.test",
            template_directory=tmp_path,
        )

        outcome = adapter.deliver(
            delivery=EmailDeliveryFixtures.delivery(),
            notification=EmailDeliveryFixtures.notification(),
            user=EmailDeliveryFixtures.user(),
        )

        assert outcome.status is DeliveryOutcomeStatus.FAILED_TERMINAL
        assert outcome.error_code == "email_template_error"
        assert smtp.messages == []
