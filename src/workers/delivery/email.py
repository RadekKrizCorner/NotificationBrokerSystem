from collections.abc import Callable
from email.message import EmailMessage
from pathlib import Path
from smtplib import SMTP, SMTPException, SMTPResponseException
from types import TracebackType
from typing import Protocol

from backend.db.models import NotificationDeliveryModel, NotificationRequestModel, UserModel
from backend.domain.enums import DeliveryOutcomeStatus
from backend.domain.results import DeliveryOutcome
from workers.delivery.email_templates import EmailTemplateError, EmailTemplateRenderer


class SmtpClient(Protocol):
    def send_message(self, message: EmailMessage) -> object:
        pass


class SmtpSession(Protocol):
    def __enter__(self) -> SmtpClient:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> object:
        pass


class EmailDeliveryAdapter:
    def __init__(
        self,
        *,
        smtp_factory: Callable[[], SmtpSession],
        from_address: str,
        template_directory: Path | None = None,
    ) -> None:
        self._smtp_factory = smtp_factory
        self._from_address = from_address
        self._renderer = (
            EmailTemplateRenderer.default()
            if template_directory is None
            else EmailTemplateRenderer(template_directory=template_directory)
        )

    @classmethod
    def for_mailpit(
        cls,
        *,
        host: str = "localhost",
        port: int = 1025,
        from_address: str = "notifications@example.test",
        timeout_seconds: float = 10.0,
        template_directory: Path | None = None,
    ) -> EmailDeliveryAdapter:
        return cls(
            smtp_factory=lambda: SMTP(host=host, port=port, timeout=timeout_seconds),
            from_address=from_address,
            template_directory=template_directory,
        )

    def deliver(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> DeliveryOutcome:
        try:
            message = self._message(
                delivery=delivery,
                notification=notification,
                user=user,
            )
        except EmailTemplateError:
            return DeliveryOutcome(
                status=DeliveryOutcomeStatus.FAILED_TERMINAL,
                error_code="email_template_error",
                error_message="email template rendering failed",
            )
        except ValueError:
            return DeliveryOutcome(
                status=DeliveryOutcomeStatus.FAILED_TERMINAL,
                error_code="email_message_error",
                error_message="email headers or identifiers are invalid",
            )
        try:
            with self._smtp_factory() as smtp:
                smtp.send_message(message)
        except SMTPResponseException as exc:
            return self._smtp_response_outcome(exc)
        except (OSError, SMTPException) as exc:
            return DeliveryOutcome(
                status=DeliveryOutcomeStatus.FAILED_RETRYABLE,
                error_code=exc.__class__.__name__,
                error_message=str(exc),
            )

        return DeliveryOutcome(
            status=DeliveryOutcomeStatus.DELIVERED,
            provider_message_id=message["Message-ID"],
        )

    def _message(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> EmailMessage:
        rendered = self._renderer.render(
            context={
                "delivery": {"id": str(delivery.id)},
                "notification": {
                    "id": str(notification.id),
                    "message": notification.message,
                    "severity": notification.severity,
                    "source_service": notification.source_service,
                },
                "user": {
                    "id": str(user.id),
                    "display_name": user.display_name,
                    "email": user.email,
                },
            }
        )
        message = EmailMessage()
        message["From"] = self._from_address
        message["To"] = user.email
        message["Subject"] = rendered.subject
        message["Message-ID"] = self._message_id(delivery)
        message.set_content(rendered.plain_body)
        message.add_alternative(rendered.html_body, subtype="html")
        return message

    def _message_id(self, delivery: NotificationDeliveryModel) -> str:
        if delivery.id is None:
            raise ValueError("delivery id is required")
        _, separator, domain = self._from_address.rpartition("@")
        if not separator or not domain or any(character in domain for character in "\r\n <>"):
            raise ValueError("from address must contain a safe domain")
        return f"<notification-{delivery.id}@{domain.lower()}>"

    def _smtp_response_outcome(self, exc: SMTPResponseException) -> DeliveryOutcome:
        status = (
            DeliveryOutcomeStatus.FAILED_RETRYABLE
            if 400 <= exc.smtp_code < 500
            else DeliveryOutcomeStatus.FAILED_TERMINAL
        )
        error_message = (
            exc.smtp_error.decode("utf-8", errors="replace")
            if isinstance(exc.smtp_error, bytes)
            else exc.smtp_error
        )
        return DeliveryOutcome(
            status=status,
            error_code=str(exc.smtp_code),
            error_message=error_message,
        )
