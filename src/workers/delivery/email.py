from collections.abc import Callable
from email.message import EmailMessage
from email.utils import make_msgid
from smtplib import SMTP, SMTPException, SMTPResponseException
from types import TracebackType
from typing import Protocol

from backend.db.models import NotificationDeliveryModel, NotificationRequestModel, UserModel
from backend.domain.enums import DeliveryOutcomeStatus
from backend.domain.results import DeliveryOutcome


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
    ) -> None:
        self._smtp_factory = smtp_factory
        self._from_address = from_address

    @classmethod
    def for_mailpit(
        cls,
        *,
        host: str = "localhost",
        port: int = 1025,
        from_address: str = "notifications@example.test",
        timeout_seconds: float = 10.0,
    ) -> "EmailDeliveryAdapter":
        return cls(
            smtp_factory=lambda: SMTP(host=host, port=port, timeout=timeout_seconds),
            from_address=from_address,
        )

    def deliver(
        self,
        *,
        delivery: NotificationDeliveryModel,
        notification: NotificationRequestModel,
        user: UserModel,
    ) -> DeliveryOutcome:
        message = self._message(notification=notification, user=user)
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

    def _message(self, *, notification: NotificationRequestModel, user: UserModel) -> EmailMessage:
        message = EmailMessage()
        message["From"] = self._from_address
        message["To"] = user.email
        message["Subject"] = f"[{notification.severity}] {notification.message}"
        message["Message-ID"] = make_msgid()
        message.set_content(notification.message)
        return message

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
