from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Annotated, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.core.auth import AuthenticatedPrincipal, decode_bearer_token
from backend.core.config import Settings
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.services.notification_service import NotificationCreationService
from backend.services.retry_service import RetryService
from backend.services.web_notification_service import WebNotificationService

bearer_scheme = HTTPBearer(auto_error=False)


def get_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_unit_of_work_factory(request: Request) -> Callable[[], SqlAlchemyUnitOfWork]:
    return cast(Callable[[], SqlAlchemyUnitOfWork], request.app.state.unit_of_work_factory)


def get_now(request: Request) -> Callable[[], datetime]:
    return cast(Callable[[], datetime], request.app.state.now)


def get_current_principal(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> AuthenticatedPrincipal:
    return decode_bearer_token(
        credentials,
        jwt_secret=settings.jwt_secret,
        jwt_algorithm=settings.jwt_algorithm,
        jwt_issuer=settings.jwt_issuer,
        jwt_audience=settings.jwt_audience,
    )


def get_notification_creation_service(
    unit_of_work_factory: Annotated[
        Callable[[], SqlAlchemyUnitOfWork],
        Depends(get_unit_of_work_factory),
    ],
    now: Annotated[Callable[[], datetime], Depends(get_now)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> NotificationCreationService:
    return NotificationCreationService(
        unit_of_work_factory=unit_of_work_factory,
        now=now,
        fallback_deduplication_window=timedelta(
            seconds=settings.fallback_deduplication_window_seconds,
        ),
    )


def get_web_notification_service(
    unit_of_work_factory: Annotated[
        Callable[[], SqlAlchemyUnitOfWork],
        Depends(get_unit_of_work_factory),
    ],
    now: Annotated[Callable[[], datetime], Depends(get_now)],
) -> WebNotificationService:
    return WebNotificationService(unit_of_work_factory=unit_of_work_factory, now=now)


def get_retry_service(
    unit_of_work_factory: Annotated[
        Callable[[], SqlAlchemyUnitOfWork],
        Depends(get_unit_of_work_factory),
    ],
    now: Annotated[Callable[[], datetime], Depends(get_now)],
) -> RetryService:
    return RetryService(unit_of_work_factory=unit_of_work_factory, now=now)
