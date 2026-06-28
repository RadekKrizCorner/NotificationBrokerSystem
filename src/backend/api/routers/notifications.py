from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Response, status

from backend.api.dependencies import (
    get_current_principal,
    get_notification_creation_service,
    get_producer_quota_service,
    get_retry_service,
)
from backend.api.routing import ApiRoutes, route
from backend.api.schemas.notification_requests import CreateNotificationRequest
from backend.api.schemas.notification_responses import (
    CreateNotificationResponse,
    RetryNotificationResponse,
)
from backend.core.auth import AuthenticatedPrincipal
from backend.domain.enums import ActionInvocationResult, NotificationCreateResultStatus
from backend.domain.errors import IdempotencyConflict, ProducerQuotaExceeded
from backend.services.notification_service import NotificationCreationService
from backend.services.quota_service import ProducerQuotaService
from backend.services.retry_service import RetryService


class NotificationRoutes(ApiRoutes):
    prefix = "/notifications"
    tags = ("notifications",)

    @route(
        method="POST",
        path="",
        response_model=CreateNotificationResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def create_notification(
        self,
        request: CreateNotificationRequest,
        principal: Annotated[AuthenticatedPrincipal, Depends(get_current_principal)],
        service: Annotated[
            NotificationCreationService,
            Depends(get_notification_creation_service),
        ],
        quota_service: Annotated[
            ProducerQuotaService,
            Depends(get_producer_quota_service),
        ],
    ) -> CreateNotificationResponse:
        principal.require_type("service")
        principal.require_scope("notifications:write")

        try:
            quota_service.consume(source_service=principal.subject)
        except ProducerQuotaExceeded as exc:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="producer request quota exceeded",
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc

        try:
            result = service.create_notification(
                source_service=principal.subject,
                request=request.to_domain(),
                idempotency_key=request.idempotency_key,
            )
        except IdempotencyConflict as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc

        return CreateNotificationResponse(
            notification_id=result.notification_id,
            status="accepted",
            recipient_count=result.recipient_count,
            delivery_count=result.delivery_count,
            deduplicated=result.status is NotificationCreateResultStatus.EXISTING,
        )

    @route(
        method="POST",
        path="/{notification_id}/retry",
        response_model=RetryNotificationResponse,
    )
    def retry_notification(
        self,
        notification_id: UUID,
        response: Response,
        principal: Annotated[AuthenticatedPrincipal, Depends(get_current_principal)],
        service: Annotated[RetryService, Depends(get_retry_service)],
    ) -> RetryNotificationResponse:
        principal.require_type("service")
        can_retry_any = "notifications:retry:any" in principal.scopes
        if "notifications:write" not in principal.scopes and not can_retry_any:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="required scope is missing",
            )

        try:
            result = service.retry_notification_for_service(
                notification_id=notification_id,
                requested_by_id=principal.subject,
                can_retry_any=can_retry_any,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="notification does not exist",
            ) from exc
        except PermissionError as exc:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="service cannot retry notification",
            ) from exc

        self._set_retry_status_code(response, result.status)
        return RetryNotificationResponse(
            replay_id=result.replay_id,
            status=result.status,
            replayed_delivery_count=result.replayed_delivery_count,
        )

    def _set_retry_status_code(
        self,
        response: Response,
        result: ActionInvocationResult,
    ) -> None:
        response.status_code = (
            status.HTTP_202_ACCEPTED
            if result is ActionInvocationResult.QUEUED
            else status.HTTP_200_OK
        )


router = NotificationRoutes().router
