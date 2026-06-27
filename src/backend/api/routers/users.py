from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Query, Response
from fastapi import status as http_status

from backend.api.dependencies import (
    get_current_principal,
    get_retry_service,
    get_settings,
    get_web_notification_service,
)
from backend.api.routing import ApiRoutes, route
from backend.api.schemas.notification_responses import (
    MarkWebNotificationReadResponse,
    RetryNotificationResponse,
    WebNotificationListResponse,
    WebNotificationResponse,
)
from backend.core.auth import AuthenticatedPrincipal
from backend.core.config import Settings
from backend.core.cursors import WebNotificationCursorCodec
from backend.domain.enums import ActionInvocationResult, WebNotificationStatus
from backend.domain.read_models import WebNotificationCursor, WebNotificationRow
from backend.services.retry_service import RetryService
from backend.services.web_notification_service import WebNotificationService


class UserRoutes(ApiRoutes):
    prefix = "/me"
    tags = ("users", "me")

    @route(
        method="GET",
        path="/notifications",
        response_model=WebNotificationListResponse,
    )
    def list_notifications(
        self,
        principal: Annotated[AuthenticatedPrincipal, Depends(get_current_principal)],
        service: Annotated[WebNotificationService, Depends(get_web_notification_service)],
        settings: Annotated[Settings, Depends(get_settings)],
        status: WebNotificationStatus = WebNotificationStatus.ALL,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query(max_length=2_048)] = None,
    ) -> WebNotificationListResponse:
        principal.require_type("user")
        principal.require_scope("notifications:read")
        user_id = self._user_id_from_principal(principal)
        after = None
        cursor_codec = WebNotificationCursorCodec(secret=settings.jwt_secret)
        if cursor is not None:
            decoded_cursor = cursor_codec.decode(cursor)
            after = (decoded_cursor.delivered_at, decoded_cursor.delivery_id)

        rows = service.list_for_user(
            user_id=user_id,
            status=status,
            limit=limit + 1,
            after=after,
        )
        page_rows = rows[:limit]
        next_cursor = self._next_cursor(
            page_rows,
            has_more=len(rows) > limit,
            cursor_codec=cursor_codec,
        )
        return WebNotificationListResponse(
            items=[self._response_from_row(row) for row in page_rows],
            next_cursor=next_cursor,
        )

    @route(
        method="POST",
        path="/notifications/{web_notification_id}/read",
        response_model=MarkWebNotificationReadResponse,
    )
    def mark_notification_read(
        self,
        web_notification_id: UUID,
        principal: Annotated[AuthenticatedPrincipal, Depends(get_current_principal)],
        service: Annotated[WebNotificationService, Depends(get_web_notification_service)],
    ) -> MarkWebNotificationReadResponse:
        principal.require_type("user")
        principal.require_scope("notifications:read")
        user_id = self._user_id_from_principal(principal)

        try:
            result = service.mark_read(
                user_id=user_id,
                web_notification_id=web_notification_id,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail="web notification does not exist",
            ) from exc

        return MarkWebNotificationReadResponse(
            id=result.web_notification_id,
            read_at=result.read_at.isoformat(),
        )

    @route(
        method="POST",
        path="/notifications/{web_notification_id}/actions/retry",
        response_model=RetryNotificationResponse,
    )
    def retry_notification_action(
        self,
        web_notification_id: UUID,
        response: Response,
        principal: Annotated[AuthenticatedPrincipal, Depends(get_current_principal)],
        service: Annotated[RetryService, Depends(get_retry_service)],
    ) -> RetryNotificationResponse:
        principal.require_type("user")
        principal.require_scope("notifications:read")
        user_id = self._user_id_from_principal(principal)

        try:
            result = service.retry_user_notification(
                web_notification_id=web_notification_id,
                user_id=user_id,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail="web notification does not exist",
            ) from exc

        self._set_retry_status_code(response, result.status)
        return RetryNotificationResponse(
            replay_id=result.replay_id,
            status=result.status,
            replayed_delivery_count=result.replayed_delivery_count,
        )

    def _next_cursor(
        self,
        rows: list[WebNotificationRow],
        *,
        has_more: bool,
        cursor_codec: WebNotificationCursorCodec,
    ) -> str | None:
        if not has_more or not rows:
            return None
        last = rows[-1]
        return cursor_codec.encode(
            WebNotificationCursor(delivered_at=last.delivered_at, delivery_id=last.id),
        )

    def _user_id_from_principal(self, principal: AuthenticatedPrincipal) -> UUID:
        try:
            return UUID(principal.subject)
        except ValueError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_401_UNAUTHORIZED,
                detail="invalid user subject",
            ) from exc

    def _set_retry_status_code(
        self,
        response: Response,
        result: ActionInvocationResult,
    ) -> None:
        response.status_code = (
            http_status.HTTP_202_ACCEPTED
            if result is ActionInvocationResult.QUEUED
            else http_status.HTTP_200_OK
        )

    def _response_from_row(self, row: WebNotificationRow) -> WebNotificationResponse:
        return WebNotificationResponse(
            id=row.id,
            notification_id=row.notification_id,
            message=row.message,
            severity=row.severity,
            read_at=row.read_at.isoformat() if row.read_at is not None else None,
            delivered_at=row.delivered_at.isoformat(),
            created_at=row.created_at.isoformat(),
        )


router = UserRoutes().router
