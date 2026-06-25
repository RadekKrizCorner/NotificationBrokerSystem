import base64
import hashlib
import hmac
import json
from datetime import datetime
from uuid import UUID

from fastapi import HTTPException, status

from backend.domain.read_models import WebNotificationCursor


class WebNotificationCursorCodec:
    def __init__(self, *, secret: str) -> None:
        self._secret = secret

    def encode(self, cursor: WebNotificationCursor) -> str:
        payload = {
            "delivered_at": cursor.delivered_at.isoformat(),
            "delivery_id": str(cursor.delivery_id),
        }
        body = _json_bytes(payload)
        signature = self._sign(body)
        envelope = {
            "payload": payload,
            "signature": signature,
        }
        return base64.urlsafe_b64encode(_json_bytes(envelope)).decode("ascii").rstrip("=")

    def decode(self, cursor: str) -> WebNotificationCursor:
        try:
            envelope = json.loads(self._base64url_decode(cursor))
            payload = envelope["payload"]
            signature = envelope["signature"]
            body = _json_bytes(payload)
            if not hmac.compare_digest(signature, self._sign(body)):
                raise ValueError("cursor signature mismatch")
            return WebNotificationCursor(
                delivered_at=datetime.fromisoformat(payload["delivered_at"]),
                delivery_id=UUID(payload["delivery_id"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="invalid cursor",
            ) from exc

    def _sign(self, body: bytes) -> str:
        return hmac.new(self._secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    def _base64url_decode(self, value: str) -> str:
        padding = "=" * (-len(value) % 4)
        return base64.urlsafe_b64decode(value + padding).decode("utf-8")


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
