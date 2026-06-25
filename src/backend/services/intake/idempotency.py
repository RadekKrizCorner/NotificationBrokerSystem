import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from backend.domain.enums import AudienceType
from backend.domain.value_objects import AudienceSelection, NotificationCreationInput


def canonical_creation_payload(
    request: NotificationCreationInput,
    *,
    source_service: str,
) -> dict[str, Any]:
    return {
        "audience": canonical_audience(request.audience),
        "channels": sorted(channel.value for channel in request.channels),
        "message": request.message,
        "severity": request.severity.value,
        "source_service": source_service,
    }


def payload_fingerprint(request: NotificationCreationInput) -> str:
    payload = {
        "audience": canonical_audience(request.audience),
        "channels": sorted(channel.value for channel in request.channels),
        "message": request.message,
        "severity": request.severity.value,
    }
    return _hash_payload(payload)


def deduplication_window_start(now: datetime, *, window: timedelta) -> datetime:
    window_total_seconds = window.total_seconds()
    if window_total_seconds <= 0 or not window_total_seconds.is_integer():
        raise ValueError("window must be a positive whole-second duration")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    utc_now = now.astimezone(UTC)
    epoch_seconds = int(utc_now.timestamp())
    window_seconds = int(window_total_seconds)
    bucket_start_seconds = epoch_seconds - (epoch_seconds % window_seconds)
    return datetime.fromtimestamp(bucket_start_seconds, tz=UTC)


def fallback_deduplication_hash(
    request: NotificationCreationInput,
    *,
    source_service: str,
    now: datetime,
    window: timedelta,
) -> str:
    window_start = deduplication_window_start(now, window=window)
    payload = canonical_creation_payload(request, source_service=source_service)
    payload["deduplication_window_start"] = window_start.isoformat()
    return _hash_payload(payload)


def _hash_payload(payload: dict[str, Any]) -> str:
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(canonical_json.encode("utf-8")).hexdigest()


def canonical_audience(audience: AudienceSelection) -> dict[str, object]:
    if audience.type is AudienceType.ALL:
        return {"type": AudienceType.ALL.value}
    if audience.type is AudienceType.GROUP:
        return {
            "group": audience.group,
            "type": AudienceType.GROUP.value,
        }
    if audience.labels is None:
        raise ValueError("labels audience requires labels")
    return {
        "labels": dict(sorted(audience.labels)),
        "type": AudienceType.LABELS.value,
    }
