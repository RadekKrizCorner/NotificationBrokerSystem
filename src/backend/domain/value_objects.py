from dataclasses import dataclass

from backend.domain.enums import AudienceType, Channel, Severity


@dataclass(frozen=True, slots=True)
class AudienceSelection:
    type: AudienceType
    group: str | None = None
    labels: tuple[tuple[str, str], ...] | None = None

    def __post_init__(self) -> None:
        audience_type = AudienceType(self.type)
        object.__setattr__(self, "type", audience_type)

        if audience_type is AudienceType.ALL:
            if self.group is not None or self.labels is not None:
                raise ValueError("all audience must not include group or labels")
            return

        if audience_type is AudienceType.GROUP:
            if self.group is None or self.labels is not None:
                raise ValueError("group audience requires group and no labels")
            _validate_exact_non_empty(self.group, "group")
            return

        if self.labels is None or self.group is not None:
            raise ValueError("labels audience requires labels and no group")
        _validate_labels(self.labels)


@dataclass(frozen=True, slots=True)
class NotificationCreationInput:
    message: str
    severity: Severity
    audience: AudienceSelection
    channels: tuple[Channel, ...]

    def __post_init__(self) -> None:
        _validate_exact_non_empty(self.message, "message")
        if not isinstance(self.severity, Severity):
            raise ValueError("severity must be a Severity")
        if not self.channels:
            raise ValueError("channels must not be empty")
        if any(not isinstance(channel, Channel) for channel in self.channels):
            raise ValueError("channels must contain only Channel values")
        if len(set(self.channels)) != len(self.channels):
            raise ValueError("channels must not contain duplicates")


def _validate_labels(labels: tuple[tuple[str, str], ...]) -> None:
    if not labels:
        raise ValueError("labels must not be empty")

    seen_keys: set[str] = set()
    for key, value in labels:
        _validate_exact_non_empty(key, "label key")
        _validate_exact_non_empty(value, "label value")
        if key in seen_keys:
            raise ValueError("duplicate label keys are not allowed")
        seen_keys.add(key)


def _validate_exact_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if not value or value != value.strip():
        raise ValueError(f"{field_name} must be exact and non-empty")
