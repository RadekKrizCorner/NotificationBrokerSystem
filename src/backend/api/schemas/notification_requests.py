from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictStr,
    StringConstraints,
    field_validator,
    model_validator,
)

from backend.domain.enums import AudienceType, Channel, Severity
from backend.domain.value_objects import AudienceSelection, NotificationCreationInput

GroupName = Annotated[StrictStr, StringConstraints(max_length=128)]
LabelKey = Annotated[StrictStr, StringConstraints(max_length=64)]
LabelValue = Annotated[StrictStr, StringConstraints(max_length=256)]


class AudienceSelector(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["all", "group", "labels"]
    group: GroupName | None = None
    labels: dict[LabelKey, LabelValue] | None = Field(default=None, max_length=20)

    @model_validator(mode="before")
    @classmethod
    def reject_non_exact_selector_shape(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data

        selector_type = data.get("type")
        fields = set(data)
        if selector_type == "all" and fields != {"type"}:
            raise ValueError("all audience accepts only type")
        if selector_type == "group" and fields != {"type", "group"}:
            raise ValueError("group audience requires exactly type and group")
        if selector_type == "labels" and fields != {"type", "labels"}:
            raise ValueError("labels audience requires exactly type and labels")
        return data

    @field_validator("group")
    @classmethod
    def reject_invalid_group(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value or value != value.strip():
            raise ValueError("group must be exact and non-empty")
        return value

    @field_validator("labels")
    @classmethod
    def reject_invalid_labels(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is None:
            return None

        for key, label_value in value.items():
            if not key or not label_value:
                raise ValueError("label keys and values must not be empty")
            if key != key.strip() or label_value != label_value.strip():
                raise ValueError("label keys and values must be exact")

        if not value:
            raise ValueError("labels must not be empty")
        return value

    @model_validator(mode="after")
    def validate_shape_for_type(self) -> Self:
        if self.type == "all":
            if self.group is not None or self.labels is not None:
                raise ValueError("all audience must not include group or labels")
            return self

        if self.type == "group":
            if self.group is None or self.labels is not None:
                raise ValueError("group audience requires group and no labels")
            return self

        if self.labels is None or self.group is not None:
            raise ValueError("labels audience requires labels and no group")
        return self


class CreateNotificationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    message: StrictStr = Field(min_length=1, max_length=2_000)
    severity: Severity
    audience: AudienceSelector
    channels: tuple[Channel, ...] = Field(min_length=1)
    idempotency_key: StrictStr | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    )

    @field_validator("message")
    @classmethod
    def reject_invalid_message(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("message must be exact")
        return value

    @field_validator("channels")
    @classmethod
    def reject_duplicate_channels(cls, value: tuple[Channel, ...]) -> tuple[Channel, ...]:
        if len(set(value)) != len(value):
            raise ValueError("channels must not contain duplicates")
        return value

    def to_domain(self) -> NotificationCreationInput:
        labels = tuple(self.audience.labels.items()) if self.audience.labels is not None else None
        return NotificationCreationInput(
            message=self.message,
            severity=self.severity,
            audience=AudienceSelection(
                type=AudienceType(self.audience.type),
                group=self.audience.group,
                labels=labels,
            ),
            channels=self.channels,
        )
