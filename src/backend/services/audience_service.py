from uuid import UUID

from backend.db.repositories import UserRepository
from backend.domain.enums import AudienceType
from backend.domain.value_objects import AudienceSelection


class AudienceResolutionService:
    def __init__(self, users: UserRepository) -> None:
        self._users = users

    def resolve(self, audience: AudienceSelection) -> list[UUID]:
        if audience.type is AudienceType.ALL:
            return self._users.list_active_user_ids()
        if audience.type is AudienceType.GROUP:
            if audience.group is None:
                raise ValueError("group audience requires group")
            return self._users.list_active_user_ids_by_group(audience.group)
        if audience.labels is None:
            raise ValueError("labels audience requires labels")
        return self._users.list_active_user_ids_by_labels(audience.labels)
