from uuid import UUID

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from backend.db.models import GroupModel, UserGroupModel, UserLabelModel, UserModel


class UserRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_active_user_ids(self) -> list[UUID]:
        active_user = UserModel.active.is_(True)

        statement = select(UserModel.id).where(active_user).order_by(UserModel.id)
        return list(self._session.scalars(statement))

    def list_active_user_ids_by_group(self, group_name: str) -> list[UUID]:
        active_user = UserModel.active.is_(True)
        matching_group = GroupModel.name == group_name

        statement = (
            select(UserModel.id)
            .join(UserGroupModel, UserGroupModel.user_id == UserModel.id)
            .join(GroupModel, GroupModel.id == UserGroupModel.group_id)
            .where(active_user, matching_group)
            .order_by(UserModel.id)
        )
        return list(self._session.scalars(statement))

    def list_active_user_ids_by_labels(self, labels: tuple[tuple[str, str], ...]) -> list[UUID]:
        if not labels:
            return []

        matching_label_conditions = [
            and_(UserLabelModel.key == key, UserLabelModel.value == value) for key, value in labels
        ]
        active_user = UserModel.active.is_(True)
        matches_requested_label = or_(*matching_label_conditions)

        statement = (
            select(UserModel.id)
            .join(UserLabelModel, UserLabelModel.user_id == UserModel.id)
            .where(active_user, matches_requested_label)
            .group_by(UserModel.id)
            .having(func.count(func.distinct(UserLabelModel.key)) == len(labels))
            .order_by(UserModel.id)
        )
        return list(self._session.scalars(statement))
