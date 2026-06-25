from collections.abc import Mapping
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from backend.core.config import Settings
from backend.db.models import GroupModel, UserGroupModel, UserLabelModel, UserModel
from backend.db.session import make_engine, make_session_factory


@dataclass(frozen=True, slots=True)
class DemoSeedResult:
    requested_user_count: int
    created_user_count: int
    created_group_count: int
    created_label_count: int
    created_membership_count: int


@dataclass(frozen=True, slots=True)
class DemoUserProfile:
    sequence: int
    email: str
    display_name: str
    group_name: str
    labels: Mapping[str, str]


class DemoUserProfileFactory:
    regions = ("EU", "US", "APAC")
    tiers = ("free", "pro", "enterprise")

    def create(self, sequence: int) -> DemoUserProfile:
        return DemoUserProfile(
            sequence=sequence,
            email=f"demo-user-{sequence:05d}@example.test",
            display_name=f"Demo User {sequence:05d}",
            group_name=self._group_name(sequence),
            labels={
                "region": self.regions[(sequence - 1) % len(self.regions)],
                "tier": self.tiers[(sequence - 1) % len(self.tiers)],
            },
        )

    def _group_name(self, sequence: int) -> str:
        if sequence % 25 == 0:
            return "Administrators"
        if sequence % 3 == 0:
            return "Billing"
        return "Support"


class DemoDataSeeder:
    group_names = ("Administrators", "Support", "Billing")
    demo_email_pattern = "demo-user-%@example.test"

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        total_users: int,
        profile_factory: DemoUserProfileFactory | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._total_users = total_users
        self._profile_factory = profile_factory or DemoUserProfileFactory()

    def run_once(self) -> DemoSeedResult:
        return self.seed()

    def seed(self) -> DemoSeedResult:
        profiles = [
            self._profile_factory.create(sequence)
            for sequence in range(1, self._total_users + 1)
        ]
        target_emails = {profile.email for profile in profiles}

        with self._session_factory() as session:
            groups_by_name, created_group_count = self._ensure_groups(session)
            users_by_email, created_user_count = self._ensure_users(
                session,
                profiles=profiles,
                target_emails=target_emails,
            )
            session.flush()
            created_label_count = self._ensure_labels(
                session,
                profiles=profiles,
                users_by_email=users_by_email,
            )
            created_membership_count = self._ensure_memberships(
                session,
                profiles=profiles,
                users_by_email=users_by_email,
                groups_by_name=groups_by_name,
            )
            session.commit()

        return DemoSeedResult(
            requested_user_count=self._total_users,
            created_user_count=created_user_count,
            created_group_count=created_group_count,
            created_label_count=created_label_count,
            created_membership_count=created_membership_count,
        )

    def _ensure_groups(self, session: Session) -> tuple[dict[str, GroupModel], int]:
        existing_groups = {
            group.name: group
            for group in session.scalars(
                select(GroupModel).where(GroupModel.name.in_(self.group_names))
            )
        }
        created_count = 0
        for group_name in self.group_names:
            if group_name in existing_groups:
                continue
            group = GroupModel(name=group_name)
            session.add(group)
            existing_groups[group_name] = group
            created_count += 1
        session.flush()
        return existing_groups, created_count

    def _ensure_users(
        self,
        session: Session,
        *,
        profiles: list[DemoUserProfile],
        target_emails: set[str],
    ) -> tuple[dict[str, UserModel], int]:
        users_by_email = {
            user.email: user
            for user in session.scalars(
                select(UserModel).where(UserModel.email.like(self.demo_email_pattern))
            )
            if user.email in target_emails
        }
        created_count = 0
        for profile in profiles:
            user = users_by_email.get(profile.email)
            if user is None:
                user = UserModel(
                    email=profile.email,
                    display_name=profile.display_name,
                    active=True,
                )
                session.add(user)
                users_by_email[profile.email] = user
                created_count += 1
                continue

            user.display_name = profile.display_name
            user.active = True
        session.flush()
        return users_by_email, created_count

    def _ensure_labels(
        self,
        session: Session,
        *,
        profiles: list[DemoUserProfile],
        users_by_email: Mapping[str, UserModel],
    ) -> int:
        labels_by_user_key = {
            (label.user_id, label.key): label
            for label in session.scalars(
                select(UserLabelModel)
                .join(UserModel, UserModel.id == UserLabelModel.user_id)
                .where(UserModel.email.like(self.demo_email_pattern))
            )
        }
        created_count = 0
        for profile in profiles:
            user = users_by_email[profile.email]
            for key, value in profile.labels.items():
                existing_label = labels_by_user_key.get((user.id, key))
                if existing_label is None:
                    label = UserLabelModel(user_id=user.id, key=key, value=value)
                    session.add(label)
                    labels_by_user_key[(user.id, key)] = label
                    created_count += 1
                    continue
                existing_label.value = value
        return created_count

    def _ensure_memberships(
        self,
        session: Session,
        *,
        profiles: list[DemoUserProfile],
        users_by_email: Mapping[str, UserModel],
        groups_by_name: Mapping[str, GroupModel],
    ) -> int:
        memberships = {
            (user_id, group_id)
            for user_id, group_id in session.execute(
                select(UserGroupModel.user_id, UserGroupModel.group_id)
                .join(UserModel, UserModel.id == UserGroupModel.user_id)
                .where(UserModel.email.like(self.demo_email_pattern))
            )
        }
        created_count = 0
        for profile in profiles:
            user = users_by_email[profile.email]
            group = groups_by_name[profile.group_name]
            membership = (user.id, group.id)
            if membership in memberships:
                continue
            session.add(UserGroupModel(user_id=user.id, group_id=group.id))
            memberships.add(membership)
            created_count += 1
        return created_count


class DemoDataSeederFactory:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session] | None = None,
    ) -> None:
        self.settings = settings
        self._session_factory = session_factory

    def create_demo_data_seeder(self) -> DemoDataSeeder:
        return DemoDataSeeder(
            session_factory=self._resolved_session_factory(),
            total_users=self.settings.demo_seed_user_count,
        )

    def _resolved_session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is not None:
            return self._session_factory
        engine = make_engine(self.settings.database_url)
        return make_session_factory(engine)
