from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.db.models import Base, GroupModel, UserGroupModel, UserLabelModel, UserModel
from backend.seeding.demo_data import DemoDataSeeder

SessionFactory = sessionmaker[Session]


@pytest.fixture()
def session_factory() -> Iterator[SessionFactory]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection: Any, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


class DemoDataSeederFixtures:
    @staticmethod
    def seeder(session_factory: SessionFactory, *, total_users: int = 12) -> DemoDataSeeder:
        return DemoDataSeeder(
            session_factory=session_factory,
            total_users=total_users,
        )


class TestDemoDataSeeder:
    def test_seed_creates_users_groups_labels_and_memberships(
        self,
        session_factory: SessionFactory,
    ) -> None:
        seeder = DemoDataSeederFixtures.seeder(session_factory, total_users=12)

        result = seeder.run_once()

        assert result.requested_user_count == 12
        assert result.created_user_count == 12
        assert result.created_group_count == 3
        assert result.created_label_count == 24
        assert result.created_membership_count == 12

        with session_factory() as session:
            assert session.scalar(select(func.count(UserModel.id))) == 12
            assert session.scalar(select(func.count(GroupModel.id))) == 3
            assert session.scalar(select(func.count(UserLabelModel.id))) == 24
            assert session.scalar(select(func.count(UserGroupModel.user_id))) == 12

    def test_seed_is_idempotent_when_rerun(
        self,
        session_factory: SessionFactory,
    ) -> None:
        seeder = DemoDataSeederFixtures.seeder(session_factory, total_users=12)

        first = seeder.run_once()
        second = seeder.run_once()

        assert first.created_user_count == 12
        assert second.created_user_count == 0
        assert second.created_group_count == 0
        assert second.created_label_count == 0
        assert second.created_membership_count == 0

        with session_factory() as session:
            assert session.scalar(select(func.count(UserModel.id))) == 12
            assert session.scalar(select(func.count(UserLabelModel.id))) == 24
            assert session.scalar(select(func.count(UserGroupModel.user_id))) == 12

    @pytest.mark.kwparametrize(
        [
            {
                "id": "first-user",
                "email": "demo-user-00001@example.test",
                "group_name": "Support",
                "labels": {"region": "EU", "tier": "free"},
            },
            {
                "id": "third-user",
                "email": "demo-user-00003@example.test",
                "group_name": "Billing",
                "labels": {"region": "APAC", "tier": "enterprise"},
            },
            {
                "id": "admin-user",
                "email": "demo-user-00025@example.test",
                "group_name": "Administrators",
                "labels": {"region": "EU", "tier": "free"},
            },
        ]
    )
    def test_seed_assigns_deterministic_profiles(
        self,
        session_factory: SessionFactory,
        email: str,
        group_name: str,
        labels: dict[str, str],
    ) -> None:
        seeder = DemoDataSeederFixtures.seeder(session_factory, total_users=25)

        seeder.run_once()

        with session_factory() as session:
            user = session.scalar(select(UserModel).where(UserModel.email == email))
            assert user is not None

            group = session.scalar(
                select(GroupModel)
                .join(UserGroupModel, UserGroupModel.group_id == GroupModel.id)
                .where(UserGroupModel.user_id == user.id)
            )
            assert group is not None
            assert group.name == group_name

            actual_labels = {
                key: value
                for key, value in session.execute(
                    select(UserLabelModel.key, UserLabelModel.value).where(
                        UserLabelModel.user_id == user.id
                    )
                )
            }
            assert actual_labels == labels
