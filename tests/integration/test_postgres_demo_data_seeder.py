import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.db.models import GroupModel, UserGroupModel, UserLabelModel, UserModel
from backend.seeding.demo_data import DemoDataSeeder

SessionFactory = sessionmaker[Session]


class PostgresDemoDataSeederFixtures:
    @staticmethod
    def seeder(session_factory: SessionFactory) -> DemoDataSeeder:
        return DemoDataSeeder(
            session_factory=session_factory,
            total_users=5000,
        )


@pytest.mark.integration
class TestPostgresDemoDataSeeder:
    def test_seed_creates_5000_demo_users_idempotently(
        self,
        postgres_session_factory: SessionFactory,
    ) -> None:
        seeder = PostgresDemoDataSeederFixtures.seeder(postgres_session_factory)

        first = seeder.run_once()
        second = seeder.run_once()

        assert first.requested_user_count == 5000
        assert first.created_user_count == 5000
        assert first.created_group_count == 3
        assert first.created_label_count == 10000
        assert first.created_membership_count == 5000
        assert second.created_user_count == 0
        assert second.created_label_count == 0
        assert second.created_membership_count == 0

        with postgres_session_factory() as session:
            assert session.scalar(select(func.count(UserModel.id))) == 5000
            assert session.scalar(select(func.count(GroupModel.id))) == 3
            assert session.scalar(select(func.count(UserLabelModel.id))) == 10000
            assert session.scalar(select(func.count(UserGroupModel.user_id))) == 5000
