from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app_factory import BackendApplicationFactory
from backend.core.config import Settings
from backend.db.unit_of_work import SqlAlchemyUnitOfWork

SessionFactory = sessionmaker[Session]


class TestBackendApplicationFactory:
    def test_create_registers_injected_dependencies_and_routers(self) -> None:
        fixed_now = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)
        settings = Settings(
            database_url="sqlite+pysqlite:///:memory:",
            jwt_secret="test-secret-long-enough",
        )
        session_factory = sessionmaker[Session]()

        app = BackendApplicationFactory(
            settings=settings,
            session_factory=session_factory,
            now=lambda: fixed_now,
        ).create()

        unit_of_work_factory = cast(
            Callable[[], SqlAlchemyUnitOfWork],
            app.state.unit_of_work_factory,
        )
        route_paths = set(app.openapi()["paths"])

        assert app.title == "Notification Center"
        assert app.state.settings is settings
        assert app.state.session_factory is session_factory
        assert isinstance(unit_of_work_factory(), SqlAlchemyUnitOfWork)
        assert app.state.now() == fixed_now
        assert "/notifications" in route_paths
        assert "/me/notifications" in route_paths

    def test_from_env_loads_settings(self) -> None:
        factory = BackendApplicationFactory.from_env()

        assert isinstance(factory.settings, Settings)
    def test_health_endpoints_report_liveness_and_database_readiness(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        app = BackendApplicationFactory(
            settings=Settings(
                database_url="sqlite+pysqlite:///:memory:",
                jwt_secret="test-secret-long-enough",
            ),
            session_factory=session_factory,
        ).create()
        client = TestClient(app)

        assert client.get("/health/live").json() == {"status": "live"}
        readiness = client.get("/health/ready")
        assert readiness.status_code == 200
        assert readiness.json() == {"status": "ready"}
        engine.dispose()
