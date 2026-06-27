from collections.abc import Callable
from datetime import UTC, datetime

from fastapi import FastAPI
from sqlalchemy.orm import Session, sessionmaker

from backend.api.routers.notifications import router as notifications_router
from backend.api.routers.users import router as users_router
from backend.core.config import Settings
from backend.core.http_limits import RequestBodyLimitMiddleware
from backend.core.metrics import (
    PrometheusHttpMiddleware,
    PrometheusMetrics,
    PrometheusMetricsEndpoint,
)
from backend.db.session import make_engine, make_session_factory
from backend.db.unit_of_work import SqlAlchemyUnitOfWork
from backend.services.pipeline_metrics_service import PipelineMetricsRefresher


class BackendApplicationFactory:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session] | None = None,
        now: Callable[[], datetime] | None = None,
        metrics: PrometheusMetrics | None = None,
    ) -> None:
        self.settings = settings
        self._session_factory = session_factory
        self._now = now
        self._metrics = metrics

    @classmethod
    def from_env(cls) -> BackendApplicationFactory:
        return cls(settings=Settings())

    def create(self) -> FastAPI:
        app = FastAPI(title="Notification Center")
        app.add_middleware(
            RequestBodyLimitMiddleware,
            max_body_bytes=self.settings.max_request_body_bytes,
        )
        metrics = self._resolved_metrics()
        session_factory = self._resolved_session_factory()
        now = self._now or self._default_now
        self._register_state(
            app,
            metrics=metrics,
            session_factory=session_factory,
            now=now,
        )
        self._register_metrics(
            app,
            metrics=metrics,
            session_factory=session_factory,
            now=now,
        )
        self._register_routers(app)
        return app

    def _register_state(
        self,
        app: FastAPI,
        *,
        metrics: PrometheusMetrics,
        session_factory: sessionmaker[Session],
        now: Callable[[], datetime],
    ) -> None:
        app.state.settings = self.settings
        app.state.session_factory = session_factory
        app.state.unit_of_work_factory = lambda: SqlAlchemyUnitOfWork(session_factory)
        app.state.now = now
        app.state.metrics = metrics

    def _register_metrics(
        self,
        app: FastAPI,
        *,
        metrics: PrometheusMetrics,
        session_factory: sessionmaker[Session],
        now: Callable[[], datetime],
    ) -> None:
        app.add_middleware(PrometheusHttpMiddleware, metrics=metrics)
        pipeline_refresher = PipelineMetricsRefresher(
            session_factory=session_factory,
            metrics=metrics,
            now=now,
            refresh_interval_seconds=self.settings.metrics_refresh_interval_seconds,
        )
        endpoint = PrometheusMetricsEndpoint(
            metrics=metrics,
            before_render=pipeline_refresher.refresh,
        )
        app.add_api_route(
            "/metrics",
            endpoint.handle,
            methods=["GET"],
            include_in_schema=False,
        )

    def _register_routers(self, app: FastAPI) -> None:
        app.include_router(notifications_router)
        app.include_router(users_router)

    def _resolved_session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is not None:
            return self._session_factory
        engine = make_engine(self.settings.database_url)
        return make_session_factory(engine)

    def _resolved_metrics(self) -> PrometheusMetrics:
        return self._metrics or PrometheusMetrics()

    def _default_now(self) -> datetime:
        return datetime.now(UTC)
