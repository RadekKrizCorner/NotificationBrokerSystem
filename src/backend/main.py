from collections.abc import Callable
from datetime import datetime

from fastapi import FastAPI
from sqlalchemy.orm import Session, sessionmaker

from backend.app_factory import BackendApplicationFactory
from backend.core.config import Settings
from backend.core.metrics import PrometheusMetrics


def create_app(
    *,
    settings: Settings | None = None,
    session_factory: sessionmaker[Session] | None = None,
    now: Callable[[], datetime] | None = None,
    metrics: PrometheusMetrics | None = None,
) -> FastAPI:
    return BackendApplicationFactory(
        settings=settings or Settings(),
        session_factory=session_factory,
        now=now,
        metrics=metrics,
    ).create()


app = BackendApplicationFactory.from_env().create()
