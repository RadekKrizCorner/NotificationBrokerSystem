import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

SessionFactory = sessionmaker[Session]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_DATABASE_URL_ENV = "INTEGRATION_DATABASE_URL"
INTEGRATION_KAFKA_BOOTSTRAP_SERVERS_ENV = "INTEGRATION_KAFKA_BOOTSTRAP_SERVERS"
POSTGRES_STARTUP_TIMEOUT_SECONDS = 30

APP_TABLES = (
    "producer_quotas",
    "notification_action_invocations",
    "delivery_attempts",
    "notification_deliveries",
    "notification_recipients",
    "user_labels",
    "user_groups",
    "processed_events",
    "outbox_events",
    "notification_requests",
    "groups",
    "users",
)


@pytest.fixture(scope="session")
def integration_database_url() -> str:
    database_url = os.getenv(INTEGRATION_DATABASE_URL_ENV)
    if database_url is None or not database_url.strip():
        pytest.skip(f"{INTEGRATION_DATABASE_URL_ENV} is not set")
    return database_url


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> str:
    bootstrap_servers = os.getenv(INTEGRATION_KAFKA_BOOTSTRAP_SERVERS_ENV)
    if bootstrap_servers is None or not bootstrap_servers.strip():
        pytest.skip(f"{INTEGRATION_KAFKA_BOOTSTRAP_SERVERS_ENV} is not set")
    return bootstrap_servers


@pytest.fixture(scope="session")
def postgres_engine(integration_database_url: str) -> Iterator[Engine]:
    engine = create_engine(integration_database_url, pool_pre_ping=True)
    _wait_for_postgres(engine)
    _run_migrations(integration_database_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def postgres_session_factory(postgres_engine: Engine) -> Iterator[SessionFactory]:
    _truncate_app_tables(postgres_engine)
    factory = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        _truncate_app_tables(postgres_engine)


def _wait_for_postgres(engine: Engine) -> None:
    deadline = time.monotonic() + POSTGRES_STARTUP_TIMEOUT_SECONDS
    while True:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


def _run_migrations(database_url: str) -> None:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "src/migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _truncate_app_tables(engine: Engine) -> None:
    table_list = ", ".join(f'"{table_name}"' for table_name in APP_TABLES)
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE"))
