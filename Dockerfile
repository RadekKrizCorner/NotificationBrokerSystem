FROM python:3.14.3-slim@sha256:5e59aae31ff0e87511226be8e2b94d78c58f05216efda3b07dbbed938ec8583b AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

RUN python -m pip install --no-cache-dir uv==0.10.6

COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY src ./src

RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14.3-slim@sha256:5e59aae31ff0e87511226be8e2b94d78c58f05216efda3b07dbbed938ec8583b AS runtime

ENV PATH=/app/.venv/bin:$PATH
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

RUN groupadd --system --gid 10001 notification \
    && useradd --system --uid 10001 --gid notification --home-dir /nonexistent notification

COPY --from=builder --chown=notification:notification /app/.venv /app/.venv
COPY --chown=notification:notification alembic.ini ./
COPY --chown=notification:notification src ./src

USER notification:notification

CMD ["python", "-m", "backend.runtime", "api"]
