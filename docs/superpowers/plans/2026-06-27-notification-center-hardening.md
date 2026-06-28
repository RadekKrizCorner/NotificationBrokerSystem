# Notification Center Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the existing notification center without replacing its architecture, preserve the one-command local demo, and add safe Jinja2 multipart email templates.

**Architecture:** Keep FastAPI, synchronous SQLAlchemy, PostgreSQL, Kafka, and current worker roles. Make database transitions atomic, split external side effects from claim/finalize transactions, validate versioned events, isolate per-item failures, and separate safe local defaults from strict production settings.

**Tech Stack:** Python 3.14, FastAPI, Pydantic 2, SQLAlchemy 2, PostgreSQL 18, Alembic, aiokafka/Redpanda, Jinja2, PyJWT, pytest, Ruff, mypy, Docker Compose.

---

### Task 1: Secure settings, JWT claims, and bounded HTTP input

**Files:**
- Modify: `src/backend/core/config.py`, `src/backend/core/auth.py`, `src/backend/app_factory.py`
- Create: `src/backend/core/http_limits.py`
- Modify: `src/backend/api/schemas/notification_requests.py`, `src/backend/api/routers/users.py`
- Modify: `src/workers/workload/generator.py`
- Test: `tests/unit/test_settings.py`, `tests/unit/test_notifications_api.py`, `tests/unit/test_notification_request_validation.py`, `tests/unit/test_workload_generator.py`

- [ ] Write failing tests proving production rejects the demo secret, JWT requires `exp`/`iat`/`iss`/`aud`, the workload token expires, oversized labels fail validation, and an oversized body returns 413.
- [ ] Run the four focused test files and confirm failures are caused by missing behavior.
- [ ] Add `runtime_mode`, a restricted HS256 algorithm, issuer/audience/TTL settings, production validation, required claim decoding, and expiring workload tokens.
- [ ] Add schema bounds for group, labels, cursor, subject, and scopes plus an ASGI request-size limiter.
- [ ] Run focused tests, Ruff, and mypy; all must pass.
- [ ] Commit with `fix: harden authentication and request bounds`.

Expected settings shape:

```python
class Settings(BaseSettings):
    runtime_mode: Literal["local", "production", "test"] = "local"
    jwt_algorithm: Literal["HS256"] = "HS256"
    jwt_issuer: str = "notification-center"
    jwt_audience: str = "notification-center-api"
    jwt_token_ttl_seconds: int = Field(default=300, gt=0, le=3600)
```

### Task 2: Producer quotas and fanout limits

**Files:**
- Create: `src/backend/db/models/quotas.py`, `src/backend/db/repositories/quotas.py`, `src/backend/services/quota_service.py`
- Modify: model/repository exports, `src/backend/db/unit_of_work.py`, notification/fanout services, and API dependencies
- Create: `src/migrations/versions/0002_hardening.py`
- Test: `tests/unit/test_quota_service.py`, `tests/unit/test_notification_fanout_service.py`, `tests/integration/test_postgres_quota_repository.py`

- [ ] Write a failing test where the second request exceeds a limit of one and a failing fanout test that exceeds configured recipient/delivery limits.
- [ ] Run both unit tests and confirm missing quota/limit behavior.
- [ ] Add a composite-key producer quota model and PostgreSQL `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` counter with a SQLite fallback.
- [ ] Map `ProducerQuotaExceeded` to HTTP 429 with `Retry-After`.
- [ ] Guard resolved recipient and delivery counts before inserts; defaults must allow 5,000 demo users.
- [ ] Run unit and PostgreSQL integration tests and commit with `feat: enforce producer and fanout quotas`.

### Task 3: Conflict-aware notification idempotency

**Files:**
- Create: `src/backend/domain/errors.py`
- Modify: `src/backend/domain/results.py`, notification repository/service/router
- Test: notification creation/API unit tests and `tests/integration/test_postgres_notification_idempotency.py`

- [ ] Write a failing test proving the same key with a different payload raises `IdempotencyConflict` and deduplicated results expose persisted counts.
- [ ] Run focused unit tests and observe the current silent acceptance and missing counts.
- [ ] Add counts to `NotificationCreateResult`, compare fingerprints on every identity match, and map the conflict to HTTP 409.
- [ ] Wrap insert/flush in a savepoint; after unique collision, load the winning row and apply the same comparison.
- [ ] Add two-thread PostgreSQL tests for same and different payloads; assert one row and one initial outbox event.
- [ ] Run focused tests and commit with `fix: make notification idempotency conflict aware`.

### Task 4: Atomic retry transitions

**Files:**
- Modify: notification repository and retry service
- Test: retry/API unit tests
- Create: `tests/integration/test_postgres_retry_concurrency.py`

- [ ] Write a two-thread failing test asserting concurrent retry results replay counts `[0, 1]` and creates one replay event.
- [ ] Run it and confirm current duplicate replay behavior.
- [ ] Replace read-then-mutate with conditional `UPDATE ... WHERE status='failed_retryable' RETURNING` scoped by authorized delivery IDs.
- [ ] Build replay audit and outbox event only from returned rows.
- [ ] Run unit/API/integration tests and commit with `fix: make delivery replay transitions atomic`.

### Task 5: Short claim/finalize transactions and worker isolation

**Files:**
- Modify: notification/outbox models and repositories, delivery/outbox workers, runtime, migration 0002
- Test: worker, outbox, runtime unit tests and PostgreSQL claim integration tests

- [ ] Write failing tests proving claimed state is committed before adapter/publisher I/O and stale claim tokens cannot finalize.
- [ ] Add nullable UUID `claim_token` columns and conditional finalize repository methods.
- [ ] Return immutable claimed DTOs, commit claims immediately, perform external I/O without a write transaction, and finalize each item separately.
- [ ] Convert unexpected per-delivery exceptions to bounded retryable outcomes without rolling back the batch.
- [ ] Write a failing polling test for bounded exponential error backoff and reset after success; implement it with structured logging.
- [ ] Run the worker/runtime unit and PostgreSQL integration matrix and commit with `fix: isolate worker claims and side effects`.

### Task 6: Versioned Kafka envelopes and dead-letter routing

**Files:**
- Create: `src/backend/domain/events.py`
- Modify: Kafka publisher/consumer, notification consumer/handler, factories, notification/retry services
- Test: Kafka and consumer unit tests plus Redpanda integration tests

- [ ] Write failing tests for envelope validation, real event-ID deduplication, explicit event-type routing, replay not invoking fanout, and invalid-message DLQ behavior.
- [ ] Add frozen version-1 event envelope fields: `event_id`, `event_type`, `version`, `occurred_at`, and `data`.
- [ ] Publish the complete envelope using the outbox row ID as event ID; keep Kafka key for partition ordering only.
- [ ] Route requested events to fanout and acknowledge replay events without fanout.
- [ ] On permanent decode/schema/type failure, publish a bounded sanitized record to `notifications.requests.dlq`; commit source offset only after DLQ success.
- [ ] Run unit/Redpanda tests and commit with `feat: validate Kafka events and route dead letters`.

### Task 7: Jinja2 multipart email templates

**Files:**
- Create: `src/workers/delivery/email_templates.py`
- Create: `src/workers/delivery/templates/v1/notification.subject.j2`, `.txt.j2`, `.html.j2`
- Modify: email adapter, worker factory, settings, and package-data configuration
- Test: `tests/unit/test_email_templates.py`, `tests/unit/test_email_delivery.py`

- [ ] Write failing tests for multiline plain text, escaped HTML, subject CR/LF removal and length, custom template directory, StrictUndefined, and deterministic Message-ID.
- [ ] Add Jinja2 dependency and implement `EmailTemplateRenderer` with `StrictUndefined`, HTML autoescape, package defaults, and startup validation.
- [ ] Sanitize rendered subject independently while preserving multiline message bodies.
- [ ] Build multipart/alternative messages and `<delivery-{id}@{domain}>` Message-ID.
- [ ] Convert rendering errors to terminal `template_render_error`; retain SMTP 4xx/network retryable and SMTP 5xx terminal classification.
- [ ] Build a wheel and assert all templates are packaged; run email/worker tests and commit with `feat: render safe multipart notification emails`.

### Task 8: Metrics cache, bulk fanout, and schema constraints

**Files:**
- Modify: pipeline metrics service, fanout service, notification repository/models, migration 0002
- Test: metrics, fanout, model, migration unit tests and `tests/integration/test_postgres_hardening_constraints.py`

- [ ] Write a failing test proving two refreshes inside TTL execute one query set and a fanout test proving flush count is bounded.
- [ ] Add a lock-protected monotonic TTL cache around pipeline aggregation refresh.
- [ ] Add PostgreSQL bulk recipient/delivery insert methods with conflict-ignore semantics and a portable SQLite test fallback.
- [ ] Recount persisted recipients and deliveries after bulk insert.
- [ ] Add validated CHECK constraints for enums/counts and consistency constraints/indexes for delivery identity and claims.
- [ ] Run metrics/fanout/model/migration/PostgreSQL tests and commit with `perf: harden fanout persistence and metrics`.

### Task 9: Docker, Compose, supply chain, CI, and docs

**Files:**
- Modify: `Dockerfile`, Compose files, `.env.example`, `pyproject.toml`, CI workflow, README, architecture docs
- Create: `requirements.lock`
- Test: container and local observability configuration tests

- [ ] Write failing tests requiring a non-root Docker user and loopback-only published ports.
- [ ] Add non-root runtime ownership, dropped capabilities, restart policies, health checks, and `127.0.0.1` host bindings while preserving one-command local startup.
- [ ] Add explicit local JWT issuer/audience values and production-mode documentation.
- [ ] Generate hash-pinned dependencies; install frozen dependencies in Docker/CI and install the project with `--no-deps`.
- [ ] Add CI migration upgrade/downgrade, dependency audit, container scan, and immutable third-party pins.
- [ ] Document quotas, JWT claims, event/DLQ contract, at-least-once behavior, custom email templates, metrics cache, and rollout/rollback.
- [ ] Run full unit/integration/static/build/config checks and commit with `chore: harden runtime and supply chain`.

### Task 10: End-to-end verification and handoff

**Files:** Modify only files required by defects proven by final checks.

- [ ] Start isolated PostgreSQL and Redpanda integration services and run all integration tests.
- [ ] Run all unit tests, Ruff, strict mypy, pip check/audit, migration upgrade/downgrade, Docker build, and `docker compose config --quiet`.
- [ ] Start the isolated full stack; submit a multiline notification and verify one visible web delivery plus one Mailpit multipart email with escaped HTML.
- [ ] Stop only the isolated stack, inspect `git diff --check`, status, and commit history, and map each spec requirement to a test/change.
- [ ] Commit only corrections tied to a reproduced verification failure using `fix: address hardening verification findings`.
