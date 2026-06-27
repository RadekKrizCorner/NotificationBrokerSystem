# Notification Center Incremental Hardening Design

## Status

Approved on 2026-06-27.

## Goal

Harden the existing notification center for production-like operation without replacing its
FastAPI, PostgreSQL, Kafka, outbox, or worker architecture. Preserve the one-command local demo,
keep multiline notification messages, and add repository-backed Jinja2 email templates that render
both plain text and safely escaped HTML.

## Scope

This design covers every issue identified in the code review:

- secure local and production configuration modes;
- stronger JWT validation and bounded API inputs;
- producer quotas and fanout limits;
- conflict-aware, concurrency-safe notification idempotency;
- atomic retry state transitions;
- short claim and finalize transactions for outbox and delivery work;
- explicit Kafka event envelopes, event-type routing, and a dead-letter topic;
- isolated worker failures, structured logging, and bounded retry backoff;
- Jinja2 multipart email rendering with safe subjects and deterministic Message-IDs;
- bulk fanout, cached pipeline metrics, database constraints, and accurate API counts;
- non-root containers, loopback-only local ports, reproducible dependencies, and security checks.

The design does not replace local HS256 authentication with an external OIDC provider, introduce a
new queueing product, or convert the application to an async database stack. Those are separate
production-infrastructure decisions.

## Compatibility Requirements

- `docker compose up --build` continues to start the complete local demo without manual secret
  provisioning.
- Existing notification creation, web notification, mark-read, and retry endpoints remain.
- Existing valid request payloads remain valid, including messages with embedded newlines.
- Reusing an idempotency key for the same payload remains a successful deduplicated request.
- Reusing an idempotency key for a different payload becomes `409 Conflict` instead of silently
  returning unrelated work.
- Asynchronous first-accept responses continue to report zero counts when fanout has not run.
  Deduplicated responses report the persisted counts currently known for the original request.

## Runtime Architecture

The current process roles remain: API, outbox publisher, notification consumer, web delivery
worker, email delivery worker, workload generator, migrations, and demo seeder. Services continue
to use synchronous SQLAlchemy repositories and explicit unit-of-work boundaries.

Work that crosses PostgreSQL and an external system uses three phases:

1. **Claim:** a short transaction locks eligible rows, marks them `processing` or `publishing`,
   assigns a unique UUID claim token and lease expiry, and commits.
2. **External operation:** Kafka publish or channel delivery runs without an open database
   transaction.
3. **Finalize:** a new short transaction updates the row only when its status and claim token still
   match. A stale worker cannot overwrite a later claim or completed state.

Each delivery finalizes independently. One failure cannot roll back state transitions for other
deliveries in the batch. Lease recovery remains available when a process exits after claim but
before finalize.

Kafka and SMTP are inherently ambiguous if an external operation succeeds but the database
finalize fails. The system therefore remains explicitly at-least-once. Kafka consumers deduplicate
by event ID, and email uses a deterministic Message-ID derived from the delivery ID so providers
that deduplicate Message-IDs can suppress repeat sends.

## Notification Creation And Idempotency

Notification creation uses PostgreSQL as the concurrency arbiter rather than a read-then-insert
sequence. The repository attempts the insert and handles the unique identity conflict in a
savepoint or PostgreSQL upsert flow, then reads the winning row.

- Matching `payload_fingerprint` returns the existing notification and its persisted counts.
- A different fingerprint for the same explicit idempotency key raises a domain conflict mapped to
  HTTP 409.
- Fallback deduplication retains the existing deterministic bucket behavior but receives the same
  concurrency-safe conflict handling.
- The notification row and its initial outbox event remain in one transaction.

The API response result object carries `recipient_count` and `delivery_count`, avoiding hard-coded
zero counts for already processed deduplicated requests.

## Retry Concurrency

Retry eligibility is claimed with one conditional database statement. Only rows currently in
`failed_retryable` can transition to `replay_requested`, and PostgreSQL returns the rows actually
changed. Concurrent retry callers therefore cannot queue the same delivery twice.

The retry audit row and replay outbox event are written in the same transaction as the state
transition. A caller that loses the race records `no_eligible`; it does not create a replay event.
User-triggered retries retain the existing ownership and sibling-delivery restrictions.

## Fanout And Database Integrity

Audience selection stays functionally identical, but recipient and delivery creation uses bulk
inserts with conflict-ignore semantics instead of one flush per user. Existing uniqueness
constraints remain the final idempotency barrier.

An Alembic migration adds claim-token columns, supporting indexes, status/channel CHECK
constraints, and consistency protection for delivery identity. The migration validates existing
rows before enabling constraints and supports downgrade. Repository methods remain compatible with
SQLite unit tests where practical; PostgreSQL-specific concurrency behavior is covered by
integration tests.

A configurable maximum resolved-recipient count and maximum delivery count reject oversized
fanout before delivery rows are created. Defaults allow the existing 5,000-user demo.

## Kafka Event Contract And Dead Letters

Every published message uses a versioned envelope:

```json
{
  "event_id": "uuid",
  "event_type": "notification.requested",
  "version": 1,
  "occurred_at": "2026-06-27T12:00:00+00:00",
  "data": {}
}
```

The Kafka record key remains the aggregate or replay key for partition ordering; processed-event
deduplication uses `event_id` from the envelope. The consumer routes supported event types
explicitly:

- `notification.requested` invokes notification fanout;
- `notification.replay_requested` validates and acknowledges the replay signal without rerunning
  fanout, because PostgreSQL delivery state already drives replay work.

Invalid JSON, invalid envelopes, unsupported versions, and unsupported event types are published to
`notifications.requests.dlq` with the original key, a size-bounded encoded payload, source topic,
and sanitized reason. The source offset commits only after the DLQ publish succeeds. If DLQ publish
fails, the offset remains uncommitted and the message is retried.

## Worker Failure Isolation

Expected adapter outcomes continue to map to delivered, retryable, or terminal states. Unexpected
per-delivery exceptions are converted to a bounded error outcome and finalized for that delivery;
they do not leave the whole batch uncommitted. Template configuration errors are terminal, while
transient SMTP/network errors are retryable.

The polling runtime catches infrastructure failures at the cycle boundary, emits structured logs,
and applies capped exponential backoff before retrying. Successful cycles reset the backoff.
Invalid startup configuration fails fast. Stored error codes and messages are sanitized and length
limited.

Compose adds restart policies and health checks appropriate to each long-running process. Worker
metrics and logs include role, worker ID, event or delivery ID, final classification, and duration
without logging bearer tokens, full message bodies, or secrets.

## Authentication And Authorization

Settings expose explicit `local` and `production` modes.

- Local mode preserves the self-contained demo secret but binds all host-published services to
  `127.0.0.1` and clearly labels the credentials as development-only.
- Production mode refuses the demo secret and unsafe/default credentials.
- The configured JWT algorithm is restricted to the supported algorithm.
- Tokens require `sub`, `type`, `exp`, `iat`, `iss`, and `aud`; expiration, issuer, and audience are
  verified.
- The workload generator issues short-lived local tokens with matching claims.
- Subject and scope shapes are length and count bounded.

Authorization rules do not change: service write scope creates notifications; owner service or
retry-any scope retries service notifications; user read scope accesses only the authenticated
user's visible web deliveries.

## Input And Resource Bounds

The API enforces a configurable request-body size before JSON processing. Request schemas bound
group length, label count, label key/value length, and cursor length while preserving existing valid
payloads. Channel uniqueness and message length validation remain.

A PostgreSQL-backed fixed-window producer quota uses an atomic upsert keyed by source service and
window start, so limits remain consistent across API replicas. The default local quota permits the
existing workload generator. Exceeding the quota returns HTTP 429 with a retry hint.

Fanout recipient and delivery limits provide a second guard after authorization. These limits are
configuration values with safe production defaults and local-demo-compatible defaults.

## Email Templating

Email composition is split into a focused `EmailTemplateRenderer` and the SMTP adapter. Jinja2 uses
`StrictUndefined` so incomplete contexts fail explicitly.

The repository includes versioned default templates for:

- subject;
- plain-text body;
- HTML body.

Production can configure an alternative template directory. The default templates are packaged in
the wheel and copied into the Docker image. Template syntax and required template names are
validated when the email worker starts.

The render context is intentionally small:

- notification message and severity;
- recipient display name and email;
- notification ID and delivery ID;
- notification creation and delivery-attempt timestamps.

HTML rendering has mandatory autoescaping. Plain text preserves multiline content. The rendered
subject is stripped of CR/LF, whitespace-normalized, and length-limited even for custom templates.
The SMTP message is multipart/alternative and uses a deterministic RFC-compatible Message-ID based
on the delivery ID and configured message-ID domain.

Rendering failures become terminal `template_render_error` delivery outcomes. SMTP 4xx and network
failures remain retryable; SMTP 5xx failures remain terminal. No exception from one message may
terminate the worker batch.

## Metrics And Observability

Pipeline aggregation queries execute behind a thread-safe, time-based cache. A scrape inside the
cache interval renders the previous snapshot without rescanning business tables. Metrics are
served only on the internal Compose network in production; the local API remains reachable through
loopback for development.

New counters cover idempotency conflicts, rate-limit rejections, DLQ writes, stale finalize
attempts, template failures, retry outcomes, and worker cycle failures. Label values remain bounded
enums to avoid cardinality growth.

## Container And Supply-Chain Hardening

- Application containers run as a dedicated non-root user and drop unnecessary capabilities.
- Host ports bind to loopback in the local Compose profile. PostgreSQL, Redpanda, Mailpit,
  exporters, and cAdvisor are not remotely exposed by default.
- Production configuration does not expose infrastructure ports or use demo credentials.
- Direct and transitive Python dependencies are locked; Docker and CI use the frozen lock.
- CI runs unit and integration tests, Ruff, strict mypy, migration checks, dependency audit,
  container build, and a container vulnerability scan.
- GitHub Actions and container images use pinned immutable versions where the tooling supports it.

## Testing Strategy

Implementation follows red-green-refactor. Each behavior starts with a focused failing test.

Unit tests cover:

- settings mode validation and JWT claim enforcement;
- request bounds and quota responses;
- idempotency conflict mapping and persisted counts;
- retry state-transition results;
- claim/finalize ownership and stale tokens;
- event-envelope validation and routing;
- DLQ decision behavior;
- worker backoff and per-item error isolation;
- Jinja2 plain/HTML rendering, autoescaping, multiline content, custom templates, subject
  sanitization, deterministic Message-ID, and poison inputs;
- metrics cache behavior.

PostgreSQL integration tests use real concurrent sessions to cover notification creation, retry,
outbox claim/finalize, delivery claim/finalize, bulk fanout, quota increments, and schema
constraints. Redpanda integration tests cover event envelopes, duplicate delivery, supported event
routing, malformed-event DLQ publication, and offset behavior. Migration tests cover upgrade and
downgrade on supported databases.

The completion gate is:

1. all unit and integration tests pass;
2. Ruff and strict mypy pass;
3. Alembic upgrade and downgrade checks pass;
4. dependency and container scans have no unreviewed high or critical findings;
5. Docker image builds as non-root;
6. the full Compose stack starts healthy;
7. a smoke notification produces visible web and templated email deliveries;
8. the worktree contains no unintended changes.

## Delivery Order

The implementation proceeds in independently testable slices:

1. secure settings and JWT claims;
2. bounded requests, quotas, and fanout limits;
3. conflict-aware notification idempotency and accurate counts;
4. atomic retry;
5. claim/finalize repository APIs and worker transaction boundaries;
6. Kafka envelope, routing, and DLQ;
7. Jinja2 multipart email rendering;
8. bulk fanout, metrics cache, and database constraints;
9. Docker, Compose, dependency lock, CI, documentation, and end-to-end verification.
