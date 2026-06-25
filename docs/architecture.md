# Architecture Notes

This project is a demo implementation of a persistent notification broker for platform users. It
accepts notification requests through FastAPI, stores durable state in PostgreSQL, uses Redpanda
Kafka for asynchronous fanout signals, and delivers notifications through web and email workers.

## Design Goals

- Keep notification producers simple: message, severity, audience, channels, and idempotency key.
- Make PostgreSQL the source of truth for notification requests, recipients, deliveries, retries,
  and outbox state.
- Use Kafka as a processing signal bus, not as the only durable record of business state.
- Split API, outbox publishing, fanout, delivery, workload generation, and observability into
  separate runtime roles so each can scale independently later.
- Keep local development self-contained with Docker Compose while keeping the process boundaries
  compatible with Kubernetes deployment.

## Request Flow

1. A caller sends `POST /notifications` with a JWT bearer token.
2. FastAPI validates the Pydantic request schema and authentication scope.
3. The service computes an idempotency key from the explicit key when present, otherwise from a
   deterministic payload fingerprint.
4. The notification request and an outbox event are inserted in one PostgreSQL transaction.
5. The outbox publisher claims due outbox rows and publishes events to Redpanda Kafka.
6. The notification consumer handles the committed event, resolves the audience, and creates
   per-user delivery rows for the selected channels.
7. Channel-specific delivery workers claim due delivery rows from PostgreSQL and deliver them.
8. Web notifications are read through `GET /me/notifications` and can be marked read.

The important boundary is the database transaction before Kafka publish. The API does not publish
directly to Kafka after writing business rows because that would create a failure window where one
side commits and the other does not.

## Persistence Model

PostgreSQL stores the durable state:

- notification requests and idempotency keys;
- resolved recipients;
- per-channel delivery rows;
- web read state;
- action invocations;
- outbox and processed-event records.

Workers use row-level claiming and leases rather than in-memory ownership. If a worker dies while
processing, another worker can reclaim expired rows. This is the core reason the demo can scale
workers horizontally without making Kafka offsets the only recovery mechanism.

## Delivery Semantics

The implementation targets effectively-once behavior at the application level:

- duplicate notification requests are prevented by the idempotency key constraint;
- duplicate Kafka events are handled through processed-event tracking;
- duplicate delivery work is constrained by delivery row state transitions and leases;
- retry only selects eligible failed delivery rows and does not recreate recipients.

This is intentionally not described as true physical exactly-once delivery. Kafka, SMTP, process
crashes, and external providers can create ambiguous outcomes. For email, a production deployment
should prefer provider-side idempotency keys or deterministic provider message IDs.

## Retry And Replay

Retry is modeled as business replay, not blind Kafka topic rewind.

The retry endpoints find eligible `failed_retryable` delivery rows in PostgreSQL, mark them as
`replay_requested`, and insert a `notification.replay_requested` outbox event. Kafka wakes the
pipeline, but PostgreSQL decides exactly which delivery rows are eligible.

This avoids replaying unrelated historical Kafka messages and prevents retry from duplicating
already delivered rows.

## Scaling Model

The service roles are deliberately separate:

- API replicas scale for request volume.
- Outbox publishers scale cautiously because they claim and publish committed events.
- Notification consumers scale with Kafka partitions and processed-event deduplication.
- Web and email delivery workers scale independently based on channel backlog.
- PostgreSQL should be moved to managed or dedicated infrastructure before pushing fanout volume
  substantially.

The Grafana dashboards expose the main scaling signals: API RED metrics, container USE metrics,
Kafka offsets, outbox backlog, and waiting deliveries split by channel.

## Tradeoffs

- PostgreSQL-first persistence is slower than direct Kafka publish, but it makes API durability and
  idempotency explicit.
- The local demo uses mock users and labels instead of integrating with a real identity directory.
- The workload generator goes through REST to exercise the same path as internal services until a
  producer SDK exists.
- Worker polling is simple and visible for a demo, but production would usually add tighter
  autoscaling, alerting, and operational runbooks.
- Dashboards are provisioned from JSON so the demo is reproducible, even though hand-tuned Grafana
  dashboards would normally evolve through production usage.

## Production Hardening Gaps

- Real identity provider integration and key rotation for JWT validation.
- Secrets management instead of local Compose environment variables.
- Dead-letter topics, replay tooling, and operator approval flows.
- Provider-level email idempotency and bounce handling.
- Kubernetes manifests, autoscaling rules, pod disruption budgets, and readiness probes.
- Load tests against managed PostgreSQL and a multi-broker Kafka deployment.
