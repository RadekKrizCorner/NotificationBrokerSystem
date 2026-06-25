# NotificationBrokerSystem

Persistent notification center demo for platform users. The service accepts notifications over
FastAPI, persists requests and per-user deliveries in PostgreSQL, fans out work through Kafka, and
delivers through web and email channels.

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?logo=fastapi&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-2.x-E92063?logo=pydantic&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-000000?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?logo=grafana&logoColor=white)
![Mailpit](https://img.shields.io/badge/Mailpit-Local_Email-6B7280)
![Pytest](https://img.shields.io/badge/Pytest-Tests-0A9EDC?logo=pytest&logoColor=white)
![Ruff](https://img.shields.io/badge/Ruff-Lint-D7FF64)
![mypy](https://img.shields.io/badge/mypy-Strict_Types-2A6DB2)

## Run Locally

One command starts the full demo stack:

```bash
docker compose up --build
```

The stack runs migrations, seeds 5,000 demo users, starts the API, Redpanda Kafka, PostgreSQL,
Mailpit, Prometheus, Grafana, the outbox publisher, notification consumer, web delivery worker,
email delivery worker, and a REST workload generator.

Useful URLs:

- API docs: `http://localhost:8000/docs`
- Mailpit: `http://localhost:8025`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` with `admin` / `admin`

To reset all local state:

```bash
docker compose down -v
```

## Baseline Throughput

The default Compose profile is a small single-node setup: one API process, one outbox publisher,
one notification consumer, one web delivery worker, one email delivery worker, one PostgreSQL
container, and one Redpanda broker.

The local workload generator is configured for:

- `150` notification requests every `5 minutes`
- all requests sent through `POST /notifications`
- rotating channel selection: web, email, and web+email

In the current local demo data, that audience resolves to about 200 users. Because some requests
target both channels, the validated baseline is roughly:

- `150` notification requests / 5 minutes
- `~40,000` delivery records / 5 minutes
- `~130` delivery records / second

This is a local Docker Desktop baseline, not a production maximum. It is useful for checking that
the pipeline keeps up with near-real-time fanout and delivery on the smallest deployment shape.

## Scaling Options

The system is intentionally split by role so the bottleneck can be scaled independently:

- Scale web and email workers separately; the pipeline dashboard shows waiting deliveries by
  channel so the pressured delivery method is visible.
- Increase `NOTIFICATION_CENTER_DELIVERY_BATCH_SIZE` and reduce
  `NOTIFICATION_CENTER_DELIVERY_POLL_INTERVAL_SECONDS` when workers are DB-roundtrip bound.
- Scale API replicas behind a load balancer; idempotency is enforced in PostgreSQL.
- Scale outbox publisher and notification consumer cautiously with lease/consumer-group semantics.
- Increase Kafka partitions and worker replicas together when broker lag becomes the bottleneck.
- Move PostgreSQL to managed or dedicated infrastructure before increasing fanout volume
  substantially.

The code and process boundaries are ready for Kubernetes-style deployment even though the local demo
uses Docker Compose.

## Grafana

Grafana is provisioned automatically from `ops/grafana` and uses Prometheus as its data source.

Dashboards included:

- RED dashboard: request rate, error rate, P95 latency, P97 latency, and total API requests.
- USE dashboard: backend CPU and memory, container CPU for every Compose service, PostgreSQL
  connections and transaction rate, Kafka CPU, Kafka topics/partitions, and outbox backlog age.
- Pipeline dashboard: Kafka produced/processed offsets, unprocessed Kafka messages, outbox status,
  notification request status, delivery status, waiting email deliveries, and waiting web
  deliveries.

These dashboards are meant to answer the operational questions first: is the API healthy, where is
the queue/backlog, and which delivery channel needs more worker capacity?

## Development Checks

Unit tests run without external services:

```bash
.venv/bin/python -m pytest tests/unit -q
```

PostgreSQL and Redpanda integration tests use `docker-compose.integration.yml` when deeper local
validation is needed.
