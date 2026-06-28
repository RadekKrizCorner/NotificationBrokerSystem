import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class LocalObservabilityConfigFixtures:
    compose_path = PROJECT_ROOT / "docker-compose.yml"
    env_example_path = PROJECT_ROOT / ".env.example"
    prometheus_path = PROJECT_ROOT / "ops/prometheus/prometheus.yml"
    grafana_datasource_path = PROJECT_ROOT / "ops/grafana/provisioning/datasources/prometheus.yml"
    grafana_dashboard_provider_path = (
        PROJECT_ROOT / "ops/grafana/provisioning/dashboards/dashboards.yml"
    )
    red_dashboard_path = PROJECT_ROOT / "ops/grafana/dashboards/notification-center-red.json"
    use_dashboard_path = PROJECT_ROOT / "ops/grafana/dashboards/notification-center-use.json"
    pipeline_dashboard_path = (
        PROJECT_ROOT / "ops/grafana/dashboards/notification-center-pipeline.json"
    )


class TestLocalObservabilityConfig:
    def test_local_compose_defines_required_infrastructure_services(self) -> None:
        compose = LocalObservabilityConfigFixtures.compose_path.read_text()

        for service_name in (
            "postgres",
            "postgres-exporter",
            "cadvisor",
            "redpanda",
            "mailpit",
            "prometheus",
            "grafana",
        ):
            assert f"  {service_name}:" in compose

    def test_env_example_documents_runtime_settings(self) -> None:
        env_example = LocalObservabilityConfigFixtures.env_example_path.read_text()

        for variable_name in (
            "NOTIFICATION_CENTER_DATABASE_URL",
            "NOTIFICATION_CENTER_KAFKA_BOOTSTRAP_SERVERS",
            "NOTIFICATION_CENTER_JWT_SECRET",
            "NOTIFICATION_CENTER_OUTBOX_PUBLISH_BATCH_SIZE",
        ):
            assert variable_name in env_example

    def test_prometheus_scrapes_local_runtime_targets(self) -> None:
        prometheus_config = LocalObservabilityConfigFixtures.prometheus_path.read_text()

        for job_name in (
            "prometheus",
            "grafana",
            "redpanda",
            "postgres-exporter",
            "cadvisor",
            "notification-api",
        ):
            assert f"job_name: {job_name}" in prometheus_config

    def test_grafana_provisions_prometheus_and_dashboards(self) -> None:
        datasource_config = LocalObservabilityConfigFixtures.grafana_datasource_path.read_text()
        dashboard_provider = (
            LocalObservabilityConfigFixtures.grafana_dashboard_provider_path.read_text()
        )

        assert "uid: prometheus" in datasource_config
        assert "url: http://prometheus:9090" in datasource_config
        assert "/var/lib/grafana/dashboards" in dashboard_provider

    def test_grafana_includes_red_and_use_dashboards(self) -> None:
        red_dashboard = json.loads(LocalObservabilityConfigFixtures.red_dashboard_path.read_text())
        use_dashboard = json.loads(LocalObservabilityConfigFixtures.use_dashboard_path.read_text())

        assert red_dashboard["title"] == "Notification Center RED"
        assert use_dashboard["title"] == "Notification Center USE"
        assert {
            "Request Rate",
            "Error Rate",
            "P95 Latency",
            "P97 Latency",
            "Total Requests",
        }.issubset({panel["title"] for panel in red_dashboard["panels"]})
        assert {
            "Backend CPU Usage",
            "Container CPU Usage",
            "Backend Memory Usage",
            "Database Connections",
            "Database Transaction Rate",
            "Kafka CPU Usage",
            "Kafka Topics And Partitions",
            "Outbox Backlog Age",
        }.issubset({panel["title"] for panel in use_dashboard["panels"]})

    def test_grafana_includes_pipeline_and_kafka_dashboard(self) -> None:
        pipeline_dashboard = json.loads(
            LocalObservabilityConfigFixtures.pipeline_dashboard_path.read_text()
        )

        panel_titles = {panel["title"] for panel in pipeline_dashboard["panels"]}
        assert pipeline_dashboard["title"] == "Notification Center Pipeline"
        assert {
            "Kafka Produced Offset",
            "Kafka Processed Offset",
            "Kafka Unprocessed Messages",
            "Outbox Events By Status",
            "Deliveries By Status",
            "Waiting Email Deliveries",
            "Waiting Web Deliveries",
        }.issubset(panel_titles)
