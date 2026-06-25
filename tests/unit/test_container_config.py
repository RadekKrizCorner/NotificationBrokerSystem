from pathlib import Path


class ContainerConfigFixtures:
    root = Path(__file__).resolve().parents[2]


class TestContainerConfig:
    def test_dockerfile_uses_shared_process_entrypoint(self) -> None:
        dockerfile = (ContainerConfigFixtures.root / "Dockerfile").read_text()

        assert 'CMD ["python", "-m", "backend.runtime", "api"]' in dockerfile
        assert "PYTHONPATH=/app/src" in dockerfile

    def test_github_actions_builds_container_image(self) -> None:
        workflow = (ContainerConfigFixtures.root / ".github/workflows/ci.yml").read_text()

        assert "Build Docker image" in workflow
        assert "docker build" in workflow

    def test_compose_defines_application_process_roles(self) -> None:
        compose_file = (ContainerConfigFixtures.root / "docker-compose.yml").read_text()

        assert "api:" in compose_file
        assert "seed-demo-data:" in compose_file
        assert "outbox-publisher:" in compose_file
        assert "notification-consumer:" in compose_file
        assert "web-delivery-worker:" in compose_file
        assert "email-delivery-worker:" in compose_file
        assert "  delivery-worker:" not in compose_file
        assert "workload-generator:" in compose_file
        assert 'command: ["python", "-m", "backend.runtime", "workload-generator"]' in compose_file

    def test_compose_tunes_local_delivery_and_workload_rates(self) -> None:
        compose_file = (ContainerConfigFixtures.root / "docker-compose.yml").read_text()

        assert "NOTIFICATION_CENTER_DEMO_SEED_USER_COUNT: 5000" in compose_file
        assert "NOTIFICATION_CENTER_DELIVERY_BATCH_SIZE: 500" in compose_file
        assert "NOTIFICATION_CENTER_DELIVERY_POLL_INTERVAL_SECONDS: 0.2" in compose_file
        assert "NOTIFICATION_CENTER_WORKLOAD_NOTIFICATIONS_PER_WINDOW: 150" in compose_file

    def test_redpanda_healthcheck_accepts_padded_rpk_health_output(self) -> None:
        config_files = [
            ContainerConfigFixtures.root / "docker-compose.yml",
            ContainerConfigFixtures.root / "docker-compose.integration.yml",
            ContainerConfigFixtures.root / ".github/workflows/ci.yml",
        ]

        for config_file in config_files:
            content = config_file.read_text()

            assert "rpk cluster health | grep -q 'Healthy:[[:space:]]*true'" in content
