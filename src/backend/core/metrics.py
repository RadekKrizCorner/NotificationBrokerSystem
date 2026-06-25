import resource
import sys
from collections.abc import Callable
from time import perf_counter, process_time
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class PrometheusMetrics:
    def __init__(self, *, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        self._http_requests_total: Any = Counter(
            "notification_http_requests_total",
            "Total HTTP requests processed by the notification API.",
            ("method", "path", "status"),
            registry=self.registry,
        )
        self._http_request_duration_seconds: Any = Histogram(
            "notification_http_request_duration_seconds",
            "HTTP request duration for the notification API.",
            ("method", "path", "status"),
            registry=self.registry,
        )
        self._outbox_events_total: Any = Counter(
            "notification_outbox_events_total",
            "Outbox publish attempts by event type and final publish status.",
            ("event_type", "status"),
            registry=self.registry,
        )
        self._outbox_oldest_pending_seconds: Any = Gauge(
            "notification_outbox_oldest_pending_seconds",
            "Age in seconds of the oldest outbox event currently ready to publish.",
            registry=self.registry,
        )
        self._notification_requests_by_status: Any = Gauge(
            "notification_requests_by_status",
            "Current notification request count by request status.",
            ("status",),
            registry=self.registry,
        )
        self._outbox_events_by_status: Any = Gauge(
            "notification_outbox_events_by_status",
            "Current outbox event count by publish status.",
            ("status",),
            registry=self.registry,
        )
        self._deliveries_by_status: Any = Gauge(
            "notification_deliveries_by_status",
            "Current notification delivery count by delivery status.",
            ("status",),
            registry=self.registry,
        )
        self._deliveries_by_channel_status: Any = Gauge(
            "notification_deliveries_by_channel_status",
            "Current notification delivery count by channel and delivery status.",
            ("channel", "status"),
            registry=self.registry,
        )
        self._backend_process_cpu_seconds_total: Any = Gauge(
            "notification_backend_process_cpu_seconds_total",
            "CPU seconds consumed by the notification API process.",
            registry=self.registry,
        )
        self._backend_process_resident_memory_bytes: Any = Gauge(
            "notification_backend_process_resident_memory_bytes",
            "Resident memory used by the notification API process.",
            registry=self.registry,
        )

    def record_http_request(
        self,
        *,
        method: str,
        path: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        labels = {
            "method": method,
            "path": path,
            "status": str(status_code),
        }
        self._http_requests_total.labels(**labels).inc()
        self._http_request_duration_seconds.labels(**labels).observe(duration_seconds)

    def record_outbox_event(self, *, event_type: str, status: str) -> None:
        self._outbox_events_total.labels(event_type=event_type, status=status).inc()

    def set_outbox_oldest_pending_seconds(self, age_seconds: float) -> None:
        self._outbox_oldest_pending_seconds.set(max(age_seconds, 0.0))

    def set_notification_request_status_count(self, *, status: str, count: int) -> None:
        self._notification_requests_by_status.labels(status=status).set(count)

    def set_outbox_event_status_count(self, *, status: str, count: int) -> None:
        self._outbox_events_by_status.labels(status=status).set(count)

    def set_delivery_status_count(self, *, status: str, count: int) -> None:
        self._deliveries_by_status.labels(status=status).set(count)

    def set_delivery_channel_status_count(
        self,
        *,
        channel: str,
        status: str,
        count: int,
    ) -> None:
        self._deliveries_by_channel_status.labels(channel=channel, status=status).set(count)

    def refresh_backend_runtime_metrics(self) -> None:
        self._backend_process_cpu_seconds_total.set(process_time())
        self._backend_process_resident_memory_bytes.set(self._resident_memory_bytes())

    def render(self) -> bytes:
        self.refresh_backend_runtime_metrics()
        return generate_latest(self.registry)

    def _resident_memory_bytes(self) -> int:
        max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return int(max_rss)
        return int(max_rss * 1024)


class PrometheusMetricsEndpoint:
    def __init__(
        self,
        *,
        metrics: PrometheusMetrics,
        before_render: Callable[[], None] | None = None,
    ) -> None:
        self._metrics = metrics
        self._before_render = before_render

    def handle(self) -> Response:
        if self._before_render is not None:
            self._before_render()
        return Response(
            content=self._metrics.render(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )


class PrometheusHttpMiddleware:
    def __init__(self, app: ASGIApp, *, metrics: PrometheusMetrics) -> None:
        self._app = app
        self._metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        started_at = perf_counter()
        status_code = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)

        try:
            await self._app(scope, receive, send_wrapper)
        finally:
            path = self._route_path(scope)
            if path != "/metrics":
                self._metrics.record_http_request(
                    method=str(scope.get("method", "unknown")),
                    path=path,
                    status_code=status_code,
                    duration_seconds=perf_counter() - started_at,
                )

    def _route_path(self, scope: Scope) -> str:
        route = scope.get("route")
        route_path = getattr(route, "path", None)
        if isinstance(route_path, str):
            return route_path
        raw_path = scope.get("path", "unknown")
        return str(raw_path)
