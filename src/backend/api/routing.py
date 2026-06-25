from collections.abc import Callable, Sequence
from typing import Any, ParamSpec, TypeVar, cast

from fastapi import APIRouter

P = ParamSpec("P")
R = TypeVar("R")


class RouteDefinition:
    def __init__(
        self,
        *,
        path: str,
        method: str,
        response_model: Any | None = None,
        status_code: int | None = None,
    ) -> None:
        self.path = path
        self.method = method
        self.response_model = response_model
        self.status_code = status_code


def route(
    *,
    method: str,
    path: str,
    response_model: Any | None = None,
    status_code: int | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    definition = RouteDefinition(
        path=path,
        method=method,
        response_model=response_model,
        status_code=status_code,
    )

    def decorator(endpoint: Callable[P, R]) -> Callable[P, R]:
        cast(Any, endpoint)._api_route_definition = definition
        return endpoint

    return decorator


class ApiRoutes:
    prefix = ""
    tags: Sequence[str] = ()

    def __init__(self) -> None:
        self.router = APIRouter(prefix=self.prefix, tags=list(self.tags))
        self._register_routes()

    def _register_routes(self) -> None:
        for endpoint_name, definition in self._route_definitions():
            self.router.add_api_route(
                definition.path,
                getattr(self, endpoint_name),
                methods=[definition.method],
                response_model=definition.response_model,
                status_code=definition.status_code,
            )

    @classmethod
    def _route_definitions(cls) -> list[tuple[str, RouteDefinition]]:
        definitions: list[tuple[str, RouteDefinition]] = []
        for base_class in reversed(cls.mro()):
            for endpoint_name, endpoint in base_class.__dict__.items():
                definition = getattr(endpoint, "_api_route_definition", None)
                if isinstance(definition, RouteDefinition):
                    definitions.append((endpoint_name, definition))
        return definitions
