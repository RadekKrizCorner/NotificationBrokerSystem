from fastapi.routing import APIRoute
from pydantic import BaseModel

from backend.api.routing import ApiRoutes, route


class DemoResponse(BaseModel):
    value: str


class DemoRoutes(ApiRoutes):
    prefix = "/demo"
    tags = ["demo"]

    @route(
        method="GET",
        path="/items",
        response_model=DemoResponse,
    )
    def list_items(self) -> DemoResponse:
        return DemoResponse(value="ok")


class TestApiRoutes:
    def test_decorated_methods_register_fastapi_routes(self) -> None:
        routes = DemoRoutes()
        [registered_route] = [
            registered for registered in routes.router.routes if isinstance(registered, APIRoute)
        ]

        assert registered_route.path == "/demo/items"
        assert registered_route.methods == {"GET"}
        assert registered_route.response_model is DemoResponse

    def test_registered_endpoint_is_bound_to_route_instance(self) -> None:
        routes = DemoRoutes()
        [registered_route] = [
            registered for registered in routes.router.routes if isinstance(registered, APIRoute)
        ]

        response = registered_route.endpoint()

        assert response == DemoResponse(value="ok")
