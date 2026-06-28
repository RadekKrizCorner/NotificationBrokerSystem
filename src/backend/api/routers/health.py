from fastapi import APIRouter, Request, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from starlette.responses import JSONResponse, Response

router = APIRouter(tags=["health"])


@router.get("/health/live", include_in_schema=False)
def liveness() -> Response:
    return JSONResponse({"status": "live"})


@router.get("/health/ready", include_in_schema=False)
def readiness(request: Request) -> Response:
    session_factory: sessionmaker[Session] = request.app.state.session_factory
    try:
        with session_factory() as session:
            session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return JSONResponse(
            {"status": "not_ready"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return JSONResponse({"status": "ready"})
