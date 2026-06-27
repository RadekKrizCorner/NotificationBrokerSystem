from typing import Any

import jwt
from fastapi import HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials


class AuthenticatedPrincipal:
    def __init__(self, *, subject: str, token_type: str, scopes: frozenset[str]) -> None:
        self.subject = subject
        self.token_type = token_type
        self.scopes = scopes

    def require_type(self, token_type: str) -> None:
        if self.token_type != token_type:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="token type is not allowed",
            )

    def require_scope(self, scope: str) -> None:
        if scope not in self.scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="required scope is missing",
            )


def decode_bearer_token(
    credentials: HTTPAuthorizationCredentials | None,
    *,
    jwt_secret: str,
    jwt_algorithm: str,
    jwt_issuer: str,
    jwt_audience: str,
) -> AuthenticatedPrincipal:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )

    try:
        payload = jwt.decode(
            credentials.credentials,
            jwt_secret,
            algorithms=[jwt_algorithm],
            issuer=jwt_issuer,
            audience=jwt_audience,
            options={
                "require": ["sub", "type", "exp", "iat", "iss", "aud"],
            },
        )
    except jwt.PyJWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid bearer token",
        ) from exc

    return _principal_from_payload(payload)


def _principal_from_payload(payload: dict[str, Any]) -> AuthenticatedPrincipal:
    subject = payload.get("sub")
    token_type = payload.get("type")
    scopes = payload.get("scopes", [])

    if not isinstance(subject, str) or not subject or len(subject) > 256:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid subject")
    if token_type not in {"service", "user"}:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token type")
    if (
        not isinstance(scopes, list)
        or len(scopes) > 32
        or any(
            not isinstance(scope, str) or not scope or len(scope) > 128
            for scope in scopes
        )
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid scopes")

    return AuthenticatedPrincipal(
        subject=subject,
        token_type=token_type,
        scopes=frozenset(scopes),
    )
