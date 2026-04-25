"""JWT auth for dashboard users. Returns {user_id, role} from Bearer token."""
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from config import get_settings

settings = get_settings()


class HTTPBearer401(HTTPBearer):
    """Standard HTTPBearer returns 403 when header is missing; this returns 401."""

    def make_not_authenticated_error(self) -> HTTPException:
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )


security = HTTPBearer401()


def _require_jwt_secret() -> str:
    secret = settings.JWT_SECRET
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Auth not configured (JWT_SECRET missing)",
        )
    return secret


def create_access_token(user_id: str, role: int) -> str:
    """Issue a signed JWT with user_id (sub), role, and expiry."""
    secret = _require_jwt_secret()
    expire = datetime.now(timezone.utc) + timedelta(
        hours=settings.JWT_ACCESS_TOKEN_EXPIRE_HOURS
    )
    payload = {"sub": user_id, "role": role, "exp": expire}
    return jwt.encode(payload, secret, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """Decode and verify token. Returns payload dict or None on failure."""
    secret = settings.JWT_SECRET
    if not secret:
        return None
    try:
        return jwt.decode(token, secret, algorithms=[settings.JWT_ALGORITHM])
    except JWTError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> dict:
    """
    FastAPI dependency: require valid Bearer JWT.
    Returns {"user_id": str, "role": int}. Optionally includes "impersonator".
    """
    secret = _require_jwt_secret()
    try:
        payload = jwt.decode(
            credentials.credentials, secret, algorithms=[settings.JWT_ALGORITHM]
        )
        user_id = payload.get("sub")
        role = payload.get("role")
        if user_id is None or role is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        result: dict = {"user_id": user_id, "role": int(role)}
        impersonator = payload.get("impersonator") or payload.get("impersonator_id")
        if impersonator:
            result["impersonator"] = impersonator
        return result
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_tenant(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> str:
    """Legacy dependency: return tenant_id (falls back to sub/user_id)."""
    secret = _require_jwt_secret()
    try:
        payload = jwt.decode(
            credentials.credentials, secret, algorithms=[settings.JWT_ALGORITHM]
        )
        tenant_id = payload.get("tenant_id") or payload.get("sub")
        if not tenant_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return tenant_id
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
