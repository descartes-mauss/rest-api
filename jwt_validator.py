from typing import Any, Dict, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

SECRET_KEY = "a-string-secret-at-least-256-bits-long"
ALGORITHM = "HS256"

BEARER_HEADER = HTTPBearer()


def validate_jwt(
    credentials: HTTPAuthorizationCredentials = Depends(BEARER_HEADER),
) -> Dict[str, Any]:
    """
    Dependency that validates a JWT from `Authorization: Bearer <JWT_TOKEN>`
    """
    token = credentials.credentials

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired JWT",
        )


def get_tenant_schema(
    authorization: Dict[str, Any] = Depends(validate_jwt),
) -> str:
    """Dependency that extracts and validates the tenant schema from the JWT payload."""
    schema: Optional[str] = authorization.get("orgId")
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Authorization token missing tenant schema information.",
        )
    return schema
