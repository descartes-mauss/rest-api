from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

SECRET_KEY = "a-string-secret-at-least-256-bits-long"
ALGORITHM = "HS256"

BEARER_HEADER = HTTPBearer()


def validate_jwt(
    credentials: HTTPAuthorizationCredentials = Depends(BEARER_HEADER),
):
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
