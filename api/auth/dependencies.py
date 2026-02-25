"""
FastAPI dependencies for protecting routes.
Injects the current authenticated user into route handlers.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from db.base import get_db
from db.models_auth import ApiUser
from api.auth.jwt import verify_access_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def get_current_user(
    token: str = Depends(oauth2_scheme),
    session: Session = Depends(get_db),
) -> ApiUser:
    """
    Dependency that validates the JWT token and returns the current user.
    Inject this into any route that requires authentication.
    Raises 401 if token is invalid or user not found.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token.",
        headers={"WWW-Authenticate": "Bearer"},
    )

    username = verify_access_token(token)
    if username is None:
        raise credentials_exception

    user = session.query(ApiUser).filter_by(username=username, is_active=True).first()
    if user is None:
        raise credentials_exception

    return user


def get_admin_user(current_user: ApiUser = Depends(get_current_user)) -> ApiUser:
    """
    Dependency that requires the current user to be an admin.
    Use this for write endpoints (POST, PUT, DELETE).
    Raises 403 if user is not an admin.
    """
    if current_user.is_admin is not True:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )
    return current_user