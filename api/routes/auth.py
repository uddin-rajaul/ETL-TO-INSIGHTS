"""
Authentication routes.
Provides login endpoint that returns a JWT token.
"""
import bcrypt
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from db.base import get_db
from db.models_auth import ApiUser
from api.auth.jwt import create_access_token

router = APIRouter(prefix="/auth", tags=["Authentication"])


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


@router.post("/login")
def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: Session = Depends(get_db),
):
    """
    Login with username and password.
    Returns a JWT access token on success.
    OAuth2PasswordRequestForm expects form fields: username, password.
    """
    user = session.query(ApiUser).filter_by(
        username=form_data.username,
        is_active=True
    ).first()

    if not user or not verify_password(form_data.password, cast(str, user.hashed_password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    from sqlalchemy import func
    session.query(ApiUser).filter_by(username=user.username).update(
        {ApiUser.last_login: func.now()}
    )
    session.commit()

    token = create_access_token(data={"sub": user.username})
    return {"access_token": token, "token_type": "bearer"}


@router.post("/register")
def register(
    username: str,
    email: str,
    password: str,
    session: Session = Depends(get_db),
):
    """
    Register a new API user.
    In production this would be admin-only. For now it's open for setup.
    """
    existing = session.query(ApiUser).filter_by(username=username).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists.",
        )

    user = ApiUser(
        username=username,
        email=email,
        hashed_password=hash_password(password),
        is_active=True,
        is_admin=False,
    )
    session.add(user)
    session.commit()
    return {"message": f"User {username} created successfully."}