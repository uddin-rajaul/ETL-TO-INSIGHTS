"""
Auth layer - API user accounts for authentication and authorization.
Passwords are stored as bcrypt hashes, never plaintext.
is_admin controls write access - admin users can create, update, delete.
Non-admin users get read-only access.
"""

from sqlalchemy import Column, Integer,String, Boolean, DateTime
from sqlalchemy.sql import func
from db.base import Base

class ApiUser(Base):
    """
    Users who can authenticate and access the API.
    Created manually by an admin - there is no public registration.
    """

    __tablename__ = "api_user"
    __table_args__ = {"schema": "auth"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable= False)
    hashed_password = Column(String(255), nullable=True)
    is_active = Column(Boolean, default= True)
    is_admin = Column(Boolean, default= False)
    created_at = Column(DateTime, server_default=func.now())
    last_login = Column(DateTime)