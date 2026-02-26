"""
Database engine and session setup.
All models inherit from Base defined here.
get_db() is used as a FastAPI dependency to inject sessions into routes.
"""


import os
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from dotenv import load_dotenv

load_dotenv()

def get_database_url():
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

engine = create_engine(get_database_url())

SessionLocal = sessionmaker(bind=engine, autocommit = False, autoflush= False)

class Base(DeclarativeBase):
    pass

def get_db():
    """
    FastAPI dependency that provides a database session per request.
    Automatically closes the session when the request is done.
    Usage in routes: db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_settings() -> dict:
    """Load and return settings from config/settings.yaml."""
    import yaml
    with open("config/settings.yaml", "r") as f:
        return yaml.safe_load(f)