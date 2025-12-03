"""
Database configuration and shared async DatabaseManager instance.
Provides:
- db_manager: asyncpg-based DatabaseManager for app endpoints
- engine, SessionLocal: SQLAlchemy engine/session for legacy scripts
"""

import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db_config import DATABASE_URL
from app.database import DatabaseManager

logger = logging.getLogger(__name__)

# SQLAlchemy engine/session (used by legacy scripts like scripts/create_admin.py)
SQLALCHEMY_DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL", DATABASE_URL)
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Async database manager used across FastAPI endpoints
db_manager = DatabaseManager(DATABASE_URL)

__all__ = ["db_manager", "DatabaseManager", "engine", "SessionLocal"]
