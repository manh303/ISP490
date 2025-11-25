"""
Database Service - Improved connection management with pooling and retry logic
"""

import os
import logging
import re
from typing import Optional, Any, Dict, List
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import asyncio
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy import text, select
import tenacity
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

@dataclass
class ConnectionConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_size: int = 5
    max_size: int = 20
    max_overflow: int = 30
    pool_timeout: int = 30
    pool_recycle: int = 3600
    echo: bool = False

class DatabaseService:
    """Improved database service with connection pooling and retry logic"""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.engine = None
        self.session_maker = None
        self.is_connected = False
        self.last_health_check = None
        self.connection_failures = 0
        self.max_consecutive_failures = 5

    def _convert_positional_params(self, query: str, params: Optional[List[Any]]):
        """
        Converts $1 style positional params to SQLAlchemy :param_1 syntax.
        Returns converted query and dict parameters.
        """
        if not params:
            return query, {}

        param_dict: Dict[str, Any] = {}

        def replacer(match):
            index = int(match.group(1)) - 1
            key = f"param_{index}"
            if index < len(params):
                param_dict[key] = params[index]
            else:
                param_dict[key] = None
            return f":{key}"

        converted_query = re.sub(r"\$(\d+)", replacer, query)
        return converted_query, param_dict

    async def initialize(self) -> bool:
        """Initialize the database connection pool"""
        try:
            # Create async PostgreSQL URL
            database_url = (
                f"postgresql+asyncpg://{self.config.user}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
            )

            # Create async engine with connection pooling
            self.engine = create_async_engine(
                database_url,
                poolclass=AsyncAdaptedQueuePool,
                pool_size=self.config.min_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle,
                pool_pre_ping=True,  # Test connections before use
                echo=self.config.echo,
                # Connection pool events
                connect_args={
                    "server_settings": {
                        "application_name": "ecommerce_dss_ml_api",
                        "statement_timeout": "30000",  # 30 second timeout
                        "idle_in_transaction_session_timeout": "30000"
                    }
                }
            )

            # Create session maker
            self.session_maker = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # Test connection
            await self._test_connection()
            self.is_connected = True
            self.connection_failures = 0
            logger.info("✅ Database service initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize database service: {e}")
            self.connection_failures += 1
            return False

    async def _test_connection(self) -> None:
        """Test database connection"""
        async with self.session_maker() as session:
            await session.execute(text("SELECT 1"))
            await session.commit()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_one: bool = False,
        fetch_all: bool = True
    ) -> Any:
        """
        Execute query with retry logic

        Args:
            query: SQL query string
            params: Query parameters
            fetch_one: Return single row if True
            fetch_all: Return all rows if True

        Returns:
            Query results
        """
        if not self.is_connected:
            raise Exception("Database not connected")

        try:
            async with self.session_maker() as session:
                result = await session.execute(text(query), params or {})

                if fetch_one:
                    row = result.mappings().first()
                    return dict(row) if row else None
                elif fetch_all:
                    rows = result.mappings().all()
                    return [dict(row) for row in rows]
                else:
                    await session.commit()
                    return result.rowcount

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

    async def fetch(self, query: str, *params) -> List[Dict[str, Any]]:
        """
        Compatibility helper to mimic older database manager API.
        Accepts positional params (used by legacy services) and returns list of dicts.
        """
        converted_query, param_dict = self._convert_positional_params(query, list(params))
        return await self.execute_query(converted_query, param_dict, fetch_all=True)

    async def fetch_one(self, query: str, *params) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row using positional params (compatibility helper).
        """
        converted_query, param_dict = self._convert_positional_params(query, list(params))
        return await self.execute_query(converted_query, param_dict, fetch_one=True)

    async def fetchrow(self, query: str, *params) -> Optional[Dict[str, Any]]:
        """Alias for fetch_one to maintain backward compatibility."""
        return await self.fetch_one(query, *params)

    @asynccontextmanager
    async def get_session(self):
        """Get database session with automatic cleanup"""
        if not self.is_connected:
            raise Exception("Database not connected")

        session = self.session_maker()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health_info = {
            "service": "database",
            "status": "unknown",
            "timestamp": datetime.now().isoformat(),
            "pool_stats": {},
            "last_check": self.last_health_check.isoformat() if self.last_health_check else None
        }

        try:
            # Basic connectivity test
            start_time = datetime.now()
            async with self.session_maker() as session:
                result = await session.execute(text("SELECT 1 as test, pg_postmaster_start_time()"))
                row = result.fetchone()

            response_time = (datetime.now() - start_time).total_seconds() * 1000  # ms

            # Get pool statistics
            pool = self.engine.pool
            pool_stats = {
                "pool_size": getattr(pool, 'size', 0),
                "checkedin": getattr(pool, '_checkedin', 0),
                "checkedout": getattr(pool, '_checkedout', 0),
                "invalid": getattr(pool, '_invalid', 0),
                "overflow": getattr(pool, '_overflow', 0)
            }

            health_info.update({
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "pool_stats": pool_stats,
                "connection_failures": self.connection_failures
            })

            self.last_health_check = datetime.now()

        except Exception as e:
            health_info.update({
                "status": "unhealthy",
                "error": str(e),
                "connection_failures": self.connection_failures
            })
            self.connection_failures += 1

            # If too many consecutive failures, mark as critical
            if self.connection_failures >= self.max_consecutive_failures:
                health_info["status"] = "critical"

        return health_info

    async def get_connection_stats(self) -> Dict[str, Any]:
        """Get detailed connection pool statistics"""
        if not self.engine:
            return {"error": "Engine not initialized"}

        pool = self.engine.pool
        return {
            "pool_size": getattr(pool, 'size', 0),
            "checkedin": getattr(pool, '_checkedin', 0),
            "checkedout": getattr(pool, '_checkedout', 0),
            "invalid": getattr(pool, '_invalid', 0),
            "overflow": getattr(pool, '_overflow', 0),
            "connections_created": getattr(pool, '_created', 0),
            "connections_recycled": getattr(pool, '_recycled', 0)
        }

    async def close(self) -> None:
        """Close database connections"""
        if self.engine:
            await self.engine.dispose()
            self.is_connected = False
            logger.info("Database service closed")

# Global instance
_db_service = None

def get_database_service() -> DatabaseService:
    """Get or create database service instance"""
    global _db_service
    if _db_service is None:
        # Check if DATABASE_URL is provided (e.g., from Render)
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            # Parse DATABASE_URL
            from urllib.parse import urlparse
            parsed = urlparse(database_url)
            config = ConnectionConfig(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path.lstrip('/'),
                user=parsed.username,
                password=parsed.password,
                min_size=int(os.getenv('DB_POOL_MIN_SIZE', '5')),
                max_size=int(os.getenv('DB_POOL_MAX_SIZE', '20')),
                max_overflow=int(os.getenv('DB_POOL_MAX_OVERFLOW', '30')),
                pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', '30')),
                pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '3600')),
                echo=os.getenv('DB_ECHO', 'false').lower() == 'true'
            )
        else:
            # Load configuration from individual environment variables
            config = ConnectionConfig(
                host=os.getenv('DB_HOST', 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME', 'ecommerce_dss_1'),
                user=os.getenv('DB_USER', 'dss_user'),
                password=os.getenv('DB_PASSWORD', '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'),
                min_size=int(os.getenv('DB_POOL_MIN_SIZE', '5')),
                max_size=int(os.getenv('DB_POOL_MAX_SIZE', '20')),
                max_overflow=int(os.getenv('DB_POOL_MAX_OVERFLOW', '30')),
                pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', '30')),
                pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '3600')),
                echo=os.getenv('DB_ECHO', 'false').lower() == 'true'
            )
        _db_service = DatabaseService(config)
    return _db_service

# Backward compatibility functions
async def execute_query_async(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute query asynchronously (backward compatibility)"""
    service = get_database_service()
    return await service.execute_query(query, params, fetch_all=True)

async def get_session():
    """Get database session (backward compatibility)"""
    service = get_database_service()
    return service.get_session()
