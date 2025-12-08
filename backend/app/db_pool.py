"""
Database Connection Pool Manager
Provides centralized connection pool for async PostgreSQL operations
"""

import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def init_pool(database_url: str, min_size: int = 5, max_size: int = 20, ssl=None) -> asyncpg.Pool:
    """
    Initialize the global database connection pool

    Args:
        database_url: PostgreSQL connection string
        min_size: Minimum number of connections to maintain
        max_size: Maximum number of connections to create
        ssl: SSL context or string (e.g. 'require') for the connection
    """
    global _pool

    if _pool is not None:
        logger.warning("Connection pool already initialized")
        return _pool

    logger.info(f"Attempting to connect to database: {database_url[:50]}... (SSL: {ssl})")

    try:
        _pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=min_size,
            max_size=max_size,
            max_inactive_connection_lifetime=300,  # Close idle connections after 5 min
            command_timeout=60,  # Commands timeout after 60 seconds
            ssl=ssl
        )

        # Test the connection
        async with _pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        logger.info(f"✅ Database connection pool initialized successfully (min={min_size}, max={max_size}, id: {id(_pool)})")
        return _pool
    except Exception as e:
        logger.error(f"❌ Failed to initialize connection pool: {e}")
        logger.error(f"Database URL: {database_url}")
        _pool = None  # Reset to None on failure
        raise


async def get_pool() -> asyncpg.Pool:
    """
    Get the global connection pool

    Returns:
        The connection pool instance

    Raises:
        RuntimeError: If pool is not initialized
    """
    global _pool

    logger.debug(f"get_pool called, _pool is {'None' if _pool is None else f'initialized (id: {id(_pool)})'}")
    if _pool is None:
        raise RuntimeError("Database connection pool not initialized. Call init_pool() first.")

    # Check if pool is closed
    if _pool._closed:
        logger.warning("Pool is closed, resetting to None")
        _pool = None
        raise RuntimeError("Database connection pool was closed. Call init_pool() first.")

    return _pool


async def close_pool():
    """Close the global connection pool"""
    global _pool

    if _pool is not None:
        logger.info(f"close_pool called, _pool is initialized (id: {id(_pool)})")
        await _pool.close()
        logger.info("Database connection pool closed")
        _pool = None
    else:
        logger.info("close_pool called, _pool is already None")
