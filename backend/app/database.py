import asyncpg, asyncio, logging, os, re
from contextlib import asynccontextmanager
from app.db_pool import get_pool

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        # Connection is now managed by the pool, not locally
        self.is_connected = True 

    async def connect(self, retries: int = 20, delay: float = 1.0):
        # No-op: Pool is initialized in main.py lifespan
        pass

    async def ensure_connected(self):
        # No-op: Pool handles connections
        pass

    async def execute_query(self, query, values=None):
        try:
            pool = await get_pool()
        except RuntimeError:
            # Fallback for scripts or if pool not init (though main.py should init it)
            # This is a safety net but ideally shouldn't happen in app context
            logger.warning("Pool not initialized, creating temporary connection")
            conn = await asyncpg.connect(self.dsn)
            try:
                return await self._execute_on_conn(conn, query, values)
            finally:
                await conn.close()

        async with pool.acquire() as conn:
            return await self._execute_on_conn(conn, query, values)

    async def _execute_on_conn(self, conn, query, values):
        # --- HỖ TRỢ named params ':email' -> positional '$1' ---
        text = str(query)
        if isinstance(values, dict):
            pattern = re.compile(r'(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)')
            names = []
            def repl(m):
                names.append(m.group(1))
                return f'${len(names)}'
            sql = pattern.sub(repl, text)
            params = tuple(values[n] for n in names)
            rows = await conn.fetch(sql, *params)
        elif isinstance(values, (list, tuple)):
            rows = await conn.fetch(text, *values)
        else:
            rows = await conn.fetch(text)
        return [dict(r) for r in rows]
    
    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions with automatic rollback on error
        """
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    yield conn
        except RuntimeError:
            # Fallback for scripts
            conn = await asyncpg.connect(self.dsn)
            try:
                async with conn.transaction():
                    yield conn
            finally:
                await conn.close()
