import asyncpg, asyncio, logging, os, re
from contextlib import asynccontextmanager
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection: asyncpg.Connection | None = None
        self.is_connected = False

    async def connect(self, retries: int = 20, delay: float = 1.0):
        for i in range(retries):
            try:
                self.connection = await asyncpg.connect(self.dsn)
                self.is_connected = True
                logger.info("DB connected")
                return
            except Exception as e:
                logger.warning(f"DB connect failed ({i+1}/{retries}): {e}")
                await asyncio.sleep(delay)
        self.is_connected = False
        logger.error("DB connect retries exhausted")

    async def ensure_connected(self):
        if not self.is_connected or self.connection is None:
            await self.connect()
        else:
            try:
                await self.connection.execute('SELECT 1;')
            except Exception:
                await self.connect()

    async def execute_query(self, query, values=None):
        await self.ensure_connected()
        if not self.is_connected:
            # đừng trả 503 mù mờ; log chi tiết để debug
            raise RuntimeError("Database unavailable after reconnect attempts")

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
            rows = await self.connection.fetch(sql, *params)
        elif isinstance(values, (list, tuple)):
            rows = await self.connection.fetch(text, *values)
        else:
            rows = await self.connection.fetch(text)
        return [dict(r) for r in rows]
    
    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions with automatic rollback on error
        
        Usage:
            async with db.transaction() as conn:
                await conn.fetchrow(query, *params)
                await conn.execute(query, *params)
        """
        await self.ensure_connected()
        if not self.is_connected:
            raise RuntimeError("Database unavailable for transaction")
        
        # Start transaction
        async with self.connection.transaction():
            # Yield connection object để gọi fetchrow, execute, fetchval...
            yield self.connection