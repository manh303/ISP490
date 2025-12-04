#!/usr/bin/env python3
"""
Redis Caching Service for Performance Optimization (async)
- Dùng redis.asyncio trong môi trường FastAPI async
- Fallback sang in-memory cache khi Redis không có
"""
import os
import json
import hashlib
from typing import Optional, Any, Callable
from functools import wraps
import logging

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as redis  # cần `pip install "redis[async]"`
except ImportError:
    redis = None


class CacheService:
    """Redis cache service with fallback to in-memory cache (async API)."""

    def __init__(self) -> None:
        self._url: Optional[str] = os.getenv("REDIS_URL", "redis://default:H7452ZmrxCCJDB6pxk8pqOzgj2pAq7c3@redis-15687.crce194.ap-seast-1-1.ec2.cloud.redislabs.com:15687").strip() or None
        self._client: Optional["redis.Redis"] = None
        self.enabled: bool = False
        self.memory_cache: dict[str, Any] = {}  # Fallback in-memory cache

    async def init(self) -> None:
        """Khởi tạo kết nối Redis (gọi trong lifespan)."""
        if not redis:
            logger.warning("redis library not installed, using in-memory cache")
            self.enabled = False
            return

        if not self._url or self._url.lower() in {"", "memory", "disabled"}:
            logger.info(
                "Redis cache disabled via REDIS_URL=%s, using in-memory cache",
                self._url or "''",
            )
            self.enabled = False
            return

        try:
            self._client = redis.from_url(
                self._url,
                encoding="utf-8",
                decode_responses=True,
            )
            # Test connection
            await self._client.ping()
            self.enabled = True
            logger.info("✅ Redis cache enabled (url=%s)", self._url)
        except Exception as e:
            logger.warning("⚠️  Redis not available (%s), using in-memory cache", e)
            self.enabled = False
            self._client = None

    async def close(self) -> None:
        """Đóng kết nối Redis (gọi ở shutdown)."""
        if self._client:
            try:
                await self._client.close()
                logger.info("✅ Redis connection closed")
            except Exception as e:
                logger.error("Redis close error: %s", e)

    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from arguments."""
        key_data = f"{prefix}:{str(args)}:{str(sorted(kwargs.items()))}"
        key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
        return f"{prefix}:{key_hash}"

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache (async)."""
        try:
            if self.enabled and self._client:
                value = await self._client.get(key)
                if value is not None:
                    return json.loads(value)
            else:
                return self.memory_cache.get(key)
        except Exception as e:
            logger.error("Cache get error: %s", e)
        return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set value in cache với TTL (seconds) (async)."""
        try:
            json_value = json.dumps(value, default=str)

            if self.enabled and self._client:
                await self._client.set(key, json_value, ex=ttl)
            else:
                # In-memory cache (simple, no TTL)
                self.memory_cache[key] = json.loads(json_value)
                # Limit memory cache size
                if len(self.memory_cache) > 100:
                    # Remove oldest entries
                    keys_to_remove = list(self.memory_cache.keys())[:20]
                    for k in keys_to_remove:
                        self.memory_cache.pop(k, None)
        except Exception as e:
            logger.error("Cache set error: %s", e)

    async def delete(self, key: str) -> None:
        try:
            if self.enabled and self._client:
                await self._client.delete(key)
            else:
                self.memory_cache.pop(key, None)
        except Exception as e:
            logger.error("Cache delete error: %s", e)

    async def delete_pattern(self, pattern: str) -> None:
        try:
            if self.enabled and self._client:
                async for key in self._client.scan_iter(pattern):
                    await self._client.delete(key)
            else:
                prefix = pattern.replace("*", "")
                keys_to_delete = [k for k in self.memory_cache if k.startswith(prefix)]
                for k in keys_to_delete:
                    self.memory_cache.pop(k, None)
        except Exception as e:
            logger.error("Cache delete pattern error: %s", e)

    async def clear(self) -> None:
        try:
            if self.enabled and self._client:
                await self._client.flushdb()
            else:
                self.memory_cache.clear()
        except Exception as e:
            logger.error("Cache clear error: %s", e)


# Global cache instance
cache = CacheService()


def cached(prefix: str, ttl: int = 300):
    """
    Decorator để cache kết quả của hàm async.

    Usage:
        @cached("overview_kpis", ttl=300)
        async def get_overview_kpis(...):
            ...
    """

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache_key = cache._generate_key(prefix, *args, **kwargs)

            # 1. Try cache
            cached_result = await cache.get(cache_key)
            if cached_result is not None:
                logger.debug("Cache HIT: %s", cache_key)
                # Nếu cần reconstruct Pydantic model thì xử lý ở đây
                return cached_result

            # 2. Cache MISS → gọi hàm gốc
            logger.debug("Cache MISS: %s", cache_key)
            result = await func(*args, **kwargs)

            # 3. Lưu cache
            if result is not None:
                to_store = result
                try:
                    if hasattr(result, "dict"):
                        to_store = result.dict()
                    elif hasattr(result, "__dict__"):
                        to_store = result.__dict__
                except Exception:
                    pass

                await cache.set(cache_key, to_store, ttl=ttl)

            return result

        return wrapper

    return decorator
