#!/usr/bin/env python3
"""
Redis Caching Service for Performance Optimization
Reduces database queries by caching frequent requests
"""
import os
import json
import hashlib
from typing import Optional, Any, Callable
from functools import wraps
import logging

logger = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://localhost:6379/0"

class CacheService:
    """Redis cache service with fallback to in-memory cache"""
    
    def __init__(self):
        self.redis = None
        self.memory_cache = {}  # Fallback in-memory cache
        self.enabled = False
        
        try:
            import redis
            redis_url = os.getenv("REDIS_URL", DEFAULT_REDIS_URL).strip()

            if redis_url.lower() in {"", "memory", "disabled"}:
                logger.info("Redis cache disabled via REDIS_URL=%s, using in-memory cache", redis_url or "''")
                return

            self.redis = redis.from_url(redis_url, decode_responses=True)
            # Test connection
            self.redis.ping()
            self.enabled = True
            logger.info("✅ Redis cache enabled (url=%s)", redis_url)
        except Exception as e:
            logger.warning(f"⚠️  Redis not available ({e}), using in-memory cache")
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from arguments"""
        # Create unique key from arguments
        key_data = f"{prefix}:{str(args)}:{str(sorted(kwargs.items()))}"
        key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
        return f"{prefix}:{key_hash}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            if self.redis and self.enabled:
                value = self.redis.get(key)
                if value:
                    return json.loads(value)
            else:
                # In-memory cache
                return self.memory_cache.get(key)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        return None
    
    def set(self, key: str, value: Any, ttl: int = 300):
        """Set value in cache with TTL (seconds)"""
        try:
            json_value = json.dumps(value, default=str)
            
            if self.redis and self.enabled:
                self.redis.setex(key, ttl, json_value)
            else:
                # In-memory cache (simple, no TTL)
                self.memory_cache[key] = json.loads(json_value)
                # Limit memory cache size
                if len(self.memory_cache) > 100:
                    # Remove oldest entries
                    keys_to_remove = list(self.memory_cache.keys())[:20]
                    for k in keys_to_remove:
                        del self.memory_cache[k]
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    def delete(self, key: str):
        """Delete specific key"""
        try:
            if self.redis and self.enabled:
                self.redis.delete(key)
            else:
                self.memory_cache.pop(key, None)
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
    
    def delete_pattern(self, pattern: str):
        """Delete keys matching pattern"""
        try:
            if self.redis and self.enabled:
                for key in self.redis.scan_iter(pattern):
                    self.redis.delete(key)
            else:
                # In-memory: delete keys starting with pattern
                pattern_prefix = pattern.replace('*', '')
                keys_to_delete = [k for k in self.memory_cache.keys() if k.startswith(pattern_prefix)]
                for k in keys_to_delete:
                    del self.memory_cache[k]
        except Exception as e:
            logger.error(f"Cache delete pattern error: {e}")
    
    def clear(self):
        """Clear all cache"""
        try:
            if self.redis and self.enabled:
                self.redis.flushdb()
            else:
                self.memory_cache.clear()
        except Exception as e:
            logger.error(f"Cache clear error: {e}")


# Global cache instance
cache = CacheService()


def cached(prefix: str, ttl: int = 300):
    """
    Decorator to cache function results
    
    Usage:
        @cached("overview_kpis", ttl=300)
        async def get_overview_kpis(from_date, to_date):
            # expensive database query
            return result
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = cache._generate_key(prefix, *args, **kwargs)
            
            # Try cache first
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache HIT: {cache_key}")
                # Try to reconstruct pydantic model from dict
                try:
                    # Get the return type annotation from the function
                    return_type = func.__annotations__.get('return')
                    if return_type and hasattr(return_type, '__origin__'):
                        # Handle List[Model] types
                        return cached_result
                    elif return_type and hasattr(return_type, 'parse_obj'):
                        # Reconstruct pydantic model
                        return return_type.parse_obj(cached_result)
                    else:
                        return cached_result
                except:
                    # If reconstruction fails, return as-is
                    return cached_result
            
            # Cache miss - call function
            logger.debug(f"Cache MISS: {cache_key}")
            result = await func(*args, **kwargs)
            
            # Cache the result
            if result is not None:
                # Convert pydantic models to dict for caching
                if hasattr(result, 'dict'):
                    cache.set(cache_key, result.dict(), ttl=ttl)
                elif hasattr(result, '__dict__'):
                    cache.set(cache_key, result.__dict__, ttl=ttl)
                else:
                    cache.set(cache_key, result, ttl=ttl)
            
            return result
        return wrapper
    return decorator

