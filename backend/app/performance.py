#!/usr/bin/env python3
"""
Performance Optimization Module
Handles caching, query optimization, connection pooling, and monitoring
"""

import asyncio
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from functools import wraps
import pickle
import redis.asyncio as redis

# Database
from databases import Database
from sqlalchemy import text

# FastAPI
from fastapi import Request, Response
from fastapi.responses import JSONResponse

# ====================================
# CACHING SYSTEM
# ====================================

class CacheManager:
    """Advanced caching system with Redis"""

    def __init__(self, redis_client, default_ttl: int = 300):
        self.redis = redis_client
        self.default_ttl = default_ttl
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0
        }

    def _generate_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_data = f"{prefix}:{args}:{sorted(kwargs.items())}"
        return f"cache:{hashlib.md5(key_data.encode()).hexdigest()}"

    async def get(self, key: str) -> Any:
        """Get value from cache"""
        try:
            cached_data = await self.redis.get(key)
            if cached_data:
                self.cache_stats["hits"] += 1
                return pickle.loads(cached_data)
            else:
                self.cache_stats["misses"] += 1
                return None
        except Exception as e:
            print(f"Cache get error: {e}")
            self.cache_stats["misses"] += 1
            return None

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in cache"""
        try:
            ttl = ttl or self.default_ttl
            serialized_data = pickle.dumps(value)
            await self.redis.setex(key, ttl, serialized_data)
            self.cache_stats["sets"] += 1
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete value from cache"""
        try:
            result = await self.redis.delete(key)
            if result:
                self.cache_stats["deletes"] += 1
            return bool(result)
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False

    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern"""
        try:
            keys = await self.redis.keys(pattern)
            if keys:
                result = await self.redis.delete(*keys)
                self.cache_stats["deletes"] += result
                return result
            return 0
        except Exception as e:
            print(f"Cache clear error: {e}")
            return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_requests = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (self.cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0

        redis_info = await self.redis.info()

        return {
            "cache_stats": self.cache_stats,
            "hit_rate": round(hit_rate, 2),
            "total_requests": total_requests,
            "redis_info": {
                "used_memory": redis_info.get("used_memory_human", "0B"),
                "connected_clients": redis_info.get("connected_clients", 0),
                "keyspace_hits": redis_info.get("keyspace_hits", 0),
                "keyspace_misses": redis_info.get("keyspace_misses", 0)
            }
        }

# ====================================
# CACHING DECORATORS
# ====================================

def cache_result(ttl: int = 300, key_prefix: str = "default"):
    """Decorator to cache function results"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get cache manager from app state
            cache_manager = getattr(wrapper, '_cache_manager', None)
            if not cache_manager:
                return await func(*args, **kwargs)

            # Generate cache key
            cache_key = cache_manager._generate_cache_key(key_prefix, *args, **kwargs)

            # Try to get from cache
            cached_result = await cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result

            # Execute function and cache result
            result = await func(*args, **kwargs)
            await cache_manager.set(cache_key, result, ttl)

            return result

        return wrapper
    return decorator

# ====================================
# QUERY OPTIMIZATION
# ====================================

class QueryOptimizer:
    """Database query optimization utilities"""

    def __init__(self, db: Database):
        self.db = db
        self.query_stats = {}

    async def explain_query(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get query execution plan"""
        try:
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
            result = await self.db.fetch_one(text(explain_query), params or {})

            return {
                "query": query,
                "execution_plan": result[0] if result else None,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "query": query,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }

    async def track_query_performance(self, query: str, execution_time: float):
        """Track query performance metrics"""
        query_hash = hashlib.md5(query.encode()).hexdigest()

        if query_hash not in self.query_stats:
            self.query_stats[query_hash] = {
                "query": query,
                "execution_count": 0,
                "total_time": 0,
                "min_time": float('inf'),
                "max_time": 0,
                "avg_time": 0
            }

        stats = self.query_stats[query_hash]
        stats["execution_count"] += 1
        stats["total_time"] += execution_time
        stats["min_time"] = min(stats["min_time"], execution_time)
        stats["max_time"] = max(stats["max_time"], execution_time)
        stats["avg_time"] = stats["total_time"] / stats["execution_count"]

    async def get_slow_queries(self, threshold: float = 1.0) -> List[Dict[str, Any]]:
        """Get queries slower than threshold (in seconds)"""
        slow_queries = []

        for query_hash, stats in self.query_stats.items():
            if stats["avg_time"] >= threshold:
                slow_queries.append(stats)

        return sorted(slow_queries, key=lambda x: x["avg_time"], reverse=True)

    async def optimize_table_indexes(self, table_name: str) -> Dict[str, Any]:
        """Suggest indexes for a table"""
        try:
            # Get table statistics
            stats_query = text("""
                SELECT
                    schemaname,
                    tablename,
                    attname,
                    n_distinct,
                    correlation
                FROM pg_stats
                WHERE tablename = :table_name
                ORDER BY n_distinct DESC
            """)

            stats = await self.db.fetch_all(stats_query, {"table_name": table_name})

            # Get existing indexes
            indexes_query = text("""
                SELECT
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE tablename = :table_name
            """)

            indexes = await self.db.fetch_all(indexes_query, {"table_name": table_name})

            # Suggest new indexes based on column statistics
            suggestions = []
            for stat in stats:
                if stat["n_distinct"] > 100:  # High cardinality columns
                    suggestions.append({
                        "column": stat["attname"],
                        "reason": "High cardinality - good for selective queries",
                        "suggested_index": f"CREATE INDEX idx_{table_name}_{stat['attname']} ON {table_name} ({stat['attname']});"
                    })

            return {
                "table_name": table_name,
                "existing_indexes": [dict(idx) for idx in indexes],
                "column_stats": [dict(stat) for stat in stats],
                "index_suggestions": suggestions
            }

        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e)
            }

# ====================================
# CONNECTION POOLING
# ====================================

class ConnectionPoolManager:
    """Advanced connection pool management"""

    def __init__(self, db: Database):
        self.db = db
        self.pool_stats = {
            "active_connections": 0,
            "total_connections": 0,
            "max_connections": 0,
            "connection_errors": 0
        }

    async def get_pool_status(self) -> Dict[str, Any]:
        """Get connection pool status"""
        try:
            # Get PostgreSQL connection stats
            pool_query = text("""
                SELECT
                    count(*) as total_connections,
                    count(*) FILTER (WHERE state = 'active') as active_connections,
                    count(*) FILTER (WHERE state = 'idle') as idle_connections,
                    max(EXTRACT(EPOCH FROM (NOW() - query_start))) as longest_query_time
                FROM pg_stat_activity
                WHERE datname = current_database()
            """)

            result = await self.db.fetch_one(pool_query)

            # Get database settings
            settings_query = text("""
                SELECT name, setting
                FROM pg_settings
                WHERE name IN ('max_connections', 'shared_buffers', 'work_mem')
            """)

            settings = await self.db.fetch_all(settings_query)

            return {
                "pool_stats": dict(result) if result else {},
                "database_settings": {setting["name"]: setting["setting"] for setting in settings},
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }

# ====================================
# PERFORMANCE MONITORING
# ====================================

class PerformanceMonitor:
    """Performance monitoring and metrics collection"""

    def __init__(self, cache_manager: CacheManager, query_optimizer: QueryOptimizer):
        self.cache_manager = cache_manager
        self.query_optimizer = query_optimizer
        self.request_metrics = {
            "total_requests": 0,
            "total_response_time": 0,
            "slow_requests": 0,
            "error_requests": 0
        }

    async def track_request(self, request: Request, response_time: float, status_code: int):
        """Track request performance"""
        self.request_metrics["total_requests"] += 1
        self.request_metrics["total_response_time"] += response_time

        if response_time > 1.0:  # Slow request threshold
            self.request_metrics["slow_requests"] += 1

        if status_code >= 400:
            self.request_metrics["error_requests"] += 1

    async def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        try:
            # Cache statistics
            cache_stats = await self.cache_manager.get_stats()

            # Query performance
            slow_queries = await self.query_optimizer.get_slow_queries()

            # Request metrics
            total_requests = self.request_metrics["total_requests"]
            avg_response_time = (
                self.request_metrics["total_response_time"] / total_requests
                if total_requests > 0 else 0
            )

            error_rate = (
                self.request_metrics["error_requests"] / total_requests * 100
                if total_requests > 0 else 0
            )

            slow_request_rate = (
                self.request_metrics["slow_requests"] / total_requests * 100
                if total_requests > 0 else 0
            )

            return {
                "timestamp": datetime.utcnow().isoformat(),
                "cache_performance": cache_stats,
                "request_metrics": {
                    "total_requests": total_requests,
                    "avg_response_time": round(avg_response_time, 3),
                    "error_rate": round(error_rate, 2),
                    "slow_request_rate": round(slow_request_rate, 2)
                },
                "slow_queries": slow_queries[:10],  # Top 10 slow queries
                "recommendations": self._generate_recommendations(cache_stats, slow_queries, slow_request_rate)
            }

        except Exception as e:
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }

    def _generate_recommendations(self, cache_stats: Dict, slow_queries: List, slow_request_rate: float) -> List[str]:
        """Generate performance optimization recommendations"""
        recommendations = []

        # Cache recommendations
        if cache_stats["hit_rate"] < 80:
            recommendations.append("Consider increasing cache TTL or caching more frequently accessed data")

        if cache_stats["cache_stats"]["misses"] > cache_stats["cache_stats"]["hits"]:
            recommendations.append("Cache hit rate is low - review caching strategy")

        # Query recommendations
        if slow_queries:
            recommendations.append(f"Found {len(slow_queries)} slow queries - consider optimization or indexing")

        # Request recommendations
        if slow_request_rate > 10:
            recommendations.append("High slow request rate - consider implementing pagination or query optimization")

        if not recommendations:
            recommendations.append("Performance looks good! No immediate optimizations needed.")

        return recommendations

# ====================================
# PERFORMANCE MIDDLEWARE
# ====================================

class PerformanceMiddleware:
    """FastAPI middleware for performance monitoring"""

    def __init__(self, performance_monitor: PerformanceMonitor):
        self.performance_monitor = performance_monitor

    async def __call__(self, request: Request, call_next):
        """Process performance monitoring middleware"""
        start_time = time.time()

        try:
            response = await call_next(request)
            response_time = time.time() - start_time

            # Track request performance
            await self.performance_monitor.track_request(
                request,
                response_time,
                response.status_code
            )

            # Add performance headers
            response.headers["X-Response-Time"] = f"{response_time:.3f}s"
            response.headers["X-Request-ID"] = str(id(request))

            return response

        except Exception as e:
            response_time = time.time() - start_time
            await self.performance_monitor.track_request(request, response_time, 500)
            raise

# ====================================
# OPTIMIZATION UTILITIES
# ====================================

class OptimizationUtils:
    """Utility functions for performance optimization"""

    @staticmethod
    async def bulk_insert_optimized(db: Database, table: str, data: List[Dict], batch_size: int = 1000):
        """Optimized bulk insert with batching"""
        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]

            if not batch:
                continue

            # Generate INSERT query
            columns = list(batch[0].keys())
            placeholders = ", ".join([f":{col}" for col in columns])
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

            try:
                await db.execute_many(text(query), batch)
                total_inserted += len(batch)
            except Exception as e:
                print(f"Bulk insert error for batch {i//batch_size + 1}: {e}")

        return total_inserted

    @staticmethod
    async def create_materialized_view(db: Database, view_name: str, query: str) -> bool:
        """Create materialized view for performance"""
        try:
            create_view_query = f"CREATE MATERIALIZED VIEW {view_name} AS {query}"
            await db.execute(text(create_view_query))

            # Create refresh function
            refresh_function = f"""
            CREATE OR REPLACE FUNCTION refresh_{view_name}()
            RETURNS void AS $$
            BEGIN
                REFRESH MATERIALIZED VIEW {view_name};
            END;
            $$ LANGUAGE plpgsql;
            """
            await db.execute(text(refresh_function))

            return True
        except Exception as e:
            print(f"Materialized view creation error: {e}")
            return False

    @staticmethod
    def compress_response(data: Any, threshold: int = 1024) -> bytes:
        """Compress response data if it exceeds threshold"""
        import gzip

        json_data = json.dumps(data, default=str).encode('utf-8')

        if len(json_data) > threshold:
            return gzip.compress(json_data)

        return json_data

# Export main classes
__all__ = [
    'CacheManager', 'QueryOptimizer', 'ConnectionPoolManager',
    'PerformanceMonitor', 'PerformanceMiddleware', 'OptimizationUtils',
    'cache_result'
]