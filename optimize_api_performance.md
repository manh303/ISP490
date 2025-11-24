# 🚀 Tối Ưu Performance API với Database Xa

## ❌ VẤN ĐỀ
- Database ở **Oregon, US** (Render)
- User ở **Việt Nam**
- Latency: **~200-300ms** mỗi query
- API chậm do phải gọi nhiều queries

## ✅ GIẢI PHÁP NHANH (Không cần migrate)

### 1. **Implement Redis Caching** (KHUYẾN NGHỊ)

Cache các queries phổ biến để giảm số lần gọi database:

```python
# backend/app/core/cache.py
import redis
import json
import os
from typing import Optional, Any
from datetime import timedelta

class CacheService:
    def __init__(self):
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = redis.from_url(redis_url, decode_responses=True)
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value"""
        try:
            value = self.redis.get(key)
            return json.loads(value) if value else None
        except:
            return None
    
    def set(self, key: str, value: Any, ttl: int = 300):
        """Set cache with TTL (default 5 minutes)"""
        try:
            self.redis.setex(key, ttl, json.dumps(value))
        except:
            pass
    
    def delete(self, pattern: str):
        """Delete keys by pattern"""
        try:
            for key in self.redis.scan_iter(pattern):
                self.redis.delete(key)
        except:
            pass

# Global cache instance
cache = CacheService()
```

**Áp dụng vào Analytics Service:**

```python
# backend/app/services/analytics_service.py

from app.core.cache import cache

async def get_overview_trends(self, from_date, to_date, platform_code, category_key):
    # Generate cache key
    cache_key = f"trends:{from_date}:{to_date}:{platform_code}:{category_key}"
    
    # Try cache first
    cached = cache.get(cache_key)
    if cached:
        return OverviewTrendResponse(**cached)
    
    # Query database (slow)
    result = await self.db.fetch(sql, *params)
    
    # Cache for 5 minutes
    cache.set(cache_key, result.dict(), ttl=300)
    
    return result
```

### 2. **Connection Pooling** (ÁP DỤNG NGAY)

Giảm overhead khi tạo connection mới:

```python
# backend/app/api/v1/analytics.py

import asyncpg

# Create connection pool (reuse connections)
async def get_db_pool():
    if not hasattr(get_db_pool, 'pool'):
        get_db_pool.pool = await asyncpg.create_pool(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            min_size=5,      # Keep 5 connections open
            max_size=20,     # Max 20 connections
            command_timeout=60,
            server_settings={
                'application_name': 'analytics_api',
                'jit': 'off'
            }
        )
    return get_db_pool.pool

async def get_db():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        yield conn
```

### 3. **Batch Queries** (QUAN TRỌNG)

Gộp nhiều queries thành 1:

```python
# ❌ BAD: 3 separate queries (3 x 250ms = 750ms)
kpis = await get_overview_kpis(...)
trends = await get_overview_trends(...)
comparison = await get_platform_comparison(...)

# ✅ GOOD: 1 combined query (1 x 250ms = 250ms)
result = await conn.fetch("""
    WITH kpis AS (...),
         trends AS (...),
         comparison AS (...)
    SELECT 
        (SELECT json_agg(k.*) FROM kpis k) as kpis,
        (SELECT json_agg(t.*) FROM trends t) as trends,
        (SELECT json_agg(c.*) FROM comparison c) as comparison
""")
```

### 4. **Lazy Loading cho Reviews**

Chỉ load reviews khi cần:

```python
# ✅ GOOD: Separate endpoint for reviews
# GET /api/v1/analytics/products/{id}        -> Fast (no reviews)
# GET /api/v1/analytics/products/{id}/reviews -> Slower (with reviews)
```

### 5. **CDN cho Static Data**

Cache response ở edge locations gần user:

```python
# backend/app/main.py

from fastapi.responses import JSONResponse

@app.get("/api/v1/analytics/overview/kpis")
async def get_kpis(...):
    response = JSONResponse(content=data)
    # Cache at CDN for 5 minutes
    response.headers["Cache-Control"] = "public, max-age=300"
    return response
```

## 🔧 GIẢI PHÁP DÀI HẠN

### **Option 1: Migrate sang Region gần hơn**

**Render Regions:**
- Oregon, US (Hiện tại) ❌ 250ms+
- Singapore 🇸🇬 ✅ 50-80ms (GẦN NHẤT VỚI VN)
- Frankfurt, Germany 🇩🇪 ~180ms

**Cách migrate:**
1. Tạo database mới ở Singapore region
2. Dump data từ Oregon database
3. Restore vào Singapore database
4. Update environment variables

```bash
# 1. Export data
pg_dump $OLD_DATABASE_URL > dump.sql

# 2. Import to new database
psql $NEW_SINGAPORE_DATABASE_URL < dump.sql

# 3. Update render.yaml
DB_HOST = new-singapore-db.render.com
```

### **Option 2: Sử dụng Supabase (Free tier có Singapore)**

```yaml
# Supabase Singapore
DB_HOST: db.xxxxxxxx.supabase.co
DB_PORT: 5432
REGION: ap-southeast-1 (Singapore)
```

### **Option 3: Railway (có Asia regions)**

```yaml
# Railway
REGION: ap-southeast-1
```

## 📊 PERFORMANCE SO SÁNH

| Solution | Latency Improvement | Cost | Effort |
|----------|---------------------|------|--------|
| Redis Cache | **50-90% faster** | Free (Render) | Low ⭐⭐ |
| Connection Pool | **20-30% faster** | Free | Very Low ⭐ |
| Batch Queries | **40-60% faster** | Free | Medium ⭐⭐⭐ |
| Migrate to Singapore | **70-80% faster** | $7+/month | High ⭐⭐⭐⭐ |

## 🎯 KHUYẾN NGHỊ THỰC HIỆN

### **PHASE 1: Immediate (Làm Ngay - 1 giờ)**
1. ✅ Implement Redis caching cho top queries
2. ✅ Add connection pooling
3. ✅ Add CDN cache headers

**Expected:** Giảm latency **50-70%** cho cached queries

### **PHASE 2: Short-term (1-2 ngày)**
1. ✅ Optimize queries (batch, combine)
2. ✅ Add lazy loading
3. ✅ Implement pagination

**Expected:** Giảm thêm **20-30%** cho uncached queries

### **PHASE 3: Long-term (Khi có budget)**
1. ✅ Migrate database sang Singapore
2. ✅ Setup read replicas

**Expected:** Giảm latency xuống **50-80ms**

## 🚀 QUICK START

1. **Add Redis to Render:**
   - Dashboard → New → Redis
   - Free tier: 25MB
   - Copy REDIS_URL

2. **Add to render.yaml:**
```yaml
services:
  - type: redis
    name: ecommerce-dss-redis
    plan: free
    
  - type: web
    name: ecommerce-dss-backend
    envVars:
      - key: REDIS_URL
        fromService:
          name: ecommerce-dss-redis
          type: redis
          property: connectionString
```

3. **Deploy:**
```bash
git add .
git commit -m "Add Redis caching for performance"
git push
```

## 📈 MONITORING

Track improvement:
```python
import time

@app.middleware("http")
async def add_timing_header(request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    response.headers["X-Response-Time"] = f"{duration:.3f}s"
    return response
```

Check headers:
```bash
curl -I https://your-api.onrender.com/api/v1/analytics/overview/kpis
# X-Response-Time: 0.123s  (with cache)
# X-Response-Time: 0.789s  (without cache)
```

---

**TÓM TẮT:** 
- **Ngay:** Implement Redis cache → giảm 50-70% latency
- **Sau:** Migrate sang Singapore → giảm thêm 70-80%
- **Total:** Từ **250ms** → **30-50ms** ✅

