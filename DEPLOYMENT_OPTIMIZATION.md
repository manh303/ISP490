# Database Optimization Deployment Guide

## Overview

This guide describes how to deploy the database performance optimizations for the DSS system to the production environment (Render).

## Prerequisites

- Access to Render PostgreSQL database
- PostgreSQL client (`psql`) or database management tool
- Airflow access for DAG deployment
- Backend code deployment access

## Deployment Steps

### Step 1: Apply Index Migration

**File**: `backend/migrations/create_performance_indexes.sql`

**When to run**: During low-traffic period (recommended: after midnight)

**Execution**:
```bash
# Connect to Render PostgreSQL database
psql $DATABASE_URL -f backend/migrations/create_performance_indexes.sql
```

**Expected duration**: 5-10 minutes (depends on table size)

**Verification**:
```sql
-- Verify indexes exist
SELECT schemaname,  tablename, indexname, indexdef 
FROM pg_indexes 
WHERE schemaname IN ('ml', 'dwh')
ORDER BY tablename, indexname;

-- Check index sizes
SELECT 
    schemaname, 
    tablename, 
    indexname, 
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname IN ('ml', 'dwh')
ORDER BY pg_relation_size(indexrelid) DESC;
```

**Rollback** (if needed):
```sql
DROP INDEX CONCURRENTLY IF EXISTS ml.idx_fact_product_reco_src_sim_rank;
DROP INDEX CONCURRENTLY IF EXISTS ml.idx_fact_product_reco_sim_desc;
DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_product_product_key;
DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_product_platform_category;
DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_fact_product_daily_date_product;
DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_fact_product_daily_metrics;
DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_date_date_value;
DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_platform_code;
```

---

### Step 2: Create Product Metrics Table

**File**: `backend/migrations/create_product_metrics_table.sql`

**Execution**:
```bash
psql $DATABASE_URL -f backend/migrations/create_product_metrics_table.sql
```

**Expected duration**: 2-5 minutes (includes initial population)

**Verification**:
```sql
-- Check table exists and has data
SELECT 
    COUNT(*) as total_products,
    MAX(last_updated) as last_refresh,
    AVG(data_freshness_hours) as avg_freshness_hours
FROM dwh.product_metrics_global;

-- Sample records
SELECT * FROM dwh.product_metrics_global LIMIT 10;

-- Test refresh function
SELECT * FROM dwh.refresh_product_metrics_global();
```

**Rollback** (if needed):
```sql
DROP TABLE IF EXISTS dwh.product_metrics_global CASCADE;
DROP FUNCTION IF EXISTS dwh.refresh_product_metrics_global();
```

---

### Step 3: Deploy Updated Backend Code

**Files changed**:
- `backend/app/services/dss_service.py`

**Changes**:
- Replaced `product_metrics` CTE with `product_metrics_global` table JOIN
- Added `dim_platform` JOIN to remove SUBSTRING calls
- Increased cache TTL from 1800s to 3600s
- Removed date parameters from recommendation queries

**Deployment**:
```bash
# Commit and push changes
git add backend/app/services/dss_service.py
git commit -m "feat: optimize DSS queries with materialized product metrics"
git push origin main

# Render will auto-deploy or trigger manual deploy
```

**Testing after deployment**:
```bash
# Test recommendation endpoint
curl -X POST "https://your-app.onrender.com/api/v1/dss/reco/run" \
  -H "Content-Type: application/json" \
  -d '{
    "scope_mode": "by_category",
    "platforms": ["tiki"],
    "categories": ["1"],
    "top_k": 10,
    "from_date": "2024-11-01",
    "to_date": "2024-11-30"
  }'

# Check response time (should be < 2 seconds)
```

---

### Step 4: Deploy Airflow DAG

**File**: `airflow/dags/refresh_product_metrics_dag.py`

**Execution**:
```bash
# Copy DAG file to Airflow dags folder
cp airflow/dags/refresh_product_metrics_dag.py /path/to/airflow/dags/

# Or commit and sync if using Git-sync
git add airflow/dags/refresh_product_metrics_dag.py
git commit -m "feat: add product metrics refresh DAG"
git push origin main
```

**Configuration**:

Ensure the `postgres_dwh` connection exists in Airflow:
1. Go to Airflow UI > Admin > Connections
2. Add/edit `postgres_dwh` connection with Render PostgreSQL credentials

**Verification**:
```bash
# Check DAG appears in Airflow UI
airflow dags list | grep refresh_product_metrics

# Trigger manual test run
airflow dags test refresh_product_metrics_global $(date +%Y-%m-%d)

# Enable DAG for scheduled runs
airflow dags unpause refresh_product_metrics_global
```

---

## Configuration

### Environment Variables

No new environment variables required. Existing configuration works.

### Redis Cache (Optional - Phase 3)

If Redis is available:
- Cache is automatically enabled via existing `REDIS_URL` environment variable
- No code changes needed - caching logic already implemented
- Increased TTL (3600s) will improve hit rate

If Redis is not available:
- System falls back to in-memory cache (limited to 100 items)
- Still get some caching benefit for repeated requests

---

## Monitoring and Validation

### Performance Metrics to Monitor

After deployment, monitor these metrics:

**1. Query Performance**
```sql
-- Check slow queries
SELECT 
    query,
    mean_exec_time,
    calls
FROM pg_stat_statements
WHERE query LIKE '%fact_product_recommendation%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

**2. Cache Hit Rate**

Check application logs for cache hit/miss ratio:
```bash
grep "Cache HIT" /var/log/app.log | wc -l
grep "Cache MISS" /var/log/app.log | wc -l
```

**3. API Response Times**

Benchmark DSS endpoints before and after:
```bash
# Before optimization (expected: 5-15 seconds)
time curl -X POST .../dss/reco/run ...

# After optimization (expected: <2 seconds)
```

**4. Product Metrics Freshness**
```sql
SELECT 
    MAX(last_updated) as last_refresh,
    AVG(data_freshness_hours) as avg_freshness
FROM dwh.product_metrics_global;
```

Expected: `last_refresh` within last 24 hours, `avg_freshness` < 48 hours

---

## Troubleshooting

### Issue: Indexes taking too long to create

**Solution**: Use `CREATE INDEX CONCURRENTLY` to avoid locking:
```sql
CREATE INDEX CONCURRENTLY idx_name ON table(columns);
```

### Issue: Product metrics table empty after refresh

**Check**:
1. Verify `fact_product_daily` has data:
   ```sql
   SELECT COUNT(*), MAX(date_sk) FROM dwh.fact_product_daily;
   ```
2. Check `dim_date` has recent dates:
   ```sql
   SELECT * FROM dwh.dim_date WHERE date_value >= CURRENT_DATE - 30 ORDER BY date_value DESC;
   ```
3. Check refresh function logs in Airflow

### Issue: DSS queries return empty results after deployment

**Check**:
1. Verify `product_metrics_global` is populated
2. Check if table has correct data:
   ```sql
   SELECT * FROM dwh.product_metrics_global WHERE avg_price > 0 LIMIT 10;
   ```
3. If empty, manually run refresh function:
   ```sql
   SELECT * FROM dwh.refresh_product_metrics_global();
   ```

### Issue: High memory usage after optimization

**Cause**: Possibly caching too many large results

**Solution**: Reduce cache TTL or limit cached result size:
```python
# In dss_service.py, adjust TTL
cache.set(cache_key, result, ttl=1800)  # Reduce from 3600 to 1800
```

---

## Expected Performance Improvements

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Recommendation by_product query | 5-8 sec | <1 sec | 85%+ faster |
| Recommendation by_category query | 8-15 sec | <2 sec | 85%+ faster |
| Price prediction query | 10-20 sec | 2-4 sec | 80%+ faster |
| Cache hit (2nd request) | N/A | <100 ms | 95%+ faster |

---

## Maintenance Schedule

- **Daily 2:00 AM**: Automatic refresh of `product_metrics_global` via Airflow DAG
- **Weekly**: Review slow query logs and optimize as needed
- **Monthly**: Review cache hit rates and adjust TTL if necessary
- **Quarterly**: ANALYZE tables and REINDEX if fragmentation occurs

---

## Rollback Plan

If issues occur after deployment:

**Step 1**: Revert backend code
```bash
git revert <commit-hash>
git push origin main
```

**Step 2**: Disable Airflow DAG
```bash
airflow dags pause refresh_product_metrics_global
```

**Step 3**: Drop indexes (optional, if causing issues)
```sql
-- See rollback SQL in Step 1 above
```

**Step 4**: Drop product_metrics_global (optional)
```sql
-- See rollback SQL in Step 2 above
```

System will fall back to previous CTE-based queries.

---

## Support

For issues or questions:
- Check Airflow DAG logs: Airflow UI > DAGs > refresh_product_metrics_global > Logs
- Check application logs: Render Dashboard > Logs
- Review database slow queries: `pg_stat_statements` view

