# DSS System Optimization - Deployment Guide

This guide details the steps to deploy the performance optimizations for the DSS system (Decision Support System).

## 1. Database Migrations (Priority 1 & 2)

These migrations create essential indexes and the `product_metrics_global` materialized table.

### Step 1.1: Create Performance Indexes
Run the following SQL script to create indexes on `ml.fact_product_recommendation`, `dwh.dim_product`, and `dwh.fact_product_daily`.

**File:** `backend/migrations/create_performance_indexes.sql`

```bash
# Example using psql
psql $DATABASE_URL -f backend/migrations/create_performance_indexes.sql
```

**Verification:**
```sql
SELECT indexname FROM pg_indexes WHERE tablename = 'fact_product_recommendation';
-- Should list: idx_fact_product_reco_src_sim_rank, idx_fact_product_reco_sim_desc
```

### Step 1.2: Create Product Metrics Table
Run the SQL script to create the `dwh.product_metrics_global` table and its refresh function.

**File:** `backend/migrations/create_product_metrics_table.sql`

```bash
psql $DATABASE_URL -f backend/migrations/create_product_metrics_table.sql
```

**Verification:**
```sql
SELECT count(*) FROM dwh.product_metrics_global;
-- Should return > 0 (if fact_product_daily has data)
```

---

## 2. Backend Code Deployment

Deploy the updated `backend` code. Key changes include:
- **`app/services/dss_service.py`**: Updated to use `product_metrics_global` and Redis caching.
- **`app/core/cache.py`**: Updated to use `redis.asyncio`.
- **`main.py`**: Updated to initialize Redis cache on startup.

**Environment Variables:**
Ensure `REDIS_URL` is set in your environment (e.g., Render dashboard).
```
REDIS_URL=redis://default:PASSWORD@redis-cloud-url:port
```

---

## 3. Airflow DAG Deployment

Deploy the new DAG to refresh product metrics daily.

**File:** `airflow/dags/refresh_product_metrics_dag.py`

1. Copy the file to your Airflow `dags/` folder.
2. Ensure Airflow has a connection ID `postgres_dwh` pointing to your DWH database.
3. Enable the DAG `refresh_product_metrics_global` in the Airflow UI.

**Schedule:** Runs daily at 2:00 AM.

---

## 4. Rollback Plan

If critical issues arise:

1. **Revert Code:** Rollback backend to the previous commit (disables caching & new queries).
2. **Disable DAG:** Turn off `refresh_product_metrics_global` in Airflow.
3. **Drop Indexes (Optional):** If indexes cause write performance issues:
   ```sql
   DROP INDEX CONCURRENTLY ml.idx_fact_product_reco_src_sim_rank;
   DROP INDEX CONCURRENTLY ml.idx_fact_product_reco_sim_desc;
   -- ... (see create_performance_indexes.sql for full list)
   ```
