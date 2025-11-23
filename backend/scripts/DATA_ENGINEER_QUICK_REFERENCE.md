# Data Engineer API - Quick Reference Card

## 🚀 Quick Start Commands

### Setup

```bash
# 1. Apply extended schema
psql "$DATABASE_URL" -f database/schema/meta_schema_extended.sql

# 2. Collect initial metrics
python backend/scripts/collect_metadata_metrics.py

# 3. Test API
curl http://localhost:8000/api/v1/data-engineer/health
```

---

## 📊 Common API Calls

### Check Pipeline Status

```bash
# All jobs
curl http://localhost:8000/api/v1/data-engineer/etl/jobs | jq

# Specific job history
curl http://localhost:8000/api/v1/data-engineer/etl/runs/MINIO_ECOMMERCE_DWH_PIPELINE | jq

# Recent logs
curl http://localhost:8000/api/v1/data-engineer/etl/logs/123 | jq
```

### Monitor Table Health

```bash
# All tables
curl http://localhost:8000/api/v1/data-engineer/tables/health | jq

# DWH tables only
curl "http://localhost:8000/api/v1/data-engineer/tables/health?schema_name=dwh" | jq

# Table growth
curl "http://localhost:8000/api/v1/data-engineer/tables/growth/dwh/fact_product_daily?days=7" | jq
```

### Check Data Quality

```bash
# Open issues
curl "http://localhost:8000/api/v1/data-engineer/data-quality/issues?status=OPEN" | jq

# Critical issues
curl "http://localhost:8000/api/v1/data-engineer/data-quality/issues?severity=CRITICAL" | jq

# Summary
curl http://localhost:8000/api/v1/data-engineer/data-quality/summary | jq
```

### Database Health

```bash
curl http://localhost:8000/api/v1/data-engineer/database/health | jq
```

### Data Lineage

```bash
# Upstream sources
curl "http://localhost:8000/api/v1/data-engineer/lineage/table/dwh/fact_product_daily?direction=upstream" | jq

# Downstream dependents
curl "http://localhost:8000/api/v1/data-engineer/lineage/table/dwh/fact_product_daily?direction=downstream" | jq

# Both directions
curl "http://localhost:8000/api/v1/data-engineer/lineage/table/dwh/fact_product_daily?direction=both" | jq
```

### Alerts

```bash
# Summary
curl http://localhost:8000/api/v1/data-engineer/alerts/summary | jq

# Last 24 hours
curl "http://localhost:8000/api/v1/data-engineer/alerts/history?hours=24" | jq
```

### Statistics

```bash
# Pipeline performance (last 7 days)
curl "http://localhost:8000/api/v1/data-engineer/stats/pipeline-performance?days=7" | jq

# Data volume trends
curl "http://localhost:8000/api/v1/data-engineer/stats/data-volume?days=30" | jq
```

---

## 🔍 Common SQL Queries

### Check ETL Status

```sql
-- Latest runs
SELECT 
    j.job_code,
    r.run_date,
    r.status,
    r.started_at,
    EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60 as duration_minutes,
    r.rows_written
FROM meta.etl_run r
JOIN meta.etl_job j ON r.job_id = j.job_id
ORDER BY r.started_at DESC
LIMIT 10;

-- Success rate by job
SELECT 
    j.job_code,
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE r.status = 'SUCCESS') as successes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE r.status = 'SUCCESS') / COUNT(*), 1) as success_rate_pct
FROM meta.etl_run r
JOIN meta.etl_job j ON r.job_id = j.job_id
WHERE r.started_at >= NOW() - INTERVAL '30 days'
GROUP BY j.job_code;
```

### Check Table Health

```sql
-- Latest table stats
SELECT 
    schema_name,
    table_name,
    row_count,
    ROUND(size_bytes / 1024.0 / 1024.0, 2) as size_mb,
    last_loaded_at,
    EXTRACT(EPOCH FROM (NOW() - last_loaded_at))/3600 as age_hours
FROM meta.table_stats
WHERE snapshot_date = CURRENT_DATE
ORDER BY size_bytes DESC;

-- Growth over time
SELECT 
    snapshot_date,
    schema_name,
    table_name,
    row_count,
    ROUND(size_bytes / 1024.0 / 1024.0, 2) as size_mb
FROM meta.table_stats
WHERE table_name = 'fact_product_daily'
ORDER BY snapshot_date DESC
LIMIT 30;
```

### Check Data Quality

```sql
-- Open issues
SELECT 
    schema_name || '.' || table_name as table_full,
    issue_type,
    severity,
    affected_rows,
    issue_description,
    detected_at
FROM meta.data_quality_issue
WHERE status = 'OPEN'
ORDER BY severity, detected_at DESC;

-- Issue summary
SELECT 
    status,
    severity,
    COUNT(*) as count
FROM meta.data_quality_issue
GROUP BY status, severity
ORDER BY severity, status;
```

### Check Database Health

```sql
-- Latest health
SELECT 
    check_time,
    status,
    active_connections,
    connection_usage_pct,
    slow_queries_count
FROM meta.db_connection_health
ORDER BY check_time DESC
LIMIT 1;

-- Connection trends
SELECT 
    DATE_TRUNC('hour', check_time) as hour,
    AVG(active_connections) as avg_active,
    MAX(connection_usage_pct) as max_usage_pct
FROM meta.db_connection_health
WHERE check_time >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', check_time)
ORDER BY hour DESC;
```

### Data Lineage

```sql
-- Full lineage for a table
WITH RECURSIVE lineage AS (
    -- Start with target table
    SELECT 
        source_schema, source_table,
        target_schema, target_table,
        transformation_type, job_code,
        1 as level
    FROM meta.data_lineage
    WHERE target_schema = 'dwh' AND target_table = 'fact_product_daily'
    
    UNION ALL
    
    -- Recurse upstream
    SELECT 
        dl.source_schema, dl.source_table,
        dl.target_schema, dl.target_table,
        dl.transformation_type, dl.job_code,
        l.level + 1
    FROM meta.data_lineage dl
    JOIN lineage l ON dl.target_schema = l.source_schema 
                   AND dl.target_table = l.source_table
    WHERE l.level < 5
)
SELECT * FROM lineage ORDER BY level;
```

---

## 🛠️ Maintenance Tasks

### Cleanup Old Data

```sql
-- Delete old metrics (keep 90 days)
DELETE FROM meta.db_connection_health WHERE check_time < NOW() - INTERVAL '90 days';
DELETE FROM meta.storage_usage WHERE check_time < NOW() - INTERVAL '90 days';
DELETE FROM meta.query_performance WHERE executed_at < NOW() - INTERVAL '90 days';

-- Archive old ETL logs
DELETE FROM meta.etl_log WHERE created_at < NOW() - INTERVAL '180 days';
```

### Vacuum & Analyze

```sql
VACUUM ANALYZE meta.etl_run;
VACUUM ANALYZE meta.etl_log;
VACUUM ANALYZE meta.table_stats;
```

### Update Statistics

```bash
python backend/scripts/collect_metadata_metrics.py
```

---

## 📈 Dashboard URLs

- **API Docs:** http://localhost:8000/docs
- **Data Engineer Dashboard:** http://localhost:8000/dashboard/data-engineer
- **Airflow UI:** http://localhost:8081

---

## 🚨 Troubleshooting

### API not responding

```bash
# Check backend status
docker ps | grep backend

# Check logs
docker logs ecommerce-dss-project-backend-1

# Restart
docker-compose restart backend
```

### No metrics data

```bash
# Run collector manually
python backend/scripts/collect_metadata_metrics.py

# Check tables
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM meta.table_stats;"
```

### Stale data alerts

```sql
-- Check freshness
SELECT 
    schema_name,
    table_name,
    last_loaded_at,
    EXTRACT(EPOCH FROM (NOW() - last_loaded_at))/3600 as age_hours
FROM meta.table_stats
WHERE snapshot_date = CURRENT_DATE
  AND last_loaded_at < NOW() - INTERVAL '24 hours';

-- Trigger ETL to refresh
# Go to Airflow UI and manually trigger the DAG
```

---

## 📞 Quick Contact

For issues or questions:
- Check logs: `/var/log/metadata_collector.log`
- Review Airflow logs: Airflow UI > DAG > Task Logs
- API errors: `docker logs backend`

---

**Last Updated:** 2025-11-24

