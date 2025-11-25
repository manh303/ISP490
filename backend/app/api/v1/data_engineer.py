# backend/app/api/v1/data_engineer.py
"""
Data Engineer API - Monitoring & Operations
Provides endpoints for ETL monitoring, data quality, and system health

IMPROVEMENTS:
- Connection pooling for better performance
- Comprehensive error handling & logging
- SQL injection prevention
- Input validation
- Connection timeout & retry mechanism
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from pydantic import BaseModel, Field
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import os
import logging
import time

# Setup logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-engineer", tags=["Data Engineer"])

# ===================================================================
# CONNECTION POOL (THREAD-SAFE)
# ===================================================================

class DatabasePool:
    """Thread-safe connection pool with retry mechanism"""
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
        return cls._instance
    
    def initialize(self):
        """Initialize connection pool"""
        if self._pool is None:
            db_url = os.getenv("DATABASE_URL")
            if not db_url:
                raise ValueError("DATABASE_URL not configured")
            
            try:
                self._pool = pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=10,
                    dsn=db_url,
                    connect_timeout=10
                )
                logger.info("✅ Database connection pool initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize connection pool: {e}")
                raise
    
    @contextmanager
    def get_connection(self, max_retries=3):
        """Get connection from pool with retry mechanism"""
        if self._pool is None:
            self.initialize()
        
        conn = None
        retries = 0
        last_error = None
        
        while retries < max_retries:
            try:
                conn = self._pool.getconn()
                if conn:
                    yield conn
                    return
            except pool.PoolError as e:
                last_error = e
                retries += 1
                logger.warning(f"Connection pool error (attempt {retries}/{max_retries}): {e}")
                time.sleep(0.5 * retries)  # Exponential backoff
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error getting connection: {e}")
                break
            finally:
                if conn:
                    try:
                        self._pool.putconn(conn)
                    except Exception as e:
                        logger.error(f"Error returning connection to pool: {e}")
        
        # If we get here, all retries failed
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed after {max_retries} retries: {str(last_error)}"
        )
    
    def close_all(self):
        """Close all connections in pool"""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")

# Initialize global pool
db_pool = DatabasePool()

def get_db_conn():
    """Get database connection from pool (context manager)"""
    return db_pool.get_connection()

# ===================================================================
# MODELS
# ===================================================================

class ETLJobStatus(BaseModel):
    job_code: str
    job_name: str
    is_active: bool
    last_run_date: Optional[date]
    last_run_status: Optional[str]
    last_run_duration_minutes: Optional[float]
    total_runs: int
    success_rate: float
    avg_duration_minutes: Optional[float]

class ETLRunDetail(BaseModel):
    run_id: int
    job_code: str
    run_date: date
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    rows_read: Optional[int]
    rows_written: Optional[int]
    duration_minutes: Optional[float]
    error_message: Optional[str]
    airflow_run_id: Optional[str]

class TableHealth(BaseModel):
    schema_name: str
    table_name: str
    row_count: int
    size_mb: float
    last_loaded_at: Optional[datetime]
    freshness_hours: Optional[float]
    health_status: str  # 'HEALTHY', 'STALE', 'EMPTY', 'DEGRADED'

class DataQualityIssue(BaseModel):
    issue_id: int
    schema_name: str
    table_name: str
    issue_type: str
    severity: str
    status: str
    affected_rows: int
    issue_description: str
    detected_at: datetime

class DatabaseHealth(BaseModel):
    status: str
    active_connections: int
    idle_connections: int
    max_connections: int
    connection_usage_pct: float
    avg_query_time_ms: Optional[float]
    slow_queries_count: int
    check_time: datetime

class DataLineageNode(BaseModel):
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    transformation_type: str
    job_code: Optional[str]

class AlertSummary(BaseModel):
    alert_name: str
    alert_type: str
    severity: str
    target_name: str
    triggered_count_24h: int
    last_triggered_at: Optional[datetime]
    status: str

# ===================================================================
# ENDPOINTS
# ===================================================================

@router.get("/health", summary="API Health Check")
async def health_check():
    """Simple health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}

@router.get("/dashboard/summary", summary="Get Dashboard Summary")
async def get_dashboard_summary():
    """
    Get aggregated dashboard data in single API call
    Optimized for dashboard overview - combines multiple endpoints
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. ETL Overview
                cur.execute("""
                    WITH recent_runs AS (
                        SELECT DISTINCT ON (job_id)
                            job_id, status, finished_at, started_at
                        FROM meta.etl_run
                        WHERE started_at >= NOW() - INTERVAL '24 hours'
                        ORDER BY job_id, started_at DESC
                    )
                    SELECT 
                        COUNT(DISTINCT j.job_id) as total_jobs,
                        COUNT(r.run_id) FILTER (WHERE r.status = 'SUCCESS') as successful_runs_24h,
                        COUNT(r.run_id) FILTER (WHERE r.status = 'FAILED') as failed_runs_24h,
                        AVG(EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60) as avg_duration_minutes,
                        COUNT(rr.job_id) FILTER (WHERE rr.status = 'RUNNING') as currently_running
                    FROM meta.etl_job j
                    LEFT JOIN meta.etl_run r ON j.job_id = r.job_id 
                        AND r.started_at >= NOW() - INTERVAL '24 hours'
                    LEFT JOIN recent_runs rr ON j.job_id = rr.job_id;
                """)
                etl_overview = cur.fetchone()
                
                # 2. Table Health Summary
                cur.execute("""
                    WITH latest_stats AS (
                        SELECT DISTINCT ON (schema_name, table_name)
                            schema_name, table_name, row_count, size_bytes, last_loaded_at
                        FROM meta.table_stats
                        WHERE snapshot_date >= CURRENT_DATE - 7
                        ORDER BY schema_name, table_name, snapshot_date DESC
                    )
                    SELECT 
                        COUNT(*) as total_tables,
                        SUM(row_count) as total_rows,
                        ROUND(SUM(size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) as total_size_gb,
                        COUNT(*) FILTER (
                            WHERE last_loaded_at IS NOT NULL 
                            AND EXTRACT(EPOCH FROM (NOW() - last_loaded_at))/3600 > 24
                        ) as stale_tables
                    FROM latest_stats;
                """)
                table_summary = cur.fetchone()
                
                # 3. Data Quality Summary
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_issues,
                        COUNT(*) FILTER (WHERE severity = 'CRITICAL') as critical_issues,
                        COUNT(*) FILTER (WHERE severity = 'HIGH') as high_issues,
                        COUNT(*) FILTER (WHERE severity = 'MEDIUM') as medium_issues,
                        COUNT(*) FILTER (WHERE status = 'OPEN') as open_issues
                    FROM meta.data_quality_issue
                    WHERE detected_at >= NOW() - INTERVAL '30 days';
                """)
                dq_summary = cur.fetchone()
                
                # 4. Database Health
                cur.execute("""
                    SELECT status, connection_usage_pct, slow_queries_count
                    FROM meta.db_connection_health
                    ORDER BY check_time DESC
                    LIMIT 1;
                """)
                db_health = cur.fetchone() or {
                    'status': 'UNKNOWN', 
                    'connection_usage_pct': 0, 
                    'slow_queries_count': 0
                }
                
                # 5. Recent Activity (last 5 runs)
                cur.execute("""
                    SELECT 
                        r.run_id,
                        j.job_code,
                        r.status,
                        r.started_at,
                        r.finished_at,
                        EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60 as duration_minutes
                    FROM meta.etl_run r
                    JOIN meta.etl_job j ON r.job_id = j.job_id
                    WHERE r.started_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY r.started_at DESC
                    LIMIT 5;
                """)
                recent_activity = cur.fetchall()
                
                # 6. Alert Count
                cur.execute("""
                    SELECT COUNT(*) as alert_count_24h
                    FROM meta.alert_history
                    WHERE triggered_at >= NOW() - INTERVAL '24 hours'
                      AND status != 'RESOLVED';
                """)
                alert_count = cur.fetchone()['alert_count_24h'] or 0
                
                # Combine all data
                return {
                    "timestamp": datetime.now(),
                    "overview": {
                        "etl": {
                            "total_jobs": etl_overview['total_jobs'] or 0,
                            "successful_runs_24h": etl_overview['successful_runs_24h'] or 0,
                            "failed_runs_24h": etl_overview['failed_runs_24h'] or 0,
                            "currently_running": etl_overview['currently_running'] or 0,
                            "avg_duration_minutes": round(etl_overview['avg_duration_minutes'] or 0, 2)
                        },
                        "tables": {
                            "total_tables": table_summary['total_tables'] or 0,
                            "total_rows": table_summary['total_rows'] or 0,
                            "total_size_gb": float(table_summary['total_size_gb'] or 0),
                            "stale_tables": table_summary['stale_tables'] or 0
                        },
                        "data_quality": {
                            "total_issues": dq_summary['total_issues'] or 0,
                            "critical_issues": dq_summary['critical_issues'] or 0,
                            "high_issues": dq_summary['high_issues'] or 0,
                            "medium_issues": dq_summary['medium_issues'] or 0,
                            "open_issues": dq_summary['open_issues'] or 0
                        },
                        "database": {
                            "status": db_health['status'],
                            "connection_usage_pct": round(db_health['connection_usage_pct'] or 0, 2),
                            "slow_queries": db_health['slow_queries_count'] or 0
                        },
                        "alerts": {
                            "active_alerts_24h": alert_count
                        }
                    },
                    "recent_activity": recent_activity
                }
                
    except psycopg2.Error as e:
        logger.error(f"Database error in get_dashboard_summary: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_dashboard_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# ETL MONITORING
# ===================================================================

@router.get("/etl/jobs", response_model=List[ETLJobStatus], summary="Get All ETL Jobs Status")
async def get_etl_jobs_status():
    """
    Get status of all ETL jobs with recent run history
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    WITH recent_runs AS (
                    SELECT DISTINCT ON (job_id) 
                        job_id,
                        run_date as last_run_date,
                        started_at as last_run_started,
                        finished_at as last_run_finished,
                        status as last_run_status
                    FROM meta.etl_run
                    ORDER BY job_id, started_at DESC
                ),
                stats AS (
                    SELECT 
                        job_id,
                        COUNT(*) as total_runs,
                        AVG(EXTRACT(EPOCH FROM (finished_at - started_at))/60) as avg_duration_minutes,
                        100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) as success_rate
                    FROM meta.etl_run
                    WHERE started_at >= NOW() - INTERVAL '30 days'
                    GROUP BY job_id
                )
                SELECT 
                    j.job_code,
                    j.job_name,
                    j.is_active,
                    r.last_run_date,
                    r.last_run_status,
                    EXTRACT(EPOCH FROM (r.last_run_finished - r.last_run_started))/60 as last_run_duration_minutes,
                    COALESCE(s.total_runs, 0) as total_runs,
                    COALESCE(s.success_rate, 0) as success_rate,
                    s.avg_duration_minutes
                FROM meta.etl_job j
                LEFT JOIN recent_runs r ON j.job_id = r.job_id
                LEFT JOIN stats s ON j.job_id = s.job_id
                    ORDER BY j.job_code;
                """)
                rows = cur.fetchall()
                return [ETLJobStatus(**row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_etl_jobs_status: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_etl_jobs_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/etl/runs/{job_code}", response_model=List[ETLRunDetail], summary="Get ETL Run History")
async def get_etl_run_history(
    job_code: str,
    limit: int = Query(default=20, le=100, ge=1),
    status: Optional[str] = None
):
    """
    Get run history for a specific ETL job
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    SELECT 
                        r.run_id,
                        j.job_code,
                        r.run_date,
                        r.started_at,
                        r.finished_at,
                        r.status,
                        r.rows_read,
                        r.rows_written,
                        EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60 as duration_minutes,
                        r.error_message,
                        r.airflow_run_id
                    FROM meta.etl_run r
                    JOIN meta.etl_job j ON r.job_id = j.job_id
                    WHERE j.job_code = %s
                """
                params = [job_code]
                
                if status:
                    query += " AND r.status = %s"
                    params.append(status)
                
                query += " ORDER BY r.started_at DESC LIMIT %s"
                params.append(limit)
                
                cur.execute(query, params)
                rows = cur.fetchall()
                return [ETLRunDetail(**row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_etl_run_history: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_etl_run_history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/etl/logs/{run_id}", summary="Get ETL Run Logs")
async def get_etl_run_logs(run_id: int):
    """
    Get detailed logs for a specific ETL run
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        log_id,
                        job_name,
                        stage,
                        log_level,
                        log_message,
                        records_processed,
                        records_failed,
                        error_message,
                        created_at
                    FROM meta.etl_log
                    WHERE run_id = %s
                    ORDER BY created_at ASC;
                """, (run_id,))
                return cur.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Database error in get_etl_run_logs: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_etl_run_logs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# TABLE HEALTH
# ===================================================================

@router.get("/tables/health", response_model=List[TableHealth], summary="Get Table Health Status")
async def get_table_health(
    schema_name: Optional[str] = None,
    stale_hours: int = Query(default=24, ge=1, le=720, description="Hours to consider data stale")
):
    """
    Get health status of all tables (row count, size, freshness)
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    WITH latest_stats AS (
                    SELECT DISTINCT ON (schema_name, table_name)
                        schema_name,
                        table_name,
                        row_count,
                        size_bytes,
                        last_loaded_at
                    FROM meta.table_stats
                    ORDER BY schema_name, table_name, snapshot_date DESC
                )
                SELECT 
                    schema_name,
                    table_name,
                    row_count,
                    ROUND(size_bytes / 1024.0 / 1024.0, 2) as size_mb,
                    last_loaded_at,
                    EXTRACT(EPOCH FROM (NOW() - last_loaded_at))/3600 as freshness_hours,
                    CASE 
                        WHEN last_loaded_at IS NULL THEN 'EMPTY'
                        WHEN EXTRACT(EPOCH FROM (NOW() - last_loaded_at))/3600 > %s THEN 'STALE'
                        WHEN row_count = 0 THEN 'EMPTY'
                        ELSE 'HEALTHY'
                        END as health_status
                    FROM latest_stats
                """
                
                params = [stale_hours]
                
                if schema_name:
                    query += " WHERE schema_name = %s"
                    params.append(schema_name)
                
                query += " ORDER BY schema_name, table_name"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                return [TableHealth(**row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_table_health: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_table_health: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/tables/growth/{schema_name}/{table_name}", summary="Get Table Growth History")
async def get_table_growth(
    schema_name: str, 
    table_name: str, 
    days: int = Query(default=30, ge=1, le=365)
):
    """
    Get row count and size growth over time
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        snapshot_date,
                        row_count,
                        ROUND(size_bytes / 1024.0 / 1024.0, 2) as size_mb,
                        ROUND((size_bytes / 1024.0 / 1024.0) / NULLIF(row_count, 0), 4) as avg_row_size_kb
                    FROM meta.table_stats
                    WHERE schema_name = %s 
                      AND table_name = %s
                      AND snapshot_date >= CURRENT_DATE - %s
                    ORDER BY snapshot_date DESC
                """, (schema_name, table_name, days))
                return cur.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Database error in get_table_growth: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_table_growth: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# DATA QUALITY
# ===================================================================

@router.get("/data-quality/issues", response_model=List[DataQualityIssue], summary="Get Data Quality Issues")
async def get_data_quality_issues(
    status: str = Query(default="OPEN", description="OPEN, IN_PROGRESS, RESOLVED, IGNORED"),
    severity: Optional[str] = None,
    schema_name: Optional[str] = None
):
    """
    Get data quality issues filtered by status, severity, schema
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    SELECT 
                        issue_id,
                        schema_name,
                        table_name,
                        issue_type,
                        severity,
                        status,
                        affected_rows,
                        issue_description,
                        detected_at
                    FROM meta.data_quality_issue
                    WHERE status = %s
                """
                params = [status]
                
                if severity:
                    query += " AND severity = %s"
                    params.append(severity)
                
                if schema_name:
                    query += " AND schema_name = %s"
                    params.append(schema_name)
                
                query += " ORDER BY severity DESC, detected_at DESC LIMIT 100"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                return [DataQualityIssue(**row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_data_quality_issues: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_data_quality_issues: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/data-quality/summary", summary="Get Data Quality Summary")
async def get_data_quality_summary():
    """
    Get summary statistics of data quality issues
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        status,
                        severity,
                        COUNT(*) as issue_count,
                        SUM(affected_rows) as total_affected_rows
                    FROM meta.data_quality_issue
                    GROUP BY status, severity
                    ORDER BY severity, status;
                """)
                return cur.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Database error in get_data_quality_summary: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_data_quality_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# DATABASE HEALTH
# ===================================================================

@router.get("/database/health", response_model=DatabaseHealth, summary="Get Database Health")
async def get_database_health():
    """
    Get current database connection health and performance metrics
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get latest health check or perform new one
                cur.execute("""
                    SELECT 
                        check_time,
                        status,
                        active_connections,
                        idle_connections,
                        max_connections,
                        connection_usage_pct,
                        avg_query_time_ms,
                        slow_queries_count
                    FROM meta.db_connection_health
                    WHERE check_time >= NOW() - INTERVAL '5 minutes'
                    ORDER BY check_time DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
                
                if row:
                    return DatabaseHealth(**row)
                
                # If no recent check, return a basic health status
                # (In production, you'd trigger a health check here)
                return DatabaseHealth(
                    status="UNKNOWN",
                    active_connections=0,
                    idle_connections=0,
                    max_connections=100,
                    connection_usage_pct=0.0,
                    avg_query_time_ms=None,
                    slow_queries_count=0,
                    check_time=datetime.now()
                )
    except psycopg2.Error as e:
        logger.error(f"Database error in get_database_health: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_database_health: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# DATA LINEAGE
# ===================================================================

@router.get("/lineage/table/{schema_name}/{table_name}", response_model=List[DataLineageNode], summary="Get Table Lineage")
async def get_table_lineage(
    schema_name: str, 
    table_name: str, 
    direction: str = Query(default="both", pattern="^(upstream|downstream|both)$")
):
    """
    Get data lineage for a specific table
    - upstream: Show source tables
    - downstream: Show dependent tables
    - both: Show both directions
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if direction in ["upstream", "both"]:
                    # Get upstream sources
                    cur.execute("""
                        SELECT 
                            source_schema,
                            source_table,
                            target_schema,
                            target_table,
                            transformation_type,
                            job_code
                        FROM meta.data_lineage
                        WHERE target_schema = %s AND target_table = %s
                          AND is_active = TRUE
                    """, (schema_name, table_name))
                    upstream = cur.fetchall()
                else:
                    upstream = []
                
                if direction in ["downstream", "both"]:
                    # Get downstream dependents
                    cur.execute("""
                        SELECT 
                            source_schema,
                            source_table,
                            target_schema,
                            target_table,
                            transformation_type,
                            job_code
                        FROM meta.data_lineage
                        WHERE source_schema = %s AND source_table = %s
                          AND is_active = TRUE
                    """, (schema_name, table_name))
                    downstream = cur.fetchall()
                else:
                    downstream = []
                
                all_lineage = upstream + downstream
                return [DataLineageNode(**row) for row in all_lineage]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_table_lineage: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_table_lineage: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# ALERTS
# ===================================================================

@router.get("/alerts/summary", response_model=List[AlertSummary], summary="Get Alert Summary")
async def get_alert_summary():
    """
    Get summary of all configured alerts and their recent trigger counts
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        ac.alert_name,
                        ac.alert_type,
                        ac.severity,
                        ac.target_name,
                        COUNT(CASE WHEN ah.triggered_at >= NOW() - INTERVAL '24 hours' THEN ah.alert_history_id END) as triggered_count_24h,
                        MAX(ah.triggered_at) as last_triggered_at,
                        COALESCE((
                            SELECT status 
                            FROM meta.alert_history 
                            WHERE alert_id = ac.alert_id 
                            ORDER BY triggered_at DESC 
                            LIMIT 1
                        ), 'NONE') as status
                    FROM meta.alert_config ac
                    LEFT JOIN meta.alert_history ah ON ac.alert_id = ah.alert_id
                    WHERE ac.is_active = TRUE
                    GROUP BY ac.alert_id, ac.alert_name, ac.alert_type, ac.severity, ac.target_name
                    ORDER BY triggered_count_24h DESC, ac.severity;
                """)
                rows = cur.fetchall()
                return [AlertSummary(**row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_alert_summary: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_alert_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/alerts/history", summary="Get Alert History")
async def get_alert_history(
    hours: int = Query(default=24, ge=1, le=168),
    status: Optional[str] = None
):
    """
    Get alert trigger history
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # FIX: Use proper parameterized query for INTERVAL
                query = """
                    SELECT 
                        ah.alert_history_id,
                        ac.alert_name,
                        ac.alert_type,
                        ah.severity,
                        ah.alert_message,
                        ah.triggered_at,
                        ah.status,
                        ah.acknowledged_by,
                        ah.resolved_by
                    FROM meta.alert_history ah
                    JOIN meta.alert_config ac ON ah.alert_id = ac.alert_id
                    WHERE ah.triggered_at >= NOW() - INTERVAL '1 hour' * %s
                """
                params = [hours]
                
                if status:
                    query += " AND ah.status = %s"
                    params.append(status)
                
                query += " ORDER BY ah.triggered_at DESC LIMIT 100"
                
                cur.execute(query, params)
                return cur.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Database error in get_alert_history: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_alert_history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ===================================================================
# STATISTICS & DASHBOARDS
# ===================================================================

@router.get("/stats/pipeline-performance", summary="Get Pipeline Performance Stats")
async def get_pipeline_performance(days: int = Query(default=7, ge=1, le=365)):
    """
    Get pipeline performance statistics over time
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # FIX: Use proper parameterized query for INTERVAL
                cur.execute("""
                    SELECT 
                        j.job_code,
                        j.job_name,
                        DATE(r.started_at) as run_date,
                        COUNT(*) as runs_count,
                        COUNT(*) FILTER (WHERE r.status = 'SUCCESS') as success_count,
                        COUNT(*) FILTER (WHERE r.status = 'FAILED') as failed_count,
                        AVG(EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60) as avg_duration_minutes,
                        MIN(EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60) as min_duration_minutes,
                        MAX(EXTRACT(EPOCH FROM (r.finished_at - r.started_at))/60) as max_duration_minutes
                    FROM meta.etl_run r
                    JOIN meta.etl_job j ON r.job_id = j.job_id
                    WHERE r.started_at >= NOW() - INTERVAL '1 day' * %s
                    GROUP BY j.job_code, j.job_name, DATE(r.started_at)
                    ORDER BY run_date DESC, j.job_code;
                """, (days,))
                return cur.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Database error in get_pipeline_performance: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_pipeline_performance: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/stats/data-volume", summary="Get Data Volume Trends")
async def get_data_volume_trends(days: int = Query(default=30, ge=1, le=365)):
    """
    Get data volume growth trends across schemas
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        schema_name,
                        snapshot_date,
                        SUM(row_count) as total_rows,
                        ROUND(SUM(size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) as total_size_gb
                    FROM meta.table_stats
                    WHERE snapshot_date >= CURRENT_DATE - %s
                    GROUP BY schema_name, snapshot_date
                    ORDER BY snapshot_date DESC, schema_name;
                """, (days,))
                return cur.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Database error in get_data_volume_trends: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_data_volume_trends: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


