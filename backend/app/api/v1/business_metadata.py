# backend/app/api/v1/business_metadata.py
"""
Business Metadata API - Data Catalog, Glossary, and Quality
Provides endpoints for data catalog, business glossary, expectations, and source systems
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os

router = APIRouter(prefix="/business-metadata", tags=["Business Metadata"])

# Database connection
def get_db_conn():
    """Get database connection"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
    return psycopg2.connect(db_url)

# ===================================================================
# MODELS
# ===================================================================

class SourceSystem(BaseModel):
    source_id: int
    code: str
    name: Optional[str]
    owner_contact: Optional[str]
    dataset_count: Optional[int]

class Dataset(BaseModel):
    dataset_id: int
    source_code: str
    source_name: Optional[str]
    layer: str
    schema_name: str
    table_name: str
    dataset_type: str
    pii_class: Optional[str]
    retention_days: Optional[int]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    row_count: Optional[int]
    size_mb: Optional[float]

class DatasetDetail(BaseModel):
    dataset_id: int
    source_code: str
    source_name: Optional[str]
    layer: str
    schema_name: str
    table_name: str
    dataset_type: str
    pii_class: Optional[str]
    retention_days: Optional[int]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    # Statistics
    row_count: Optional[int]
    size_mb: Optional[float]
    last_loaded_at: Optional[datetime]
    freshness_hours: Optional[float]
    # Related info
    upstream_sources: List[str]
    downstream_targets: List[str]
    quality_issues_count: int
    expectations_count: int

class BusinessTerm(BaseModel):
    term_id: int
    term_name: str
    definition: Optional[str]
    steward: Optional[str]
    status: Optional[str]
    related_datasets: Optional[List[str]]

class DataExpectation(BaseModel):
    exp_id: int
    dataset_id: int
    schema_name: str
    table_name: str
    name: str
    severity: str
    check_sql: str
    owner: Optional[str]
    tags: Optional[str]
    last_check_passed: Optional[bool]
    last_check_time: Optional[datetime]

class Job(BaseModel):
    job_id: int
    job_name: str
    owner: Optional[str]
    schedule: Optional[str]
    active: bool
    related_datasets: Optional[List[str]]

# ===================================================================
# SOURCE SYSTEMS
# ===================================================================

@router.get("/sources", response_model=List[SourceSystem], summary="Get All Source Systems")
async def get_source_systems():
    """
    Get list of all registered source systems with dataset counts
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    s.source_id,
                    s.code,
                    s.name,
                    s.owner_contact,
                    COUNT(d.dataset_id) as dataset_count
                FROM meta_source_system s
                LEFT JOIN meta_dataset d ON s.source_id = d.source_id
                GROUP BY s.source_id, s.code, s.name, s.owner_contact
                ORDER BY s.code;
            """)
            rows = cur.fetchall()
            return [SourceSystem(**row) for row in rows]
    finally:
        conn.close()

@router.get("/sources/{code}", summary="Get Source System Details")
async def get_source_system_detail(code: str):
    """
    Get detailed information about a specific source system
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get source info
            cur.execute("""
                SELECT 
                    s.source_id,
                    s.code,
                    s.name,
                    s.owner_contact,
                    COUNT(d.dataset_id) as dataset_count
                FROM meta_source_system s
                LEFT JOIN meta_dataset d ON s.source_id = d.source_id
                WHERE s.code = %s
                GROUP BY s.source_id, s.code, s.name, s.owner_contact;
            """, (code,))
            source = cur.fetchone()
            
            if not source:
                raise HTTPException(status_code=404, detail=f"Source system '{code}' not found")
            
            # Get datasets from this source
            cur.execute("""
                SELECT 
                    d.dataset_id,
                    d.layer,
                    d.schema_name,
                    d.table_name,
                    d.dataset_type,
                    d.pii_class,
                    d.retention_days
                FROM meta_dataset d
                WHERE d.source_id = %s
                ORDER BY d.layer, d.table_name;
            """, (source['source_id'],))
            datasets = cur.fetchall()
            
            return {
                **source,
                "datasets": datasets
            }
    finally:
        conn.close()

# ===================================================================
# DATA CATALOG
# ===================================================================

@router.get("/catalog/datasets", response_model=List[Dataset], summary="Get All Datasets")
async def get_datasets(
    layer: Optional[str] = None,
    source_code: Optional[str] = None,
    pii_only: bool = False
):
    """
    Get list of all datasets (tables) in the catalog with filtering options
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    d.dataset_id,
                    s.code as source_code,
                    s.name as source_name,
                    d.layer,
                    d.schema_name,
                    d.table_name,
                    d.dataset_type,
                    d.pii_class,
                    d.retention_days,
                    d.created_at,
                    d.updated_at,
                    ts.row_count,
                    ROUND(ts.size_bytes / 1024.0 / 1024.0, 2) as size_mb
                FROM meta_dataset d
                LEFT JOIN meta_source_system s ON d.source_id = s.source_id
                LEFT JOIN LATERAL (
                    SELECT row_count, size_bytes
                    FROM meta.table_stats
                    WHERE schema_name = d.schema_name 
                      AND table_name = d.table_name
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                ) ts ON TRUE
                WHERE 1=1
            """
            params = []
            
            if layer:
                query += " AND d.layer = %s"
                params.append(layer)
            
            if source_code:
                query += " AND s.code = %s"
                params.append(source_code)
            
            if pii_only:
                query += " AND d.pii_class IS NOT NULL"
            
            query += " ORDER BY d.layer, d.table_name;"
            
            cur.execute(query, params)
            rows = cur.fetchall()
            return [Dataset(**row) for row in rows]
    finally:
        conn.close()

@router.get("/catalog/datasets/{dataset_id}", response_model=DatasetDetail, summary="Get Dataset Details")
async def get_dataset_detail(dataset_id: int):
    """
    Get detailed information about a specific dataset including lineage and quality
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get dataset info
            cur.execute("""
                SELECT 
                    d.dataset_id,
                    s.code as source_code,
                    s.name as source_name,
                    d.layer,
                    d.schema_name,
                    d.table_name,
                    d.dataset_type,
                    d.pii_class,
                    d.retention_days,
                    d.created_at,
                    d.updated_at,
                    ts.row_count,
                    ROUND(ts.size_bytes / 1024.0 / 1024.0, 2) as size_mb,
                    ts.last_loaded_at,
                    EXTRACT(EPOCH FROM (NOW() - ts.last_loaded_at))/3600 as freshness_hours
                FROM meta_dataset d
                LEFT JOIN meta_source_system s ON d.source_id = s.source_id
                LEFT JOIN LATERAL (
                    SELECT row_count, size_bytes, last_loaded_at
                    FROM meta.table_stats
                    WHERE schema_name = d.schema_name 
                      AND table_name = d.table_name
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                ) ts ON TRUE
                WHERE d.dataset_id = %s;
            """, (dataset_id,))
            dataset = cur.fetchone()
            
            if not dataset:
                raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
            
            # Get upstream sources
            cur.execute("""
                SELECT CONCAT(source_schema, '.', source_table) as source_table
                FROM meta.data_lineage
                WHERE target_schema = %s AND target_table = %s
                  AND is_active = TRUE;
            """, (dataset['schema_name'], dataset['table_name']))
            upstream = [row['source_table'] for row in cur.fetchall()]
            
            # Get downstream targets
            cur.execute("""
                SELECT CONCAT(target_schema, '.', target_table) as target_table
                FROM meta.data_lineage
                WHERE source_schema = %s AND source_table = %s
                  AND is_active = TRUE;
            """, (dataset['schema_name'], dataset['table_name']))
            downstream = [row['target_table'] for row in cur.fetchall()]
            
            # Get quality issues count
            cur.execute("""
                SELECT COUNT(*) as count
                FROM meta.data_quality_issue
                WHERE schema_name = %s AND table_name = %s
                  AND status IN ('OPEN', 'IN_PROGRESS');
            """, (dataset['schema_name'], dataset['table_name']))
            quality_count = cur.fetchone()['count']
            
            # Get expectations count
            cur.execute("""
                SELECT COUNT(*) as count
                FROM meta_expectation
                WHERE dataset_id = %s;
            """, (dataset_id,))
            exp_count = cur.fetchone()['count']
            
            return DatasetDetail(
                **dataset,
                upstream_sources=upstream,
                downstream_targets=downstream,
                quality_issues_count=quality_count,
                expectations_count=exp_count
            )
    finally:
        conn.close()

@router.get("/catalog/search", summary="Search Data Catalog")
async def search_catalog(
    q: str = Query(..., description="Search query"),
    limit: int = Query(default=50, le=100)
):
    """
    Search datasets by name, description, or tags
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            search_pattern = f"%{q}%"
            cur.execute("""
                SELECT 
                    d.dataset_id,
                    s.code as source_code,
                    d.layer,
                    d.schema_name,
                    d.table_name,
                    d.dataset_type,
                    d.pii_class,
                    ts.row_count,
                    ROUND(ts.size_bytes / 1024.0 / 1024.0, 2) as size_mb
                FROM meta_dataset d
                LEFT JOIN meta_source_system s ON d.source_id = s.source_id
                LEFT JOIN LATERAL (
                    SELECT row_count, size_bytes
                    FROM meta.table_stats
                    WHERE schema_name = d.schema_name 
                      AND table_name = d.table_name
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                ) ts ON TRUE
                WHERE d.table_name ILIKE %s
                   OR d.schema_name ILIKE %s
                   OR s.name ILIKE %s
                ORDER BY 
                    CASE WHEN d.table_name ILIKE %s THEN 1 ELSE 2 END,
                    d.table_name
                LIMIT %s;
            """, (search_pattern, search_pattern, search_pattern, search_pattern, limit))
            return cur.fetchall()
    finally:
        conn.close()

@router.get("/catalog/schemas", summary="Get All Schemas")
async def get_schemas():
    """
    Get list of all schemas with table counts
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    schema_name,
                    COUNT(DISTINCT table_name) as table_count,
                    SUM(ts.row_count) as total_rows,
                    ROUND(SUM(ts.size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) as total_size_gb
                FROM meta_dataset d
                LEFT JOIN LATERAL (
                    SELECT row_count, size_bytes
                    FROM meta.table_stats
                    WHERE schema_name = d.schema_name 
                      AND table_name = d.table_name
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                ) ts ON TRUE
                GROUP BY schema_name
                ORDER BY schema_name;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@router.get("/catalog/schemas/{schema_name}/tables", summary="Get Tables in Schema")
async def get_schema_tables(schema_name: str):
    """
    Get all tables in a specific schema
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    d.dataset_id,
                    d.table_name,
                    d.dataset_type,
                    d.pii_class,
                    s.code as source_code,
                    ts.row_count,
                    ROUND(ts.size_bytes / 1024.0 / 1024.0, 2) as size_mb,
                    ts.last_loaded_at
                FROM meta_dataset d
                LEFT JOIN meta_source_system s ON d.source_id = s.source_id
                LEFT JOIN LATERAL (
                    SELECT row_count, size_bytes, last_loaded_at
                    FROM meta.table_stats
                    WHERE schema_name = d.schema_name 
                      AND table_name = d.table_name
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                ) ts ON TRUE
                WHERE d.schema_name = %s
                ORDER BY d.table_name;
            """, (schema_name,))
            return cur.fetchall()
    finally:
        conn.close()

# ===================================================================
# BUSINESS GLOSSARY
# ===================================================================

@router.get("/glossary/terms", response_model=List[BusinessTerm], summary="Get All Business Terms")
async def get_business_terms(
    status: Optional[str] = None
):
    """
    Get list of all business terms from the glossary
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Simplified query without complex JOIN
            query = """
                SELECT 
                    bt.term_id,
                    bt.term_name,
                    bt.definition,
                    bt.steward,
                    bt.status,
                    ARRAY[]::TEXT[] as related_datasets
                FROM meta_business_term bt
                WHERE 1=1
            """
            params = []
            
            if status:
                query += " AND bt.status = %s"
                params.append(status)
            
            query += " ORDER BY bt.term_name;"
            
            cur.execute(query, params)
            rows = cur.fetchall()
            return [BusinessTerm(**row) for row in rows]
    finally:
        conn.close()

@router.get("/glossary/terms/{term_id}", summary="Get Business Term Detail")
async def get_business_term_detail(term_id: int):
    """
    Get detailed information about a specific business term
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    term_id,
                    term_name,
                    definition,
                    steward,
                    status
                FROM meta_business_term
                WHERE term_id = %s;
            """, (term_id,))
            term = cur.fetchone()
            
            if not term:
                raise HTTPException(status_code=404, detail=f"Business term {term_id} not found")
            
            # Find related datasets
            cur.execute("""
                SELECT 
                    d.dataset_id,
                    d.schema_name,
                    d.table_name,
                    d.layer
                FROM meta_dataset d
                WHERE d.table_name ILIKE %s
                ORDER BY d.layer, d.table_name;
            """, (f"%{term['term_name'].replace(' ', '_')}%",))
            related = cur.fetchall()
            
            return {
                **term,
                "related_datasets": related
            }
    finally:
        conn.close()

@router.get("/glossary/search", summary="Search Business Glossary")
async def search_glossary(
    q: str = Query(..., description="Search query"),
    limit: int = Query(default=50, le=100)
):
    """
    Search business terms by name or definition
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            search_pattern = f"%{q}%"
            cur.execute("""
                SELECT 
                    term_id,
                    term_name,
                    definition,
                    steward,
                    status
                FROM meta_business_term
                WHERE term_name ILIKE %s
                   OR definition ILIKE %s
                ORDER BY 
                    CASE WHEN term_name ILIKE %s THEN 1 ELSE 2 END,
                    term_name
                LIMIT %s;
            """, (search_pattern, search_pattern, search_pattern, limit))
            return cur.fetchall()
    finally:
        conn.close()

@router.post("/glossary/terms", summary="Create Business Term")
async def create_business_term(
    term_name: str,
    definition: str,
    steward: Optional[str] = None,
    status: str = "draft"
):
    """
    Create a new business term in the glossary
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO meta_business_term (term_name, definition, steward, status)
                VALUES (%s, %s, %s, %s)
                RETURNING term_id, term_name, definition, steward, status;
            """, (term_name, definition, steward, status))
            result = cur.fetchone()
            conn.commit()
            return result
    except psycopg2.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Term '{term_name}' already exists")
    finally:
        conn.close()

# ===================================================================
# DATA EXPECTATIONS / QUALITY RULES
# ===================================================================

@router.get("/expectations", response_model=List[DataExpectation], summary="Get All Data Expectations")
async def get_expectations(
    severity: Optional[str] = None,
    dataset_id: Optional[int] = None
):
    """
    Get list of all data quality expectations/rules
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    e.exp_id,
                    e.dataset_id,
                    d.schema_name,
                    d.table_name,
                    e.name,
                    e.severity,
                    e.check_sql,
                    e.owner,
                    e.tags,
                    cr.passed as last_check_passed,
                    cr.created_at as last_check_time
                FROM meta_expectation e
                JOIN meta_dataset d ON e.dataset_id = d.dataset_id
                LEFT JOIN LATERAL (
                    SELECT passed, created_at
                    FROM meta.data_quality_check_result
                    WHERE rule_id = e.exp_id
                    ORDER BY created_at DESC
                    LIMIT 1
                ) cr ON TRUE
                WHERE 1=1
            """
            params = []
            
            if severity:
                query += " AND e.severity = %s"
                params.append(severity)
            
            if dataset_id:
                query += " AND e.dataset_id = %s"
                params.append(dataset_id)
            
            query += " ORDER BY e.severity DESC, d.table_name, e.name;"
            
            cur.execute(query, params)
            rows = cur.fetchall()
            return [DataExpectation(**row) for row in rows]
    finally:
        conn.close()

@router.get("/expectations/{exp_id}/results", summary="Get Expectation Check Results")
async def get_expectation_results(
    exp_id: int,
    limit: int = Query(default=20, le=100)
):
    """
    Get check results history for a specific expectation
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    cr.check_id,
                    cr.check_date,
                    cr.passed,
                    cr.failed_count,
                    cr.total_count,
                    cr.error_message,
                    cr.created_at
                FROM meta.data_quality_check_result cr
                WHERE cr.rule_id = %s
                ORDER BY cr.created_at DESC
                LIMIT %s;
            """, (exp_id, limit))
            return cur.fetchall()
    finally:
        conn.close()

@router.post("/expectations", summary="Create Data Expectation")
async def create_expectation(
    dataset_id: int,
    name: str,
    severity: str,
    check_sql: str,
    owner: Optional[str] = None,
    tags: Optional[str] = None
):
    """
    Create a new data quality expectation/rule
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO meta_expectation (dataset_id, name, severity, check_sql, owner, tags)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING exp_id, dataset_id, name, severity, check_sql, owner, tags;
            """, (dataset_id, name, severity, check_sql, owner, tags))
            result = cur.fetchone()
            conn.commit()
            return result
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# ===================================================================
# JOBS
# ===================================================================

@router.get("/jobs", response_model=List[Job], summary="Get All Jobs")
async def get_jobs(active_only: bool = True):
    """
    Get list of all registered jobs
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    j.job_id,
                    j.job_name,
                    j.owner,
                    j.schedule,
                    j.active,
                    ARRAY_AGG(DISTINCT d.table_name) FILTER (WHERE d.table_name IS NOT NULL) as related_datasets
                FROM meta_job j
                LEFT JOIN meta_dataset d ON 
                    j.job_name ILIKE '%' || REPLACE(d.table_name, '_', '%') || '%'
            """
            
            if active_only:
                query += " WHERE j.active = TRUE"
            
            query += """
                GROUP BY j.job_id, j.job_name, j.owner, j.schedule, j.active
                ORDER BY j.job_name;
            """
            
            cur.execute(query)  # No params needed
            rows = cur.fetchall()
            return [Job(**row) for row in rows]
    finally:
        conn.close()

@router.get("/jobs/{job_id}", summary="Get Job Details")
async def get_job_detail(job_id: int):
    """
    Get detailed information about a specific job
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    job_id,
                    job_name,
                    owner,
                    schedule,
                    active
                FROM meta_job
                WHERE job_id = %s;
            """, (job_id,))
            job = cur.fetchone()
            
            if not job:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            
            return job
    finally:
        conn.close()
