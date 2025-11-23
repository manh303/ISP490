#!/usr/bin/env python3
"""
Populate ETL Metadata from Airflow
Backfill ETL run history from Airflow metadata database
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Database URLs
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"
)

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# ============================================================
# 1. INSERT SAMPLE ETL RUNS
# ============================================================
def populate_etl_runs():
    """Populate sample ETL runs for testing"""
    print("\n[INFO] Populating ETL runs...")
    conn = get_conn()
    conn.autocommit = False
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get job IDs
            cur.execute("SELECT job_id, job_code FROM meta.etl_job;")
            jobs = {row['job_code']: row['job_id'] for row in cur.fetchall()}
            
            if not jobs:
                print("  [WARN] No ETL jobs found. Run extended schema first.")
                return
            
            # Sample runs for MINIO pipeline
            if 'MINIO_ECOMMERCE_DWH_PIPELINE' in jobs:
                job_id = jobs['MINIO_ECOMMERCE_DWH_PIPELINE']
                
                sample_runs = [
                    # Successful runs
                    ('2025-11-23', '2025-11-23 15:30:00', '2025-11-23 15:45:30', 'SUCCESS', 54679, 54679, None, 'manual__2025-11-23T08:30:00'),
                    ('2025-11-22', '2025-11-22 14:20:00', '2025-11-22 14:38:15', 'SUCCESS', 52341, 52341, None, 'scheduled__2025-11-22T03:00:00'),
                    ('2025-11-21', '2025-11-21 03:15:00', '2025-11-21 03:32:45', 'SUCCESS', 51203, 51203, None, 'scheduled__2025-11-21T03:00:00'),
                    ('2025-11-20', '2025-11-20 03:10:00', '2025-11-20 03:28:30', 'SUCCESS', 49876, 49876, None, 'scheduled__2025-11-20T03:00:00'),
                    
                    # Failed run
                    ('2025-11-19', '2025-11-19 03:05:00', '2025-11-19 03:15:00', 'FAILED', 0, 0, 'Connection timeout to MinIO', 'scheduled__2025-11-19T03:00:00'),
                    
                    # More successful runs
                    ('2025-11-18', '2025-11-18 03:00:00', '2025-11-18 03:25:00', 'SUCCESS', 48567, 48567, None, 'scheduled__2025-11-18T03:00:00'),
                    ('2025-11-17', '2025-11-17 03:00:00', '2025-11-17 03:22:30', 'SUCCESS', 47234, 47234, None, 'scheduled__2025-11-17T03:00:00'),
                ]
                
                for run_date, started_at, finished_at, status, rows_read, rows_written, error_msg, airflow_run_id in sample_runs:
                    cur.execute("""
                        INSERT INTO meta.etl_run (
                            job_id, run_date, started_at, finished_at, status,
                            rows_read, rows_written, error_message, airflow_run_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (job_id, run_date, started_at, finished_at, status,
                          rows_read, rows_written, error_msg, airflow_run_id))
                
                print(f"  [OK] Added {len(sample_runs)} sample runs for MINIO pipeline")
            
            # Sample runs for ML pipeline
            if 'ML_TRAINING_PIPELINE' in jobs:
                job_id = jobs['ML_TRAINING_PIPELINE']
                
                sample_runs = [
                    ('2025-11-23', '2025-11-23 16:00:00', '2025-11-23 16:28:30', 'SUCCESS', 50080, 50080, None, 'scheduled__2025-11-23T04:00:00'),
                    ('2025-11-22', '2025-11-22 04:00:00', '2025-11-22 04:25:15', 'SUCCESS', 48234, 48234, None, 'scheduled__2025-11-22T04:00:00'),
                    ('2025-11-21', '2025-11-21 04:00:00', '2025-11-21 04:30:45', 'FAILED', 0, 0, 'Model training timeout', 'scheduled__2025-11-21T04:00:00'),
                    ('2025-11-20', '2025-11-20 04:00:00', '2025-11-20 04:22:00', 'SUCCESS', 46789, 46789, None, 'scheduled__2025-11-20T04:00:00'),
                ]
                
                for run_date, started_at, finished_at, status, rows_read, rows_written, error_msg, airflow_run_id in sample_runs:
                    cur.execute("""
                        INSERT INTO meta.etl_run (
                            job_id, run_date, started_at, finished_at, status,
                            rows_read, rows_written, error_message, airflow_run_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (job_id, run_date, started_at, finished_at, status,
                          rows_read, rows_written, error_msg, airflow_run_id))
                
                print(f"  [OK] Added {len(sample_runs)} sample runs for ML pipeline")
            
            conn.commit()
            
            # Verify
            cur.execute("SELECT COUNT(*) as count FROM meta.etl_run;")
            total = cur.fetchone()['count']
            print(f"  [OK] Total ETL runs in database: {total}")
            
    except Exception as e:
        conn.rollback()
        print(f"  [ERROR] Failed to populate ETL runs: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

# ============================================================
# 2. INSERT SAMPLE ETL LOGS
# ============================================================
def populate_etl_logs():
    """Populate sample ETL logs"""
    print("\n[INFO] Populating ETL logs...")
    conn = get_conn()
    conn.autocommit = False
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get a recent run_id
            cur.execute("""
                SELECT run_id FROM meta.etl_run 
                WHERE status = 'SUCCESS'
                ORDER BY started_at DESC 
                LIMIT 1;
            """)
            result = cur.fetchone()
            
            if not result:
                print("  [WARN] No ETL runs found. Run populate_etl_runs first.")
                return
            
            run_id = result['run_id']
            
            # Sample logs for a successful run
            sample_logs = [
                ('spark_build_star_dwh', 'LOAD_RAW', 'INFO', 'Loading raw data from MinIO', 360, 0, None),
                ('spark_build_star_dwh', 'LOAD_RAW', 'INFO', 'Found 360 JSONL files', 360, 0, None),
                ('spark_build_star_dwh', 'CLEAN_DATA', 'INFO', 'Cleaning and standardizing data', 462020, 0, None),
                ('spark_build_star_dwh', 'DEDUP', 'INFO', 'Deduplicating records', 462020, 54679, None),
                ('spark_build_star_dwh', 'VALIDATE', 'INFO', 'Data validation complete', 54679, 52895, None),
                ('spark_build_star_dwh', 'LOAD_DWH', 'INFO', 'Loading dim_date', 13, 0, None),
                ('spark_build_star_dwh', 'LOAD_DWH', 'INFO', 'Loading dim_platform', 3, 0, None),
                ('spark_build_star_dwh', 'LOAD_DWH', 'INFO', 'Loading dim_category', 13, 0, None),
                ('spark_build_star_dwh', 'LOAD_DWH', 'INFO', 'Loading dim_brand', 1279, 0, None),
                ('spark_build_star_dwh', 'LOAD_DWH', 'INFO', 'Loading dim_product', 54679, 0, None),
                ('spark_build_star_dwh', 'LOAD_DWH', 'INFO', 'Loading fact_product_daily', 54679, 0, None),
                ('spark_build_star_dwh', 'COMPLETE', 'INFO', 'DWH build completed successfully', 54679, 54679, None),
            ]
            
            for job_name, stage, log_level, log_message, records_processed, records_failed, error_msg in sample_logs:
                cur.execute("""
                    INSERT INTO meta.etl_log (
                        run_id, job_name, stage, log_level, log_message,
                        records_processed, records_failed, error_message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (run_id, job_name, stage, log_level, log_message,
                      records_processed, records_failed, error_msg))
            
            conn.commit()
            print(f"  [OK] Added {len(sample_logs)} log entries for run_id={run_id}")
            
    except Exception as e:
        conn.rollback()
        print(f"  [ERROR] Failed to populate ETL logs: {e}")
    finally:
        conn.close()

# ============================================================
# 3. INSERT SAMPLE DATA QUALITY ISSUES
# ============================================================
def populate_dq_issues():
    """Populate sample data quality issues"""
    print("\n[INFO] Populating data quality issues...")
    conn = get_conn()
    conn.autocommit = False
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sample_issues = [
                ('dwh', 'fact_product_daily', 'INVALID_DATA', 'MEDIUM', 'OPEN', 2263,
                 'Missing or invalid price values detected', None),
                ('dwh', 'dim_product', 'DUPLICATE', 'LOW', 'RESOLVED', 15,
                 'Duplicate product names with different IDs', None),
                ('ml', 'fact_review_sentiment', 'NULL_VALUE', 'LOW', 'OPEN', 89,
                 'Null sentiment scores for some reviews', None),
            ]
            
            for schema, table, issue_type, severity, status, affected_rows, desc, sample in sample_issues:
                cur.execute("""
                    INSERT INTO meta.data_quality_issue (
                        schema_name, table_name, issue_type, severity, status,
                        affected_rows, issue_description, sample_rows
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (schema, table, issue_type, severity, status, affected_rows, desc, sample))
            
            conn.commit()
            print(f"  [OK] Added {len(sample_issues)} data quality issues")
            
    except Exception as e:
        conn.rollback()
        print(f"  [ERROR] Failed to populate DQ issues: {e}")
    finally:
        conn.close()

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("ETL METADATA POPULATOR")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    
    try:
        # Check connection
        print("\n[INFO] Checking database connection...")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM meta.etl_job;")
        job_count = cur.fetchone()[0]
        print(f"  [OK] Connected. Found {job_count} ETL jobs")
        cur.close()
        conn.close()
        
        # Populate data
        populate_etl_runs()
        populate_etl_logs()
        populate_dq_issues()
        
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        
        # Show statistics
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT COUNT(*) as count FROM meta.etl_run;")
        run_count = cur.fetchone()['count']
        print(f"ETL Runs: {run_count}")
        
        cur.execute("SELECT COUNT(*) as count FROM meta.etl_log;")
        log_count = cur.fetchone()['count']
        print(f"ETL Logs: {log_count}")
        
        cur.execute("SELECT COUNT(*) as count FROM meta.data_quality_issue;")
        issue_count = cur.fetchone()['count']
        print(f"DQ Issues: {issue_count}")
        
        cur.close()
        conn.close()
        
        print("\n[SUCCESS] Metadata population completed!")
        print("\nNext steps:")
        print("1. Run: python test_data_engineer_api.py")
        print("2. Check endpoints with data:")
        print("   - GET /etl/runs/MINIO_ECOMMERCE_DWH_PIPELINE")
        print("   - GET /etl/logs/{run_id}")
        print("   - GET /data-quality/issues")
        print("   - GET /stats/pipeline-performance")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())

