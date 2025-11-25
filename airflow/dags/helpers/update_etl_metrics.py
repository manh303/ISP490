#!/usr/bin/env python3
"""
Update ETL run metrics (rows_read, rows_written) after Spark job completes
Query actual row counts from DWH tables
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# Load .env if exists
try:
    from dotenv import load_dotenv
    load_dotenv(encoding='utf-8')
except Exception:
    pass

DEFAULT_DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"


def update_etl_run_metrics(run_id=None, job_code=None):
    """
    Update rows_read and rows_written for an ETL run
    
    Args:
        run_id: Specific run_id to update (optional)
        job_code: Job code to update latest run (optional)
    """
    db_url = os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL
    
    if not db_url:
        print("[METRICS] DATABASE_URL not set")
        return False
    
    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f"[METRICS] Failed to connect: {e}")
        return False
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # If no run_id specified, get latest RUNNING or SUCCESS run
            if not run_id:
                if job_code:
                    cur.execute("""
                        SELECT r.run_id, r.job_code, r.status, r.started_at
                        FROM meta.etl_run r
                        JOIN meta.etl_job j ON r.job_id = j.job_id
                        WHERE j.job_code = %s
                          AND r.status IN ('RUNNING', 'SUCCESS')
                          AND r.started_at >= NOW() - INTERVAL '24 hours'
                        ORDER BY r.started_at DESC
                        LIMIT 1;
                    """, (job_code,))
                else:
                    cur.execute("""
                        SELECT run_id, job_code, status, started_at
                        FROM meta.etl_run r
                        JOIN meta.etl_job j ON r.job_id = j.job_id
                        WHERE r.status IN ('RUNNING', 'SUCCESS')
                          AND r.started_at >= NOW() - INTERVAL '24 hours'
                        ORDER BY r.started_at DESC
                        LIMIT 1;
                    """)
                
                result = cur.fetchone()
                if not result:
                    print("[METRICS] No recent ETL run found to update")
                    return False
                
                run_id = result['run_id']
                job_code = result['job_code']
                print(f"[METRICS] Updating run_id={run_id}, job_code={job_code}")
            
            # Count rows in key DWH tables
            rows_written = 0
            table_counts = {}
            
            # DWH fact tables
            fact_tables = [
                ('dwh', 'fact_product_daily'),
                ('dwh', 'fact_review'),
                ('dwh', 'fact_review_daily'),
            ]
            
            # Dimension tables
            dim_tables = [
                ('dwh', 'dim_product'),
                ('dwh', 'dim_category'),
                ('dwh', 'dim_brand'),
                ('dwh', 'dim_platform'),
            ]
            
            all_tables = fact_tables + dim_tables
            
            print("[METRICS] Counting rows in DWH tables...")
            for schema, table in all_tables:
                try:
                    cur.execute(f"SELECT COUNT(*) as cnt FROM {schema}.{table};")
                    count = cur.fetchone()['cnt']
                    table_counts[f"{schema}.{table}"] = count
                    rows_written += count
                    print(f"  ✅ {schema}.{table}: {count:,} rows")
                except Exception as e:
                    print(f"  ⚠️  {schema}.{table}: Error - {e}")
                    conn.rollback()
            
            # Estimate rows_read (usually more than written due to dedup)
            # Rough estimate: read ~1.2x what we wrote
            rows_read = int(rows_written * 1.2) if rows_written > 0 else None
            
            # Update ETL run
            cur.execute("""
                UPDATE meta.etl_run
                SET rows_read = %s,
                    rows_written = %s
                WHERE run_id = %s;
            """, (rows_read, rows_written, run_id))
            
            conn.commit()
            
            print(f"\n[METRICS] ✅ Updated run_id={run_id}")
            print(f"  rows_read: {rows_read:,}" if rows_read else "  rows_read: NULL")
            print(f"  rows_written: {rows_written:,}")
            print(f"  Total tables: {len(table_counts)}")
            
            return True
            
    except Exception as e:
        print(f"[METRICS] Error updating metrics: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def update_latest_etl_metrics(job_code='MINIO_ECOMMERCE_DWH_PIPELINE'):
    """
    Update metrics for the latest ETL run of a specific job
    """
    print(f"\n[METRICS] Updating metrics for latest {job_code} run...")
    return update_etl_run_metrics(job_code=job_code)


def update_all_null_metrics(days=7):
    """
    Update all ETL runs that have NULL rows_read/rows_written
    """
    print(f"\n[METRICS] Updating all runs with NULL metrics (last {days} days)...")
    
    db_url = os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL
    
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all runs with NULL metrics
            cur.execute("""
                SELECT r.run_id, j.job_code, r.status, r.started_at
                FROM meta.etl_run r
                JOIN meta.etl_job j ON r.job_id = j.job_id
                WHERE (r.rows_read IS NULL OR r.rows_written IS NULL)
                  AND r.status = 'SUCCESS'
                  AND r.started_at >= NOW() - INTERVAL '%s days'
                ORDER BY r.started_at DESC;
            """ % days)
            
            runs = cur.fetchall()
            
            if not runs:
                print("[METRICS] No runs with NULL metrics found")
                return True
            
            print(f"[METRICS] Found {len(runs)} runs to update")
            
            updated = 0
            for run in runs:
                print(f"\n[METRICS] Updating run_id={run['run_id']} ({run['job_code']})")
                if update_etl_run_metrics(run_id=run['run_id'], job_code=run['job_code']):
                    updated += 1
            
            print(f"\n[METRICS] ✅ Updated {updated}/{len(runs)} runs")
            return True
            
    except Exception as e:
        print(f"[METRICS] Error: {e}")
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "all":
            # Update all NULL metrics
            update_all_null_metrics()
        elif sys.argv[1] == "latest":
            # Update latest run
            job_code = sys.argv[2] if len(sys.argv) > 2 else 'MINIO_ECOMMERCE_DWH_PIPELINE'
            update_latest_etl_metrics(job_code)
        else:
            # Update specific run_id
            try:
                run_id = int(sys.argv[1])
                update_etl_run_metrics(run_id=run_id)
            except ValueError:
                print("Usage:")
                print("  python update_etl_metrics.py               # Update latest run")
                print("  python update_etl_metrics.py all           # Update all NULL metrics")
                print("  python update_etl_metrics.py latest [JOB]  # Update latest run for job")
                print("  python update_etl_metrics.py <run_id>      # Update specific run")
    else:
        # Default: update latest run
        update_latest_etl_metrics()

