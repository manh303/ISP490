#!/usr/bin/env python3
"""
Helper function to collect metadata metrics after ETL completes
Call this at the end of ETL DAG to update table_stats with current snapshot_date
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Load .env file if exists
try:
    from dotenv import load_dotenv
    load_dotenv(encoding='utf-8')
except Exception as e:
    # Ignore .env loading errors, will use DEFAULT_DATABASE_URL
    pass

# Default DATABASE_URL if not in environment
DEFAULT_DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"


def collect_table_stats_after_etl():
    """
    Collect and update table statistics after ETL completes
    Updates snapshot_date to TODAY for all DWH/ML tables
    """
    print("\n[METRICS] Collecting table statistics after ETL...")
    
    db_url = os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL
    if not db_url:
        print("[METRICS] DATABASE_URL not set, skipping metrics collection")
        return
    
    print(f"[METRICS] Using database: {db_url.split('@')[1].split('/')[0] if '@' in db_url else 'localhost'}")
    
    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f"[METRICS] Failed to connect to database: {e}")
        return
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all tables from dwh, ml, staging schemas
            cur.execute("""
                SELECT 
                    schemaname as schema_name,
                    tablename as table_name,
                    pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes
                FROM pg_tables
                WHERE schemaname IN ('dwh', 'ml', 'staging', 'ods')
                ORDER BY schemaname, tablename;
            """)
            tables = cur.fetchall()
            
            updated_count = 0
            for table in tables:
                schema_name = table['schema_name']
                table_name = table['table_name']
                
                try:
                    # Get row count
                    try:
                        cur.execute(f"SELECT COUNT(*) as cnt FROM {schema_name}.{table_name};")
                        row_count = cur.fetchone()['cnt']
                    except Exception as e:
                        print(f"  [WARN] Error counting {schema_name}.{table_name}: {e}")
                        conn.rollback()
                        row_count = 0
                    
                    # Get last modified time
                    try:
                        cur.execute(f"""
                            SELECT MAX(created_at) as last_modified 
                            FROM {schema_name}.{table_name} 
                            WHERE EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_schema = %s 
                                  AND table_name = %s 
                                  AND column_name = 'created_at'
                            );
                        """, (schema_name, table_name))
                        result = cur.fetchone()
                        last_modified = result['last_modified'] if result else None
                    except Exception:
                        conn.rollback()
                        last_modified = None
                    
                    # Insert/Update with CURRENT_DATE (TODAY)
                    cur.execute("""
                        INSERT INTO meta.table_stats (
                            schema_name, table_name, snapshot_date,
                            row_count, size_bytes,
                            last_loaded_at
                        )
                        VALUES (%s, %s, CURRENT_DATE, %s, %s, %s)
                        ON CONFLICT (schema_name, table_name, snapshot_date)
                        DO UPDATE SET
                            row_count = EXCLUDED.row_count,
                            size_bytes = EXCLUDED.size_bytes,
                            last_loaded_at = EXCLUDED.last_loaded_at;
                    """, (
                        schema_name, table_name, row_count,
                        table['total_size_bytes'],
                        last_modified or datetime.now()  # Use NOW if no created_at
                    ))
                    conn.commit()
                    updated_count += 1
                    
                except Exception as e:
                    print(f"  [WARN] Failed to update stats for {schema_name}.{table_name}: {e}")
                    conn.rollback()
            
            print(f"  [OK] Updated stats for {updated_count}/{len(tables)} tables with snapshot_date = {datetime.now().date()}")
            
    except Exception as e:
        print(f"[METRICS] Error collecting metrics: {e}")
    finally:
        conn.close()


def collect_db_health():
    """Quick database health check"""
    print("\n[METRICS] Collecting database health...")
    
    db_url = os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL
    if not db_url:
        return
    
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get connection stats
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                    COUNT(*) as total_connections
                FROM pg_stat_activity
                WHERE datname = current_database();
            """)
            conn_stats = cur.fetchone()
            
            # Get max connections
            cur.execute("SHOW max_connections;")
            max_conn = int(cur.fetchone()['max_connections'])
            
            usage_pct = (conn_stats['total_connections'] / max_conn * 100) if max_conn > 0 else 0
            status = 'HEALTHY' if usage_pct < 80 else 'DEGRADED'
            
            # Insert health record
            cur.execute("""
                INSERT INTO meta.db_connection_health (
                    check_time, host, port, database_name, status,
                    active_connections, idle_connections, max_connections,
                    connection_usage_pct, slow_queries_count
                )
                VALUES (
                    NOW(), %s, %s, current_database(), %s,
                    %s, %s, %s, %s, 0
                );
            """, (
                os.getenv('DB_HOST', 'localhost'),
                int(os.getenv('DB_PORT', 5432)),
                status,
                conn_stats['active_connections'],
                conn_stats['idle_connections'],
                max_conn,
                round(usage_pct, 2)
            ))
            conn.commit()
            print(f"  [OK] DB Health: {status} ({usage_pct:.1f}% connections)")
            
    except Exception as e:
        print(f"[METRICS] Error collecting DB health: {e}")
    finally:
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    # Can be run standalone
    collect_table_stats_after_etl()
    collect_db_health()
    print("\n[METRICS] Metrics collection completed!")

