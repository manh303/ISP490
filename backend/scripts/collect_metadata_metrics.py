#!/usr/bin/env python3
"""
Metadata Metrics Collector
Collects and stores metrics about database health, table stats, etc.
Run this periodically (e.g., every 15 minutes) via cron or scheduler
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from datetime import datetime
import time

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# ============================================================
# 1. COLLECT DATABASE HEALTH
# ============================================================
def collect_db_health():
    """Collect database connection and performance metrics"""
    print("[INFO] Collecting database health metrics...")
    conn = get_conn()
    try:
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
            
            # Calculate usage
            usage_pct = (conn_stats['total_connections'] / max_conn * 100) if max_conn > 0 else 0
            
            # Get slow queries count (queries > 1 second)
            cur.execute("""
                SELECT COUNT(*) as slow_queries
                FROM pg_stat_activity
                WHERE state = 'active' 
                  AND query_start < NOW() - INTERVAL '1 second'
                  AND datname = current_database();
            """)
            slow_queries = cur.fetchone()['slow_queries']
            
            # Determine status
            status = 'HEALTHY'
            if usage_pct > 90:
                status = 'DEGRADED'
            elif usage_pct > 95:
                status = 'DOWN'
            
            # Insert health record
            cur.execute("""
                INSERT INTO meta.db_connection_health (
                    check_time, host, port, database_name, status,
                    active_connections, idle_connections, max_connections,
                    connection_usage_pct, slow_queries_count
                )
                VALUES (
                    NOW(), %s, %s, current_database(), %s,
                    %s, %s, %s, %s, %s
                );
            """, (
                os.getenv('DB_HOST', 'localhost'),
                int(os.getenv('DB_PORT', 5432)),
                status,
                conn_stats['active_connections'],
                conn_stats['idle_connections'],
                max_conn,
                round(usage_pct, 2),
                slow_queries
            ))
            conn.commit()
            print(f"  [OK] Database health: {status} ({usage_pct:.1f}% connections used)")
    finally:
        conn.close()

# ============================================================
# 2. COLLECT TABLE STATISTICS
# ============================================================
def collect_table_stats():
    """Collect row count and size for all tables"""
    print("[INFO] Collecting table statistics...")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all tables from dwh, ml schemas
            cur.execute("""
                SELECT 
                    schemaname as schema_name,
                    tablename as table_name,
                    pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes,
                    pg_table_size(schemaname||'.'||tablename) as table_size_bytes,
                    pg_indexes_size(schemaname||'.'||tablename) as indexes_size_bytes
                FROM pg_tables
                WHERE schemaname IN ('dwh', 'ml', 'staging', 'ods')
                ORDER BY schemaname, tablename;
            """)
            tables = cur.fetchall()
            
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
                    
                    # Get last modified time (approximate)
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
                    
                    # Insert stats (use size_bytes for total size)
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
                        last_modified
                    ))
                    conn.commit()  # Commit after each table
                except Exception as e:
                    print(f"  [WARN] Failed to collect stats for {schema_name}.{table_name}: {e}")
                    conn.rollback()
            print(f"  [OK] Collected stats for {len(tables)} tables")
    finally:
        conn.close()

# ============================================================
# 3. CHECK DATA FRESHNESS
# ============================================================
def check_data_freshness():
    """Check if data is fresh and create alerts if stale"""
    print("[INFO] Checking data freshness...")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check critical tables
            critical_tables = [
                ('dwh', 'fact_product_daily', 24),  # Should update daily
                ('dwh', 'fact_review', 24),
                ('ml', 'fact_review_sentiment', 48),
            ]
            
            for schema, table, max_age_hours in critical_tables:
                cur.execute("""
                    SELECT 
                        last_loaded_at,
                        EXTRACT(EPOCH FROM (NOW() - last_loaded_at))/3600 as age_hours
                    FROM meta.table_stats
                    WHERE schema_name = %s 
                      AND table_name = %s 
                      AND snapshot_date = CURRENT_DATE;
                """, (schema, table))
                
                result = cur.fetchone()
                if not result or not result['last_loaded_at']:
                    print(f"  [WARN] {schema}.{table}: No freshness data")
                    continue
                
                age_hours = result['age_hours']
                if age_hours > max_age_hours:
                    print(f"  [WARN] {schema}.{table}: STALE ({age_hours:.1f} hours old)")
                    
                    # Create data quality issue
                    cur.execute("""
                        INSERT INTO meta.data_quality_issue (
                            schema_name, table_name, issue_type, severity, status,
                            issue_description, detected_at
                        )
                        VALUES (%s, %s, 'STALE_DATA', 'HIGH', 'OPEN', %s, NOW())
                        ON CONFLICT DO NOTHING;
                    """, (
                        schema, table,
                        f"Data is {age_hours:.1f} hours old (threshold: {max_age_hours}h)"
                    ))
                else:
                    print(f"  [OK] {schema}.{table}: Fresh ({age_hours:.1f} hours old)")
            
            conn.commit()
    finally:
        conn.close()

# ============================================================
# 4. COLLECT STORAGE USAGE
# ============================================================
def collect_storage_usage():
    """Collect detailed storage usage metrics"""
    print("[INFO] Collecting storage usage...")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    pg_stat_user_tables.schemaname as schema_name,
                    pg_stat_user_tables.relname as table_name,
                    pg_total_relation_size(pg_stat_user_tables.schemaname||'.'||pg_stat_user_tables.relname) as total_size_bytes,
                    pg_table_size(pg_stat_user_tables.schemaname||'.'||pg_stat_user_tables.relname) as table_size_bytes,
                    pg_indexes_size(pg_stat_user_tables.schemaname||'.'||pg_stat_user_tables.relname) as indexes_size_bytes,
                    n_live_tup as row_count,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                WHERE pg_stat_user_tables.schemaname IN ('dwh', 'ml', 'staging')
                ORDER BY pg_total_relation_size(pg_stat_user_tables.schemaname||'.'||pg_stat_user_tables.relname) DESC;
            """)
            tables = cur.fetchall()
            
            for table in tables:
                avg_row_size = (
                    int(table['table_size_bytes'] / table['row_count'])
                    if table['row_count'] > 0 else 0
                )
                
                last_vacuum = table['last_vacuum'] or table['last_autovacuum']
                last_analyze = table['last_analyze'] or table['last_autoanalyze']
                
                cur.execute("""
                    INSERT INTO meta.storage_usage (
                        check_time, schema_name, table_name,
                        table_size_bytes, indexes_size_bytes, total_size_bytes,
                        row_count, avg_row_size_bytes, last_vacuum, last_analyze
                    )
                    VALUES (
                        NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s
                    );
                """, (
                    table['schema_name'], table['table_name'],
                    table['table_size_bytes'], table['indexes_size_bytes'],
                    table['total_size_bytes'], table['row_count'],
                    avg_row_size, last_vacuum, last_analyze
                ))
            
            conn.commit()
            print(f"  [OK] Collected storage usage for {len(tables)} tables")
    finally:
        conn.close()

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("METADATA METRICS COLLECTOR")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    start_time = time.time()
    
    try:
        collect_db_health()
        collect_table_stats()
        check_data_freshness()
        collect_storage_usage()
        
        elapsed = time.time() - start_time
        print()
        print(f"[OK] Collection completed in {elapsed:.2f} seconds")
        
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())


