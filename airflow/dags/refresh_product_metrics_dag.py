#!/usr/bin/env python3
"""
Airflow DAG: Refresh Product Metrics Global Table
==================================================
Purpose: Daily refresh of dwh.product_metrics_global table for DSS query optimization
Schedule: Daily at 2:00 AM (after main data pipeline completes)
Dependencies: Requires create_product_metrics_table.sql migration to be run first

This DAG replaces expensive CTE queries in DSS endpoints with pre-aggregated data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'refresh_product_metrics_global',
    default_args=default_args,
    description='Refresh pre-aggregated product metrics for DSS optimization',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['optimization', 'dss', 'materialized-view'],
    max_active_runs=1,
)

# Task 1: Check if product_metrics_global table exists
check_table_exists = PostgresOperator(
    task_id='check_table_exists',
    postgres_conn_id='postgres_dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'dwh' 
            AND table_name = 'product_metrics_global'
        );
    """,
    dag=dag,
)

# Task 2: Refresh product metrics using the stored function
refresh_metrics = PostgresOperator(
    task_id='refresh_product_metrics',
    postgres_conn_id='postgres_dwh',
    sql="""
        SELECT * FROM dwh.refresh_product_metrics_global();
    """,
    autocommit=True,
    dag=dag,
)

# Task 3: Verify refresh succeeded
verify_refresh = PostgresOperator(
    task_id='verify_refresh',
    postgres_conn_id='postgres_dwh',
    sql="""
        -- Check that data was refreshed in last hour
        DO $$
        DECLARE
            last_update TIMESTAMP;
            row_count INTEGER;
        BEGIN
            SELECT MAX(last_updated), COUNT(*) 
            INTO last_update, row_count
            FROM dwh.product_metrics_global;
            
            IF last_update IS NULL THEN
                RAISE EXCEPTION 'Product metrics table is empty after refresh';
            END IF;
            
            IF last_update < NOW() - INTERVAL '1 hour' THEN
                RAISE EXCEPTION 'Product metrics not refreshed: last_updated = %', last_update;
            END IF;
            
            IF row_count < 100 THEN
                RAISE WARNING 'Product metrics table has only % rows - expected more', row_count;
            END IF;
            
            RAISE NOTICE 'Refresh verification passed: % rows, last_updated = %', row_count, last_update;
        END $$;
    """,
    dag=dag,
)

# Task 4: Log refresh statistics
def log_refresh_stats(**context):
    """Log refresh statistics to Airflow logs"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    # Get statistics
    stats_query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(CASE WHEN avg_price > 0 THEN 1 END) as products_with_price,
            COUNT(CASE WHEN total_orders > 0 THEN 1 END) as products_with_orders,
            AVG(avg_price) as avg_product_price,
            MAX(last_updated) as last_refresh,
            AVG(data_freshness_hours) as avg_freshness_hours
        FROM dwh.product_metrics_global;
    """
    
    result = hook.get_first(stats_query)
    
    if result:
        logger.info("=" * 60)
        logger.info("Product Metrics Refresh Statistics")
        logger.info("=" * 60)
        logger.info(f"Total Products: {result[0]}")
        logger.info(f"Products with Price: {result[1]}")
        logger.info(f"Products with Orders: {result[2]}")
        logger.info(f"Average Product Price: {result[3]:.2f}" if result[3] else "Average Product Price: N/A")
        logger.info(f"Last Refresh: {result[4]}")
        logger.info(f"Avg Data Freshness (hours): {result[5]:.2f}" if result[5] else "Avg Data Freshness: N/A")
        logger.info("=" * 60)
    else:
        logger.warning("No statistics available from product_metrics_global")

log_stats = PythonOperator(
    task_id='log_refresh_stats',
    python_callable=log_refresh_stats,
    dag=dag,
)

# Task 5: Analyze table for query optimization
analyze_table = PostgresOperator(
    task_id='analyze_table',
    postgres_conn_id='postgres_dwh',
    sql="""
        -- Update table statistics for query planner
        ANALYZE dwh.product_metrics_global;
    """,
    dag=dag,
)

# Task dependencies
check_table_exists >> refresh_metrics >> verify_refresh >> log_stats >> analyze_table

if __name__ == "__main__":
    dag.cli()
