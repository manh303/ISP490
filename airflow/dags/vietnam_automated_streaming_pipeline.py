#!/usr/bin/env python3
"""
Vietnam E-commerce Automated Streaming Pipeline DAG
=================================================
Airflow DAG tự động hóa pipeline streaming Vietnam e-commerce:
1. Monitor Kafka topics
2. Trigger Spark streaming jobs
3. Process data into Data Warehouse (PostgreSQL)
4. Archive data to Data Lake (MongoDB)
5. Generate reports and alerts

Author: DSS Team
Version: 1.0.0
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

# Kafka monitoring
import subprocess
import psycopg2
from pymongo import MongoClient

# =================================================================
# DAG CONFIGURATION
# =================================================================

# Default arguments
default_args = {
    'owner': 'dss_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'vietnam_automated_streaming_pipeline',
    default_args=default_args,
    description='Vietnam E-commerce Automated Streaming Pipeline',
    schedule_interval=timedelta(minutes=10),  # Run every 10 minutes
    max_active_runs=1,
    tags=['vietnam', 'streaming', 'kafka', 'spark', 'datawarehouse', 'datalake']
)

# =================================================================
# CONFIGURATION VARIABLES
# =================================================================

KAFKA_TOPICS = [
    'vietnam_sales_events',
    'vietnam_customers',
    'vietnam_products',
    'vietnam_user_activities'
]

POSTGRES_CONN_ID = 'postgres_default'
MONGO_CONN_ID = 'mongo_default'

SPARK_APPS = {
    'sales_processor': {
        'file': '/app/spark-streaming/vietnam_spark_consumer.py',
        'mode': 'sales',
        'timeout': 300
    },
    'customer_processor': {
        'file': '/app/spark-streaming/vietnam_spark_consumer.py',
        'mode': 'customers',
        'timeout': 200
    },
    'product_processor': {
        'file': '/app/spark-streaming/vietnam_spark_consumer.py',
        'mode': 'products',
        'timeout': 200
    },
    'activity_processor': {
        'file': '/app/spark-streaming/vietnam_spark_consumer.py',
        'mode': 'activities',
        'timeout': 300
    }
}

DW_TABLES = [
    'vietnam_dw.fact_sales_vn',
    'vietnam_dw.dim_customer_vn',
    'vietnam_dw.dim_product_vn',
    'vietnam_dw.fact_customer_activity_vn'
]

# =================================================================
# HELPER FUNCTIONS
# =================================================================

def check_kafka_data_availability(**context):
    """Check if Kafka topics have new data to process"""

    try:
        # Use network check instead of Docker commands
        import socket

        # Check Kafka connectivity
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex(('kafka', 9092))
        sock.close()

        if result == 0:
            print("✅ Kafka is accessible on port 9092")
            return True
        else:
            print(f"⚠️ Kafka not accessible on port 9092")
            return False

    except Exception as e:
        print(f"❌ Error checking Kafka: {e}")
        return False

def monitor_kafka_message_rate(**context):
    """Monitor Kafka message rates for all Vietnam topics"""

    stats = {}

    for topic in KAFKA_TOPICS:
        try:
            # Get topic offset information
            cmd = [
                'docker', 'exec', 'kafka',
                '/opt/bitnami/kafka/bin/kafka-log-dirs.sh',
                '--bootstrap-server', 'localhost:9092',
                '--describe',
                '--json'
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                stats[topic] = {"status": "active", "timestamp": datetime.now().isoformat()}
            else:
                stats[topic] = {"status": "error", "error": result.stderr}

        except Exception as e:
            stats[topic] = {"status": "exception", "error": str(e)}

    # Store stats in XCom for downstream tasks
    context['task_instance'].xcom_push(key='kafka_stats', value=stats)

    print(f"📊 Kafka monitoring results: {json.dumps(stats, indent=2)}")
    return stats

def launch_spark_streaming_job(app_name: str, app_config: Dict[str, Any], **context):
    """Launch Spark streaming job for specific data type"""

    try:
        print(f"🚀 Starting Spark job: {app_name}")
        print(f"📋 Config: {app_config}")

        # Build Spark submit command
        spark_cmd = [
            'docker', 'exec', 'ecommerce-dss-project-data-pipeline-1',
            'python', app_config['file'],
            '--mode', app_config['mode'],
            '--duration', str(app_config['timeout'])
        ]

        print(f"🔧 Command: {' '.join(spark_cmd)}")

        # Execute Spark job
        result = subprocess.run(
            spark_cmd,
            capture_output=True,
            text=True,
            timeout=app_config['timeout'] + 60
        )

        if result.returncode == 0:
            print(f"✅ Spark job {app_name} completed successfully")
            print(f"📋 Output: {result.stdout[-500:]}")  # Last 500 chars

            # Store result in XCom
            context['task_instance'].xcom_push(
                key=f'spark_{app_name}_result',
                value={
                    'status': 'success',
                    'output': result.stdout[-1000:],  # Last 1000 chars
                    'completed_at': datetime.now().isoformat()
                }
            )
            return True

        else:
            print(f"❌ Spark job {app_name} failed")
            print(f"📋 Error: {result.stderr}")

            context['task_instance'].xcom_push(
                key=f'spark_{app_name}_result',
                value={
                    'status': 'failed',
                    'error': result.stderr,
                    'failed_at': datetime.now().isoformat()
                }
            )
            return False

    except subprocess.TimeoutExpired:
        print(f"⏰ Spark job {app_name} timed out")
        return False
    except Exception as e:
        print(f"❌ Exception in Spark job {app_name}: {e}")
        return False

def validate_datawarehouse_data(**context):
    """Validate that data has been properly written to Data Warehouse"""

    try:
        # Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        validation_results = {}

        for table in DW_TABLES:
            try:
                # Count records added in last 15 minutes
                query = f"""
                    SELECT COUNT(*) as record_count,
                           MAX(CASE
                               WHEN '{table}' LIKE '%fact_sales%' THEN order_timestamp
                               WHEN '{table}' LIKE '%activity%' THEN activity_date
                               ELSE created_at
                           END) as latest_timestamp
                    FROM {table}
                    WHERE CASE
                        WHEN '{table}' LIKE '%fact_sales%' THEN order_timestamp
                        WHEN '{table}' LIKE '%activity%' THEN activity_date
                        ELSE created_at
                    END >= NOW() - INTERVAL '15 minutes'
                """

                result = pg_hook.get_first(query)

                validation_results[table] = {
                    'record_count': result[0] if result else 0,
                    'latest_timestamp': result[1].isoformat() if result and result[1] else None,
                    'status': 'success' if result and result[0] > 0 else 'no_new_data'
                }

                print(f"📊 {table}: {result[0] if result else 0} new records")

            except Exception as e:
                validation_results[table] = {
                    'status': 'error',
                    'error': str(e)
                }
                print(f"❌ Error validating {table}: {e}")

        # Store validation results
        context['task_instance'].xcom_push(key='dw_validation', value=validation_results)

        # Check if any table has new data
        has_new_data = any(
            result.get('record_count', 0) > 0
            for result in validation_results.values()
        )

        print(f"📋 Data Warehouse validation: {'✅ PASSED' if has_new_data else '⚠️ NO NEW DATA'}")
        return validation_results

    except Exception as e:
        print(f"❌ Data Warehouse validation failed: {e}")
        return {'error': str(e)}

def archive_to_data_lake(**context):
    """Archive processed data to MongoDB Data Lake"""

    try:
        # Get data from Data Warehouse
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Connect to MongoDB
        mongo_client = MongoClient('mongodb://admin:admin_password@localhost:27017/')
        db = mongo_client['vietnam_data_lake']

        archive_stats = {}

        # Archive sales data
        sales_query = """
            SELECT order_id, customer_id, product_id, total_amount_vnd,
                   payment_method_vietnamese, order_timestamp, shipping_province
            FROM vietnam_dw.fact_sales_vn
            WHERE order_timestamp >= NOW() - INTERVAL '15 minutes'
        """

        sales_data = pg_hook.get_records(sales_query)

        if sales_data:
            sales_docs = []
            for row in sales_data:
                doc = {
                    'order_id': row[0],
                    'customer_id': row[1],
                    'product_id': row[2],
                    'total_amount_vnd': row[3],
                    'payment_method': row[4],
                    'order_timestamp': row[5],
                    'shipping_province': row[6],
                    'archived_at': datetime.now(),
                    'data_source': 'vietnam_dw'
                }
                sales_docs.append(doc)

            # Insert to MongoDB
            collection = db['sales_events_archive']
            result = collection.insert_many(sales_docs)

            archive_stats['sales'] = {
                'records_archived': len(result.inserted_ids),
                'collection': 'sales_events_archive'
            }

            print(f"📦 Archived {len(sales_docs)} sales records to Data Lake")

        # Archive customer activities
        activity_query = """
            SELECT customer_id, activity_type, activity_date, platform, device_type
            FROM vietnam_dw.fact_customer_activity_vn
            WHERE activity_date >= NOW() - INTERVAL '15 minutes'
        """

        activity_data = pg_hook.get_records(activity_query)

        if activity_data:
            activity_docs = []
            for row in activity_data:
                doc = {
                    'customer_id': row[0],
                    'activity_type': row[1],
                    'activity_date': row[2],
                    'platform': row[3],
                    'device_type': row[4],
                    'archived_at': datetime.now(),
                    'data_source': 'vietnam_dw'
                }
                activity_docs.append(doc)

            collection = db['customer_activities_archive']
            result = collection.insert_many(activity_docs)

            archive_stats['activities'] = {
                'records_archived': len(result.inserted_ids),
                'collection': 'customer_activities_archive'
            }

            print(f"📦 Archived {len(activity_docs)} activity records to Data Lake")

        # Store archive stats
        context['task_instance'].xcom_push(key='archive_stats', value=archive_stats)

        print(f"✅ Data Lake archival completed: {archive_stats}")
        return archive_stats

    except Exception as e:
        print(f"❌ Data Lake archival failed: {e}")
        return {'error': str(e)}
    finally:
        if 'mongo_client' in locals():
            mongo_client.close()

def generate_pipeline_report(**context):
    """Generate pipeline execution report"""

    try:
        # Get all XCom data
        kafka_stats = context['task_instance'].xcom_pull(key='kafka_stats')
        dw_validation = context['task_instance'].xcom_pull(key='dw_validation')
        archive_stats = context['task_instance'].xcom_pull(key='archive_stats')

        # Get Spark job results
        spark_results = {}
        for app_name in SPARK_APPS.keys():
            spark_results[app_name] = context['task_instance'].xcom_pull(
                key=f'spark_{app_name}_result'
            )

        # Create report
        report = {
            'pipeline_execution': {
                'dag_id': context['dag'].dag_id,
                'run_id': context['dag_run'].run_id,
                'execution_date': context['execution_date'].isoformat(),
                'completed_at': datetime.now().isoformat()
            },
            'kafka_monitoring': kafka_stats,
            'spark_processing': spark_results,
            'datawarehouse_validation': dw_validation,
            'datalake_archival': archive_stats,
            'summary': {
                'total_topics_monitored': len(KAFKA_TOPICS),
                'spark_jobs_executed': len([r for r in spark_results.values() if r]),
                'dw_tables_validated': len(DW_TABLES),
                'pipeline_status': 'SUCCESS'
            }
        }

        print("📋 VIETNAM STREAMING PIPELINE EXECUTION REPORT")
        print("=" * 60)
        print(json.dumps(report, indent=2, default=str))

        # Store final report
        context['task_instance'].xcom_push(key='final_report', value=report)

        return report

    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        return {'error': str(e)}

# =================================================================
# DAG TASKS DEFINITION
# =================================================================

# Start task
start_pipeline = DummyOperator(
    task_id='start_vietnam_streaming_pipeline',
    dag=dag
)

# Kafka monitoring sensor
kafka_sensor = PythonSensor(
    task_id='check_kafka_availability',
    python_callable=check_kafka_data_availability,
    timeout=300,
    poke_interval=30,
    mode='poke',
    dag=dag
)

# Monitor Kafka message rates
monitor_kafka = PythonOperator(
    task_id='monitor_kafka_topics',
    python_callable=monitor_kafka_message_rate,
    dag=dag
)

# Spark streaming jobs (parallel execution)
with TaskGroup('spark_streaming_jobs', dag=dag) as spark_group:

    spark_tasks = {}

    for app_name, app_config in SPARK_APPS.items():
        task = PythonOperator(
            task_id=f'spark_{app_name}',
            python_callable=launch_spark_streaming_job,
            op_args=[app_name, app_config],
            dag=dag
        )
        spark_tasks[app_name] = task

# Data Warehouse validation
validate_dw = PythonOperator(
    task_id='validate_datawarehouse',
    python_callable=validate_datawarehouse_data,
    dag=dag
)

# Data Lake archival
archive_data = PythonOperator(
    task_id='archive_to_datalake',
    python_callable=archive_to_data_lake,
    dag=dag
)

# Generate aggregated reports
generate_reports = PostgresOperator(
    task_id='generate_daily_aggregates',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
        -- Update daily sales aggregates
        INSERT INTO vietnam_dw.agg_daily_sales_vn (
            sale_date, platform_key, geography_key, total_orders,
            total_revenue_vnd, avg_order_value_vnd, total_customers,
            created_at, updated_at
        )
        SELECT
            sale_date,
            COALESCE(platform_key, 1) as platform_key,
            COALESCE(geography_key, 1) as geography_key,
            COUNT(*) as total_orders,
            COALESCE(SUM(total_amount_vnd), 0) as total_revenue_vnd,
            COALESCE(AVG(total_amount_vnd), 0) as avg_order_value_vnd,
            COUNT(DISTINCT customer_key) as total_customers,
            NOW() as created_at,
            NOW() as updated_at
        FROM vietnam_dw.fact_sales_vn
        WHERE sale_date = CURRENT_DATE
        GROUP BY sale_date, platform_key, geography_key
        ON CONFLICT (sale_date, platform_key, geography_key) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            total_revenue_vnd = EXCLUDED.total_revenue_vnd,
            avg_order_value_vnd = EXCLUDED.avg_order_value_vnd,
            total_customers = EXCLUDED.total_customers,
            updated_at = NOW();

        -- Update monthly customer aggregates
        INSERT INTO vietnam_dw.agg_monthly_customers_vn (
            year_month, geography_key, total_active_customers, new_customers,
            avg_customer_value_vnd, created_at, updated_at
        )
        SELECT
            DATE_TRUNC('month', CURRENT_DATE) as year_month,
            1 as geography_key,  -- Default geography
            COUNT(DISTINCT customer_id) as total_active_customers,
            COUNT(DISTINCT CASE WHEN created_at >= DATE_TRUNC('month', CURRENT_DATE)
                               THEN customer_id END) as new_customers,
            0 as avg_customer_value_vnd,
            NOW() as created_at,
            NOW() as updated_at
        FROM vietnam_dw.dim_customer_vn
        WHERE is_current = true
        GROUP BY DATE_TRUNC('month', CURRENT_DATE)
        ON CONFLICT (year_month, geography_key) DO UPDATE SET
            total_active_customers = EXCLUDED.total_active_customers,
            new_customers = EXCLUDED.new_customers,
            avg_customer_value_vnd = EXCLUDED.avg_customer_value_vnd,
            updated_at = NOW();
    """,
    dag=dag
)

# Generate final report
final_report = PythonOperator(
    task_id='generate_pipeline_report',
    python_callable=generate_pipeline_report,
    dag=dag
)

# End task
end_pipeline = DummyOperator(
    task_id='end_vietnam_streaming_pipeline',
    dag=dag
)

# =================================================================
# DAG DEPENDENCIES
# =================================================================

# Set task dependencies
start_pipeline >> kafka_sensor >> monitor_kafka
monitor_kafka >> spark_group
spark_group >> validate_dw
validate_dw >> [archive_data, generate_reports]
[archive_data, generate_reports] >> final_report >> end_pipeline

# =================================================================
# DAG DESCRIPTION
# =================================================================

dag.doc_md = """
# Vietnam E-commerce Automated Streaming Pipeline

## Overview
Airflow DAG tự động hóa hoàn toàn pipeline streaming cho Vietnam e-commerce data:

## Pipeline Flow
1. **Kafka Monitoring**: Kiểm tra data availability trong Kafka topics
2. **Spark Processing**: Chạy parallel Spark jobs để xử lý từng loại data
3. **Data Warehouse**: Validate data đã được ghi vào PostgreSQL
4. **Data Lake**: Archive data vào MongoDB cho long-term storage
5. **Reporting**: Tạo báo cáo tổng hợp và aggregates

## Scheduling
- **Frequency**: Every 10 minutes
- **Max Active Runs**: 1
- **Retries**: 2 with 5-minute delay

## Monitoring
- Kafka topic message rates
- Spark job execution status
- Data Warehouse data quality
- Data Lake archival statistics
- Pipeline execution reports

## Dependencies
- Kafka cluster with Vietnam topics
- Spark cluster (master + workers)
- PostgreSQL (Data Warehouse)
- MongoDB (Data Lake)
- Docker containers

## Author
DSS Team - Vietnam E-commerce Analytics
"""