#!/usr/bin/env python3
"""
Vietnam E-commerce Streaming Pipeline DAG
==========================================
Airflow DAG for orchestrating Vietnam e-commerce real-time streaming pipeline
Manages Kafka producers, Spark consumers, and streaming API

Features:
- Kafka producer orchestration
- Spark streaming management
- API health monitoring
- Data quality validation
- Performance monitoring
- Auto-scaling capabilities

Author: DSS Team
Version: 1.0.0
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# Data processing
import pandas as pd
import requests
import psutil
import redis
import asyncio

# Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================================================
# DAG CONFIGURATION
# ====================================================================

DAG_ID = "vietnam_streaming_pipeline"
DAG_DESCRIPTION = "Real-time streaming pipeline for Vietnam e-commerce data warehouse"

# Every 5 minutes to check and maintain streaming processes
SCHEDULE_INTERVAL = "*/5 * * * *"

DEFAULT_ARGS = {
    'owner': 'vietnam-streaming-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['manhndhe173383@fpt.edu.vn'],
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
    'max_active_runs': 1
}

# Environment variables
KAFKA_SERVERS = Variable.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
STREAMING_API_URL = Variable.get("STREAMING_API_URL", "http://localhost:8001")
SPARK_MASTER_URL = Variable.get("SPARK_MASTER_URL", "spark://spark-master:7077")

# Streaming configuration
PRODUCER_RATES = {
    'customers': int(Variable.get("CUSTOMER_RATE", "5")),
    'products': int(Variable.get("PRODUCT_RATE", "3")),
    'sales': int(Variable.get("SALES_RATE", "20")),
    'activities': int(Variable.get("ACTIVITY_RATE", "50"))
}

# ====================================================================
# UTILITY FUNCTIONS
# ====================================================================

def get_postgres_connection():
    """Get PostgreSQL connection"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        return postgres_hook.get_sqlalchemy_engine()
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        raise

def get_redis_connection():
    """Get Redis connection"""
    try:
        redis_url = Variable.get("REDIS_URL", "redis://redis:6379")
        return redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        raise

def check_service_health(service_url: str, endpoint: str = "/health") -> Dict:
    """Check health of a service"""
    try:
        response = requests.get(f"{service_url}{endpoint}", timeout=10)
        response.raise_for_status()
        return {"status": "healthy", "data": response.json()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

# ====================================================================
# STREAMING MANAGEMENT FUNCTIONS
# ====================================================================

def check_kafka_cluster_health(**context):
    """Check Kafka cluster health and topics"""
    logger.info("🔍 Checking Kafka cluster health...")

    try:
        from kafka import KafkaAdminClient, KafkaConsumer
        from kafka.errors import KafkaError

        # Check Kafka admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVERS.split(','),
            client_id='vietnam_health_check'
        )

        # List topics
        metadata = admin_client.describe_topics()
        topics = list(metadata.keys())

        # Required topics for Vietnam streaming
        required_topics = [
            'vietnam_sales_events',
            'vietnam_customers',
            'vietnam_products',
            'vietnam_user_activities'
        ]

        missing_topics = [topic for topic in required_topics if topic not in topics]

        # Check consumer lag
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVERS.split(','),
            group_id='vietnam_health_consumer',
            auto_offset_reset='latest'
        )

        partitions = []
        for topic in required_topics:
            if topic in topics:
                topic_partitions = consumer.partitions_for_topic(topic)
                if topic_partitions:
                    partitions.extend([(topic, p) for p in topic_partitions])

        # Get consumer lag
        committed = consumer.committed(partitions) if partitions else {}
        end_offsets = consumer.end_offsets(partitions) if partitions else {}

        total_lag = 0
        for partition in partitions:
            committed_offset = committed.get(partition, 0) or 0
            end_offset = end_offsets.get(partition, 0) or 0
            lag = max(0, end_offset - committed_offset)
            total_lag += lag

        consumer.close()
        admin_client.close()

        kafka_health = {
            'cluster_status': 'healthy',
            'available_topics': topics,
            'required_topics': required_topics,
            'missing_topics': missing_topics,
            'total_consumer_lag': total_lag,
            'timestamp': datetime.now().isoformat()
        }

        # Store health status in Redis
        redis_client = get_redis_connection()
        redis_client.setex(
            'kafka_health_status',
            300,  # 5 minutes TTL
            json.dumps(kafka_health)
        )

        context['task_instance'].xcom_push(key='kafka_health', value=kafka_health)

        logger.info(f"✅ Kafka cluster healthy: {len(topics)} topics, lag: {total_lag}")

        if missing_topics:
            logger.warning(f"⚠️ Missing topics: {missing_topics}")

        return kafka_health

    except Exception as e:
        error_msg = f"Kafka health check failed: {str(e)}"
        logger.error(f"❌ {error_msg}")

        kafka_health = {
            'cluster_status': 'unhealthy',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }

        context['task_instance'].xcom_push(key='kafka_health', value=kafka_health)
        raise

def check_streaming_api_health(**context):
    """Check streaming API health"""
    logger.info("🔍 Checking streaming API health...")

    try:
        health_data = check_service_health(STREAMING_API_URL, "/health")

        if health_data["status"] == "healthy":
            api_metrics = health_data["data"]["metrics"]

            streaming_health = {
                'api_status': 'healthy',
                'active_websockets': api_metrics.get('active_websockets', 0),
                'kafka_messages_consumed': api_metrics.get('kafka_messages_consumed', 0),
                'kafka_messages_per_second': api_metrics.get('kafka_messages_per_second', 0),
                'memory_usage_percent': api_metrics.get('memory_usage_percent', 0),
                'cpu_usage_percent': api_metrics.get('cpu_usage_percent', 0),
                'timestamp': datetime.now().isoformat()
            }

            # Store in Redis
            redis_client = get_redis_connection()
            redis_client.setex(
                'streaming_api_health',
                300,
                json.dumps(streaming_health)
            )

            context['task_instance'].xcom_push(key='streaming_api_health', value=streaming_health)

            logger.info(f"✅ Streaming API healthy: {api_metrics.get('active_websockets', 0)} connections")

        else:
            raise Exception(f"API unhealthy: {health_data.get('error', 'Unknown error')}")

        return streaming_health

    except Exception as e:
        error_msg = f"Streaming API health check failed: {str(e)}"
        logger.error(f"❌ {error_msg}")

        streaming_health = {
            'api_status': 'unhealthy',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }

        context['task_instance'].xcom_push(key='streaming_api_health', value=streaming_health)
        raise

def check_spark_cluster_health(**context):
    """Check Spark cluster health"""
    logger.info("🔍 Checking Spark cluster health...")

    try:
        # Check Spark Master UI
        spark_master_ui = SPARK_MASTER_URL.replace('spark://', 'http://').replace(':7077', ':8080')

        response = requests.get(f"{spark_master_ui}/json/", timeout=10)
        response.raise_for_status()
        spark_info = response.json()

        # Check workers
        workers = spark_info.get('workers', [])
        active_workers = [w for w in workers if w['state'] == 'ALIVE']

        # Check running applications
        apps_response = requests.get(f"{spark_master_ui}/api/v1/applications", timeout=10)
        apps_response.raise_for_status()
        applications = apps_response.json()

        # Find Vietnam streaming applications
        vietnam_apps = [app for app in applications
                       if 'vietnam' in app.get('name', '').lower()]

        spark_health = {
            'cluster_status': 'healthy',
            'master_url': SPARK_MASTER_URL,
            'total_workers': len(workers),
            'active_workers': len(active_workers),
            'total_cores': sum(w.get('cores', 0) for w in active_workers),
            'used_cores': sum(w.get('coresused', 0) for w in active_workers),
            'total_memory_mb': sum(w.get('memory', 0) for w in active_workers),
            'used_memory_mb': sum(w.get('memoryused', 0) for w in active_workers),
            'running_applications': len(applications),
            'vietnam_applications': len(vietnam_apps),
            'timestamp': datetime.now().isoformat()
        }

        # Store in Redis
        redis_client = get_redis_connection()
        redis_client.setex(
            'spark_cluster_health',
            300,
            json.dumps(spark_health)
        )

        context['task_instance'].xcom_push(key='spark_health', value=spark_health)

        logger.info(f"✅ Spark cluster healthy: {len(active_workers)} workers, {len(vietnam_apps)} Vietnam apps")

        return spark_health

    except Exception as e:
        error_msg = f"Spark health check failed: {str(e)}"
        logger.error(f"❌ {error_msg}")

        spark_health = {
            'cluster_status': 'unhealthy',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }

        context['task_instance'].xcom_push(key='spark_health', value=spark_health)
        raise

def manage_kafka_producer(**context):
    """Manage Kafka producer processes"""
    logger.info("🚀 Managing Kafka producer...")

    try:
        # Check if producer is already running
        redis_client = get_redis_connection()
        producer_status = redis_client.get('kafka_producer_status')

        if producer_status:
            producer_info = json.loads(producer_status)
            logger.info(f"📊 Producer status: {producer_info}")

        # Check system resources
        memory_usage = psutil.virtual_memory().percent
        cpu_usage = psutil.cpu_percent()

        # Adjust producer rates based on system load
        adjusted_rates = PRODUCER_RATES.copy()

        if memory_usage > 80 or cpu_usage > 80:
            # Reduce rates if system is under pressure
            logger.warning(f"⚠️ High system load: Memory {memory_usage}%, CPU {cpu_usage}%")
            for key in adjusted_rates:
                adjusted_rates[key] = max(1, adjusted_rates[key] // 2)

        # Store adjusted rates
        redis_client.setex(
            'producer_rates',
            3600,  # 1 hour TTL
            json.dumps(adjusted_rates)
        )

        producer_management = {
            'status': 'managed',
            'original_rates': PRODUCER_RATES,
            'adjusted_rates': adjusted_rates,
            'system_memory_percent': memory_usage,
            'system_cpu_percent': cpu_usage,
            'timestamp': datetime.now().isoformat()
        }

        context['task_instance'].xcom_push(key='producer_management', value=producer_management)

        logger.info(f"✅ Producer management completed: {adjusted_rates}")

        return producer_management

    except Exception as e:
        error_msg = f"Producer management failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise

def validate_streaming_data_quality(**context):
    """Validate data quality in streaming pipeline"""
    logger.info("🔍 Validating streaming data quality...")

    try:
        postgres_engine = get_postgres_connection()
        redis_client = get_redis_connection()

        # Check recent data in fact table
        recent_sales_query = """
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN created_at >= NOW() - INTERVAL '5 minutes' THEN 1 END) as recent_records,
            COUNT(DISTINCT customer_key) as unique_customers,
            COUNT(DISTINCT product_key) as unique_products,
            SUM(CASE WHEN net_sales_amount_vnd > 0 THEN 1 ELSE 0 END) as valid_sales,
            MAX(created_at) as latest_record
        FROM vietnam_dw.fact_sales_vn
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        """

        df_sales = pd.read_sql(recent_sales_query, postgres_engine)

        if not df_sales.empty:
            row = df_sales.iloc[0]

            sales_quality = {
                'total_records_1h': int(row['total_records']) if row['total_records'] else 0,
                'recent_records_5m': int(row['recent_records']) if row['recent_records'] else 0,
                'unique_customers': int(row['unique_customers']) if row['unique_customers'] else 0,
                'unique_products': int(row['unique_products']) if row['unique_products'] else 0,
                'valid_sales': int(row['valid_sales']) if row['valid_sales'] else 0,
                'latest_record': str(row['latest_record']) if row['latest_record'] else None,
                'data_freshness_score': min(100, (int(row['recent_records']) or 0) * 5),  # 5 points per recent record
                'data_completeness_score': min(100, (int(row['valid_sales']) or 0) / max(1, int(row['total_records']) or 1) * 100)
            }
        else:
            sales_quality = {
                'total_records_1h': 0,
                'recent_records_5m': 0,
                'unique_customers': 0,
                'unique_products': 0,
                'valid_sales': 0,
                'latest_record': None,
                'data_freshness_score': 0,
                'data_completeness_score': 0
            }

        # Check customer dimension updates
        customer_quality_query = """
        SELECT
            COUNT(*) as total_customers,
            COUNT(CASE WHEN created_at >= NOW() - INTERVAL '1 hour' THEN 1 END) as recent_customers,
            COUNT(CASE WHEN is_current = true THEN 1 END) as current_customers
        FROM vietnam_dw.dim_customer_vn
        """

        df_customers = pd.read_sql(customer_quality_query, postgres_engine)
        customer_row = df_customers.iloc[0] if not df_customers.empty else {}

        # Check product dimension updates
        product_quality_query = """
        SELECT
            COUNT(*) as total_products,
            COUNT(CASE WHEN created_at >= NOW() - INTERVAL '1 hour' THEN 1 END) as recent_products,
            COUNT(CASE WHEN is_current = true THEN 1 END) as current_products
        FROM vietnam_dw.dim_product_vn
        """

        df_products = pd.read_sql(product_quality_query, postgres_engine)
        product_row = df_products.iloc[0] if not df_products.empty else {}

        # Overall quality assessment
        overall_quality_score = (
            sales_quality['data_freshness_score'] * 0.5 +
            sales_quality['data_completeness_score'] * 0.3 +
            min(100, (customer_row.get('recent_customers', 0) or 0) * 10) * 0.1 +
            min(100, (product_row.get('recent_products', 0) or 0) * 10) * 0.1
        )

        quality_report = {
            'overall_quality_score': round(overall_quality_score, 2),
            'sales_quality': sales_quality,
            'customer_quality': {
                'total_customers': int(customer_row.get('total_customers', 0) or 0),
                'recent_customers': int(customer_row.get('recent_customers', 0) or 0),
                'current_customers': int(customer_row.get('current_customers', 0) or 0)
            },
            'product_quality': {
                'total_products': int(product_row.get('total_products', 0) or 0),
                'recent_products': int(product_row.get('recent_products', 0) or 0),
                'current_products': int(product_row.get('current_products', 0) or 0)
            },
            'validation_timestamp': datetime.now().isoformat()
        }

        # Store quality report
        redis_client.setex(
            'streaming_data_quality',
            1800,  # 30 minutes TTL
            json.dumps(quality_report)
        )

        context['task_instance'].xcom_push(key='data_quality', value=quality_report)

        logger.info(f"✅ Data quality validation completed: {overall_quality_score:.1f}% overall score")

        # Alert if quality is low
        if overall_quality_score < 70:
            logger.warning(f"⚠️ Low data quality detected: {overall_quality_score:.1f}%")

        return quality_report

    except Exception as e:
        error_msg = f"Data quality validation failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise

def generate_streaming_report(**context):
    """Generate comprehensive streaming pipeline report"""
    logger.info("📊 Generating streaming pipeline report...")

    try:
        # Collect data from previous tasks
        kafka_health = context['task_instance'].xcom_pull(key='kafka_health', task_ids='check_kafka_health')
        streaming_api_health = context['task_instance'].xcom_pull(key='streaming_api_health', task_ids='check_streaming_api_health')
        spark_health = context['task_instance'].xcom_pull(key='spark_health', task_ids='check_spark_health')
        data_quality = context['task_instance'].xcom_pull(key='data_quality', task_ids='validate_data_quality')
        producer_management = context['task_instance'].xcom_pull(key='producer_management', task_ids='manage_producer')

        # Calculate overall pipeline health
        health_scores = []

        if kafka_health and kafka_health.get('cluster_status') == 'healthy':
            health_scores.append(100)
        else:
            health_scores.append(0)

        if streaming_api_health and streaming_api_health.get('api_status') == 'healthy':
            health_scores.append(100)
        else:
            health_scores.append(0)

        if spark_health and spark_health.get('cluster_status') == 'healthy':
            health_scores.append(100)
        else:
            health_scores.append(0)

        if data_quality:
            health_scores.append(data_quality.get('overall_quality_score', 0))

        overall_health_score = sum(health_scores) / len(health_scores) if health_scores else 0

        # Create comprehensive report
        streaming_report = {
            'report_id': f"stream_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'pipeline_name': 'Vietnam E-commerce Streaming Pipeline',
            'overall_health_score': round(overall_health_score, 2),
            'health_status': 'healthy' if overall_health_score >= 80 else 'degraded' if overall_health_score >= 60 else 'unhealthy',
            'components': {
                'kafka_cluster': kafka_health,
                'streaming_api': streaming_api_health,
                'spark_cluster': spark_health,
                'data_quality': data_quality,
                'producer_management': producer_management
            },
            'recommendations': [],
            'generated_at': datetime.now().isoformat()
        }

        # Add recommendations based on health status
        if overall_health_score < 80:
            streaming_report['recommendations'].append("Monitor system resources and consider scaling")

        if kafka_health and kafka_health.get('total_consumer_lag', 0) > 1000:
            streaming_report['recommendations'].append("High Kafka consumer lag detected - investigate processing bottlenecks")

        if data_quality and data_quality.get('overall_quality_score', 0) < 70:
            streaming_report['recommendations'].append("Data quality issues detected - review data sources and transformations")

        # Store report
        redis_client = get_redis_connection()
        redis_client.setex(
            'streaming_pipeline_report',
            3600,  # 1 hour TTL
            json.dumps(streaming_report)
        )

        context['task_instance'].xcom_push(key='streaming_report', value=streaming_report)

        logger.info(f"✅ Streaming report generated: {overall_health_score:.1f}% health score")

        return streaming_report

    except Exception as e:
        error_msg = f"Report generation failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise

# ====================================================================
# DAG DEFINITION
# ====================================================================

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DAG_DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    tags=['vietnam', 'streaming', 'kafka', 'spark', 'real-time']
)

# ====================================================================
# TASK DEFINITIONS
# ====================================================================

# Start task
start_task = DummyOperator(
    task_id='start_streaming_pipeline',
    dag=dag
)

# Health check tasks
check_kafka_health_task = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_cluster_health,
    dag=dag
)

check_streaming_api_health_task = PythonOperator(
    task_id='check_streaming_api_health',
    python_callable=check_streaming_api_health,
    dag=dag
)

check_spark_health_task = PythonOperator(
    task_id='check_spark_health',
    python_callable=check_spark_cluster_health,
    dag=dag
)

# Management tasks
manage_producer_task = PythonOperator(
    task_id='manage_producer',
    python_callable=manage_kafka_producer,
    dag=dag
)

# Data quality validation
validate_data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_streaming_data_quality,
    dag=dag
)

# Report generation
generate_report_task = PythonOperator(
    task_id='generate_streaming_report',
    python_callable=generate_streaming_report,
    dag=dag
)

# Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup_resources',
    bash_command="""
    echo "🧹 Cleaning up streaming pipeline resources..."

    # Clean old checkpoint files (older than 7 days)
    find /app/streaming/checkpoints -type f -mtime +7 -delete || true

    # Clean old log files
    find /opt/airflow/logs -name "vietnam_streaming_pipeline*" -mtime +3 -delete || true

    echo "✅ Cleanup completed"
    """,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end_streaming_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# ====================================================================
# TASK DEPENDENCIES
# ====================================================================

# Main pipeline flow
start_task >> [
    check_kafka_health_task,
    check_streaming_api_health_task,
    check_spark_health_task
]

# Parallel health checks feed into management
[
    check_kafka_health_task,
    check_streaming_api_health_task,
    check_spark_health_task
] >> manage_producer_task

# Producer management and data quality can run in parallel
manage_producer_task >> validate_data_quality_task

# All components feed into report generation
[
    check_kafka_health_task,
    check_streaming_api_health_task,
    check_spark_health_task,
    validate_data_quality_task,
    manage_producer_task
] >> generate_report_task

# Cleanup and finish
generate_report_task >> cleanup_task >> end_task

# ====================================================================
# DAG DOCUMENTATION
# ====================================================================

dag.doc_md = """
# Vietnam E-commerce Streaming Pipeline

## Overview
This DAG orchestrates the real-time streaming pipeline for Vietnam e-commerce data warehouse.
It manages Kafka producers, Spark consumers, streaming APIs, and monitors data quality.

## Components Managed
1. **Kafka Cluster**: Topic management and health monitoring
2. **Streaming API**: WebSocket and HTTP endpoints for real-time data
3. **Spark Cluster**: Structured streaming consumers for data warehouse
4. **Data Quality**: Validation and monitoring of streaming data

## Monitoring
- **Health Checks**: All components checked every 5 minutes
- **Performance Metrics**: CPU, memory, and throughput monitoring
- **Data Quality**: Freshness, completeness, and accuracy validation
- **Auto-scaling**: Dynamic adjustment of producer rates based on system load

## Data Flow
```
Kafka Topics → Spark Streaming → Vietnam DW → Streaming API → Dashboards
```

## Alerts
- System health degradation
- Data quality issues
- High resource utilization
- Consumer lag warnings

## Manual Operations
To manually trigger pipeline components:
1. Start Kafka producer: `python streaming/kafka_producer_vietnam.py`
2. Start Spark consumer: `python data-pipeline/spark-streaming/vietnam_spark_consumer.py`
3. Start streaming API: `python backend/app/streaming_api.py`

## Configuration
- Producer rates can be adjusted via Airflow Variables
- System thresholds configurable in DAG code
- Auto-scaling parameters tunable

---
**Maintainer**: Vietnam E-commerce DSS Team
**Version**: 1.0.0
"""