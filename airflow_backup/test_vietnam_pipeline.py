#!/usr/bin/env python3
"""
Test Vietnam E-commerce Pipeline DAG
==================================
Simple DAG to test the complete Vietnam e-commerce data flow

Flow:
1. Check data sources → 2. Test Kafka → 3. Test Spark → 4. Validate results

Author: Vietnam DSS Team
Version: 1.0.0
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# External libraries
import pandas as pd
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================================
# DAG CONFIGURATION
# ================================
DAG_ID = "test_vietnam_pipeline"
DAG_DESCRIPTION = "Test Vietnam E-commerce Pipeline - Quick Validation"

# Default arguments
DEFAULT_ARGS = {
    'owner': 'vietnam-test-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

# ================================
# TASK FUNCTIONS
# ================================

def test_data_sources(**context):
    """Test if Vietnamese e-commerce data sources are available"""
    logger.info("🇻🇳 Testing Vietnamese e-commerce data sources...")

    test_results = {}

    # Test data files
    data_paths = [
        '/opt/airflow/data/vietnamese_ecommerce',
        '/opt/airflow/data/integrated_data',
        '/opt/airflow/data/real_datasets'
    ]

    for path in data_paths:
        try:
            if os.path.exists(path):
                files = os.listdir(path)
                csv_files = [f for f in files if f.endswith('.csv')]
                json_files = [f for f in files if f.endswith('.json')]

                test_results[path] = {
                    'status': 'available',
                    'total_files': len(files),
                    'csv_files': len(csv_files),
                    'json_files': len(json_files),
                    'sample_files': files[:3]
                }
            else:
                test_results[path] = {
                    'status': 'missing',
                    'message': f"Directory not found: {path}"
                }
        except Exception as e:
            test_results[path] = {
                'status': 'error',
                'error': str(e)
            }

    # Summary
    available_sources = len([r for r in test_results.values() if r['status'] == 'available'])
    total_sources = len(data_paths)

    summary = {
        'available_sources': available_sources,
        'total_sources': total_sources,
        'coverage_percent': (available_sources / total_sources) * 100,
        'details': test_results,
        'test_timestamp': datetime.now().isoformat()
    }

    logger.info(f"✅ Data sources test: {available_sources}/{total_sources} available")
    return summary

def test_kafka_connection(**context):
    """Test Kafka connection and topics"""
    logger.info("📡 Testing Kafka connection...")

    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import NoBrokersAvailable

        # Test producer
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:29092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1)
            )

            # Send test message
            test_message = {
                'test_id': 'vietnam_pipeline_test',
                'timestamp': datetime.now().isoformat(),
                'message': 'Test message from Vietnam pipeline'
            }

            producer.send('test_topic', value=test_message)
            producer.flush()
            producer.close()

            kafka_test = {
                'producer_status': 'success',
                'test_message_sent': True
            }

        except Exception as e:
            kafka_test = {
                'producer_status': 'failed',
                'producer_error': str(e)
            }

        # Test consumer (quick check)
        try:
            consumer = KafkaConsumer(
                bootstrap_servers='kafka:29092',
                consumer_timeout_ms=5000,
                api_version=(0, 10, 1)
            )

            # Get available topics
            topics = consumer.topics()
            vietnam_topics = [t for t in topics if 'vietnam' in t]

            consumer.close()

            kafka_test['consumer_status'] = 'success'
            kafka_test['available_topics'] = len(topics)
            kafka_test['vietnam_topics'] = len(vietnam_topics)
            kafka_test['vietnam_topic_list'] = list(vietnam_topics)

        except Exception as e:
            kafka_test['consumer_status'] = 'failed'
            kafka_test['consumer_error'] = str(e)

        logger.info(f"📡 Kafka test completed: {kafka_test}")
        return kafka_test

    except ImportError:
        logger.error("Kafka library not available")
        return {'status': 'library_missing', 'error': 'kafka-python not installed'}

def test_spark_processor(**context):
    """Test Spark processor execution"""
    logger.info("⚡ Testing Spark processor...")

    try:
        # Quick test of Spark processor
        test_command = """
python -c "
import sys
sys.path.append('.')
try:
    from simple_spark_processor import VietnamDataProcessor
    print('✅ VietnamDataProcessor imported successfully')

    processor = VietnamDataProcessor()
    print('✅ Processor initialized')

    # Test configuration
    print(f'Kafka config: {processor.kafka_config}')
    print(f'Postgres config: {processor.postgres_config}')

    # Quick connection test
    try:
        if processor.init_postgres_connection():
            print('✅ PostgreSQL connection successful')
        else:
            print('❌ PostgreSQL connection failed')
    except Exception as e:
        print(f'❌ PostgreSQL error: {e}')

    print('✅ Spark processor test completed')

except ImportError as e:
    print(f'❌ Import error: {e}')
except Exception as e:
    print(f'❌ Test error: {e}')
"
"""

        result = subprocess.run(
            ['docker', 'exec', 'ecommerce-dss-project-data-pipeline-1', 'bash', '-c', test_command],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            # Parse output
            output_lines = result.stdout.split('\n')
            success_lines = [line for line in output_lines if '✅' in line]
            error_lines = [line for line in output_lines if '❌' in line]

            spark_test = {
                'status': 'success' if len(error_lines) == 0 else 'partial',
                'success_checks': len(success_lines),
                'error_checks': len(error_lines),
                'output': result.stdout,
                'execution_time': '< 30s'
            }
        else:
            spark_test = {
                'status': 'failed',
                'error': result.stderr,
                'output': result.stdout,
                'return_code': result.returncode
            }

        logger.info(f"⚡ Spark test completed: {spark_test['status']}")
        return spark_test

    except Exception as e:
        logger.error(f"Spark test error: {str(e)}")
        return {'status': 'execution_error', 'error': str(e)}

def test_database_connections(**context):
    """Test database connections"""
    logger.info("🗄️ Testing database connections...")

    db_tests = {}

    # Test PostgreSQL
    try:
        import psycopg2

        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='ecommerce_dss',
            user='dss_user',
            password='dss_password_123'
        )

        cursor = conn.cursor()

        # Test basic query
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        # Test Vietnam DW schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'vietnam_dw'
        """)
        tables = cursor.fetchall()

        cursor.close()
        conn.close()

        db_tests['postgresql'] = {
            'status': 'success',
            'test_query': 'passed',
            'vietnam_dw_tables': len(tables),
            'table_list': [t[0] for t in tables]
        }

    except Exception as e:
        db_tests['postgresql'] = {
            'status': 'failed',
            'error': str(e)
        }

    # Test MongoDB
    try:
        import pymongo

        client = pymongo.MongoClient('mongodb://admin:admin_password@mongodb:27017/')

        # Test connection
        client.admin.command('ping')

        # Test database
        db = client['vietnam_ecommerce_dss']
        collections = db.list_collection_names()

        client.close()

        db_tests['mongodb'] = {
            'status': 'success',
            'collections': len(collections),
            'collection_list': collections[:10]  # First 10
        }

    except Exception as e:
        db_tests['mongodb'] = {
            'status': 'failed',
            'error': str(e)
        }

    # Test Redis
    try:
        import redis

        r = redis.Redis(host='redis', port=6379, db=0)
        r.ping()

        db_tests['redis'] = {
            'status': 'success',
            'ping': 'successful'
        }

    except Exception as e:
        db_tests['redis'] = {
            'status': 'failed',
            'error': str(e)
        }

    # Summary
    successful_dbs = len([db for db in db_tests.values() if db['status'] == 'success'])
    total_dbs = len(db_tests)

    db_summary = {
        'successful_connections': successful_dbs,
        'total_databases': total_dbs,
        'success_rate': (successful_dbs / total_dbs) * 100,
        'database_details': db_tests,
        'test_timestamp': datetime.now().isoformat()
    }

    logger.info(f"🗄️ Database test: {successful_dbs}/{total_dbs} connections successful")
    return db_summary

def run_quick_pipeline_test(**context):
    """Run a quick end-to-end pipeline test"""
    logger.info("🚀 Running quick pipeline test...")

    try:
        # Get results from previous tasks
        data_sources = context['task_instance'].xcom_pull(task_ids='test_data_sources')
        kafka_test = context['task_instance'].xcom_pull(task_ids='test_kafka_connection')
        spark_test = context['task_instance'].xcom_pull(task_ids='test_spark_processor')
        db_test = context['task_instance'].xcom_pull(task_ids='test_database_connections')

        # Calculate overall health score
        scores = []

        # Data sources score
        if data_sources:
            scores.append(data_sources.get('coverage_percent', 0))

        # Kafka score
        if kafka_test:
            kafka_score = 100 if kafka_test.get('producer_status') == 'success' else 0
            scores.append(kafka_score)

        # Spark score
        if spark_test:
            spark_score = 100 if spark_test.get('status') == 'success' else 50 if spark_test.get('status') == 'partial' else 0
            scores.append(spark_score)

        # Database score
        if db_test:
            scores.append(db_test.get('success_rate', 0))

        overall_score = sum(scores) / len(scores) if scores else 0

        # Quick data flow test
        try:
            # Run mini producer test
            quick_producer_test = """
python streaming/kafka_producer_vietnam.py --duration 10 --customer-rate 1 --product-rate 1 --sales-rate 2 --activity-rate 3
"""

            producer_result = subprocess.run(
                quick_producer_test,
                shell=True,
                capture_output=True,
                text=True,
                timeout=15
            )

            data_flow_test = {
                'producer_test': 'success' if producer_result.returncode == 0 else 'failed',
                'producer_output': producer_result.stdout[-200:] if producer_result.stdout else '',
                'data_flow_working': True if producer_result.returncode == 0 else False
            }

        except Exception as e:
            data_flow_test = {
                'producer_test': 'error',
                'error': str(e),
                'data_flow_working': False
            }

        # Compile test report
        test_report = {
            'overall_health_score': round(overall_score, 2),
            'pipeline_status': 'healthy' if overall_score >= 80 else 'degraded' if overall_score >= 60 else 'unhealthy',
            'component_scores': {
                'data_sources': data_sources.get('coverage_percent', 0) if data_sources else 0,
                'kafka': 100 if kafka_test and kafka_test.get('producer_status') == 'success' else 0,
                'spark': 100 if spark_test and spark_test.get('status') == 'success' else 0,
                'databases': db_test.get('success_rate', 0) if db_test else 0
            },
            'data_flow_test': data_flow_test,
            'test_timestamp': datetime.now().isoformat(),
            'recommendations': []
        }

        # Add recommendations
        if overall_score < 80:
            test_report['recommendations'].append("Pipeline health below 80% - investigate failing components")
        if not data_flow_test.get('data_flow_working', False):
            test_report['recommendations'].append("Data flow test failed - check Kafka producer functionality")

        logger.info(f"🎯 Pipeline test completed: {overall_score:.1f}% health score")
        return test_report

    except Exception as e:
        logger.error(f"Pipeline test error: {str(e)}")
        return {
            'overall_health_score': 0,
            'pipeline_status': 'error',
            'error': str(e),
            'test_timestamp': datetime.now().isoformat()
        }

# ================================
# DAG DEFINITION
# ================================

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DAG_DESCRIPTION,
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    catchup=False,
    tags=['vietnam', 'test', 'pipeline', 'validation']
)

# ================================
# TASK DEFINITIONS
# ================================

# Start task
start_task = DummyOperator(
    task_id='start_test',
    dag=dag
)

# Test tasks
test_data_sources_task = PythonOperator(
    task_id='test_data_sources',
    python_callable=test_data_sources,
    dag=dag
)

test_kafka_task = PythonOperator(
    task_id='test_kafka_connection',
    python_callable=test_kafka_connection,
    dag=dag
)

test_spark_task = PythonOperator(
    task_id='test_spark_processor',
    python_callable=test_spark_processor,
    dag=dag
)

test_databases_task = PythonOperator(
    task_id='test_database_connections',
    python_callable=test_database_connections,
    dag=dag
)

# Integration test
quick_pipeline_test_task = PythonOperator(
    task_id='run_quick_pipeline_test',
    python_callable=run_quick_pipeline_test,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end_test',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# ================================
# TASK DEPENDENCIES
# ================================

# Run tests in parallel, then integration test
start_task >> [test_data_sources_task, test_kafka_task, test_spark_task, test_databases_task]
[test_data_sources_task, test_kafka_task, test_spark_task, test_databases_task] >> quick_pipeline_test_task >> end_task

# ================================
# DAG DOCUMENTATION
# ================================

dag.doc_md = """
# Test Vietnam E-commerce Pipeline

## 🎯 Purpose
Quick validation of the Vietnam e-commerce data pipeline components.

## 🧪 Tests Included
1. **Data Sources Test**: Check availability of Vietnamese e-commerce data files
2. **Kafka Connection Test**: Validate Kafka producer/consumer functionality
3. **Spark Processor Test**: Test Spark data processing components
4. **Database Connections Test**: Verify PostgreSQL, MongoDB, Redis connectivity
5. **Quick Pipeline Test**: Run mini end-to-end data flow validation

## 📊 Health Scoring
- **80-100%**: Healthy - All components working well
- **60-79%**: Degraded - Some issues need attention
- **0-59%**: Unhealthy - Major issues require immediate fix

## 🚀 Usage
Trigger manually to validate pipeline health before running full pipeline.

## ⏱️ Expected Runtime
~2-3 minutes for complete test suite
"""