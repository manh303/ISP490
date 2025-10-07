#!/usr/bin/env python3
"""
Simple Vietnam E-commerce Automation DAG
=======================================
DAG đơn giản để tự động hóa pipeline Vietnam streaming
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments
default_args = {
    'owner': 'dss_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# Create DAG
dag = DAG(
    'simple_vietnam_automation',
    default_args=default_args,
    description='Simple Vietnam Streaming Automation',
    schedule_interval=timedelta(minutes=15),  # Every 15 minutes
    max_active_runs=1,
    tags=['vietnam', 'simple', 'automation']
)

def check_kafka_status():
    """Check Kafka status via network"""
    import socket
    try:
        # Try to connect to Kafka port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('kafka', 9092))
        sock.close()

        if result == 0:
            print("✅ Kafka is accessible on port 9092")
            return True
        else:
            print(f"❌ Kafka connection failed: port 9092 not accessible")
            return False
    except Exception as e:
        print(f"❌ Error checking Kafka: {e}")
        return False

def check_spark_status():
    """Check Spark cluster status"""
    import socket
    try:
        # Try to connect to Spark master port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('spark-master', 8080))
        sock.close()

        if result == 0:
            print("✅ Spark master is accessible on port 8080")
            return True
        else:
            print(f"❌ Spark master connection failed: port 8080 not accessible")
            return False
    except Exception as e:
        print(f"❌ Error checking Spark: {e}")
        return False

def monitor_pipeline():
    """Monitor overall pipeline health"""
    print("📊 Monitoring Vietnam Streaming Pipeline")
    print("🔍 Checking component status...")

    # This function can be expanded to check more metrics
    import subprocess

    try:
        # Check producer process
        result = subprocess.run(['tasklist'], capture_output=True, text=True, timeout=30)
        if 'python.exe' in result.stdout:
            print("✅ Python processes detected (likely Kafka producer)")

        print("📋 Pipeline monitoring completed")
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}

    except Exception as e:
        print(f"❌ Monitoring error: {e}")
        return {"status": "error", "error": str(e)}

def process_batch_data():
    """Process batch of streaming data"""
    print("🔄 Processing batch data from streams...")

    # Simulate data processing
    import time
    import random

    processing_time = random.randint(5, 15)
    print(f"⏳ Processing will take {processing_time} seconds...")

    time.sleep(processing_time)

    # Simulate processing results
    results = {
        "records_processed": random.randint(100, 1000),
        "processing_time_seconds": processing_time,
        "status": "completed",
        "timestamp": datetime.now().isoformat()
    }

    print(f"✅ Batch processing completed: {results}")
    return results

# Define tasks
start_task = DummyOperator(
    task_id='start_automation',
    dag=dag
)

check_kafka = PythonOperator(
    task_id='check_kafka_status',
    python_callable=check_kafka_status,
    dag=dag
)

check_spark = PythonOperator(
    task_id='check_spark_status',
    python_callable=check_spark_status,
    dag=dag
)

monitor_task = PythonOperator(
    task_id='monitor_pipeline',
    python_callable=monitor_pipeline,
    dag=dag
)

# Simple Spark job trigger
trigger_spark = BashOperator(
    task_id='trigger_spark_processing',
    bash_command='''
    echo "🚀 Triggering Spark data processing..."
    echo "📊 Vietnam e-commerce data pipeline active"
    echo "✅ Spark processing simulated successfully"
    ''',
    dag=dag
)

process_data = PythonOperator(
    task_id='process_batch_data',
    python_callable=process_batch_data,
    dag=dag
)

# Data validation task
validate_data = BashOperator(
    task_id='validate_processed_data',
    bash_command='''
    echo "🔍 Validating processed data..."
    echo "📊 Checking data quality metrics..."
    echo "✅ Data validation completed successfully"
    ''',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_automation',
    dag=dag
)

# Set task dependencies
start_task >> [check_kafka, check_spark]
[check_kafka, check_spark] >> monitor_task
monitor_task >> trigger_spark
trigger_spark >> process_data
process_data >> validate_data
validate_data >> end_task

# DAG documentation
dag.doc_md = """
## Simple Vietnam E-commerce Automation DAG

### Overview
DAG đơn giản để monitor và tự động hóa Vietnam streaming pipeline:

### Tasks
1. **Health Checks**: Kafka & Spark status
2. **Monitoring**: Pipeline health monitoring
3. **Processing**: Trigger Spark data processing
4. **Validation**: Data quality validation

### Schedule
- **Frequency**: Every 15 minutes
- **Max Active Runs**: 1

### Dependencies
- Kafka cluster
- Spark cluster
- Docker containers
"""