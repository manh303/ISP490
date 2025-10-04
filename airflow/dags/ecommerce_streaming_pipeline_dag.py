#!/usr/bin/env python3
"""
E-commerce Streaming Pipeline DAG
Automated real-time data collection, processing, and analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import subprocess
import requests
import json
import time

# Default arguments
default_args = {
    'owner': 'ecommerce-dss-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'ecommerce_streaming_pipeline',
    default_args=default_args,
    description='E-commerce Real-time Data Pipeline with Kafka and Analytics',
    schedule_interval=timedelta(hours=1),  # Run every hour
    max_active_runs=1,
    tags=['ecommerce', 'streaming', 'big-data', 'kafka', 'analytics']
)

def check_kafka_health():
    """Check if Kafka is healthy and has topics"""
    try:
        response = requests.get('http://kafka-ui:8080/api/clusters/local/topics', timeout=10)
        if response.status_code == 200:
            topics = response.json()
            topic_count = len(topics.get('topics', []))
            print(f"Kafka health check PASSED - {topic_count} topics found")
            return True
        else:
            raise Exception(f"Kafka UI returned status {response.status_code}")
    except Exception as e:
        print(f"Kafka health check FAILED: {e}")
        raise

def check_data_volume():
    """Check current data volume in Kafka topics"""
    try:
        response = requests.get('http://kafka-ui:8080/api/clusters/local/topics', timeout=10)
        if response.status_code == 200:
            data = response.json()
            total_messages = 0
            topic_stats = {}

            for topic in data.get('topics', []):
                topic_name = topic['name']
                message_count = sum(partition['offsetMax'] for partition in topic['partitions'])
                topic_stats[topic_name] = message_count
                total_messages += message_count

            print(f"Current Kafka Data Volume:")
            print(f"Total Messages: {total_messages:,}")
            for topic, count in topic_stats.items():
                print(f"  {topic}: {count:,} messages")

            # Store metrics for monitoring
            Variable.set("kafka_total_messages", total_messages)
            Variable.set("kafka_topic_stats", json.dumps(topic_stats))

            return topic_stats
        else:
            raise Exception(f"Failed to get topic stats: {response.status_code}")
    except Exception as e:
        print(f"Data volume check failed: {e}")
        raise

def start_api_data_collection():
    """Start API data collection from FakeStore and DummyJSON"""
    import subprocess
    import sys
    import os

    print("Starting API data collection...")

    # Path to collection script
    script_path = "/opt/airflow/data-collection/simple_collector.py"

    try:
        # Run the collector
        result = subprocess.run([
            sys.executable, script_path
        ], capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            print("API data collection completed successfully")
            print("STDOUT:", result.stdout)
        else:
            print("API data collection failed")
            print("STDERR:", result.stderr)
            raise Exception(f"Collection failed with return code {result.returncode}")

    except subprocess.TimeoutExpired:
        raise Exception("API data collection timed out after 5 minutes")
    except Exception as e:
        print(f"Error running API collection: {e}")
        raise

def monitor_real_time_generator():
    """Check if real-time data generator is running"""
    try:
        # Check if generator is producing data by monitoring Kafka
        response = requests.get('http://kafka-ui:8080/api/clusters/local/topics', timeout=10)
        if response.status_code == 200:
            data = response.json()

            # Look for real-time topics
            realtime_topics = ['user_events', 'transactions', 'inventory_updates', 'price_updates']
            active_topics = []

            for topic in data.get('topics', []):
                if topic['name'] in realtime_topics:
                    message_count = sum(partition['offsetMax'] for partition in topic['partitions'])
                    if message_count > 0:
                        active_topics.append(topic['name'])

            print(f"Active real-time topics: {active_topics}")

            if len(active_topics) >= 2:  # At least 2 real-time topics should be active
                print("Real-time data generator is working properly")
                return True
            else:
                print("WARNING: Real-time data generator may not be running")
                return False
        else:
            raise Exception(f"Failed to check real-time topics: {response.status_code}")
    except Exception as e:
        print(f"Real-time generator check failed: {e}")
        raise

def start_analytics_processing():
    """Start or verify analytics processing is running"""
    import subprocess
    import sys

    print("Checking analytics processing...")

    try:
        # For this demo, we'll just verify Kafka connectivity
        # In production, you'd start/check your analytics services
        check_kafka_health()

        print("Analytics processing verification completed")
        return True

    except Exception as e:
        print(f"Analytics processing check failed: {e}")
        raise

def generate_pipeline_report():
    """Generate pipeline status report"""
    try:
        # Get current data volume
        topic_stats = check_data_volume()

        # Calculate some basic metrics
        total_events = topic_stats.get('user_events', 0)
        total_transactions = topic_stats.get('transactions', 0)
        total_products = topic_stats.get('products_raw', 0)

        report = {
            'pipeline_run_time': datetime.now().isoformat(),
            'total_user_events': total_events,
            'total_transactions': total_transactions,
            'total_products': total_products,
            'total_messages': sum(topic_stats.values()),
            'active_topics': len(topic_stats),
            'status': 'healthy' if sum(topic_stats.values()) > 1000 else 'warning'
        }

        print("E-COMMERCE STREAMING PIPELINE REPORT")
        print("=" * 50)
        print(f"Pipeline Run Time: {report['pipeline_run_time']}")
        print(f"Total User Events: {report['total_user_events']:,}")
        print(f"Total Transactions: {report['total_transactions']:,}")
        print(f"Total Products: {report['total_products']:,}")
        print(f"Total Messages: {report['total_messages']:,}")
        print(f"Active Topics: {report['active_topics']}")
        print(f"Pipeline Status: {report['status'].upper()}")

        # Store report for monitoring
        Variable.set("pipeline_last_report", json.dumps(report))

        return report

    except Exception as e:
        print(f"Report generation failed: {e}")
        raise

# Task definitions
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

kafka_health_check = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag
)

api_collection_task = PythonOperator(
    task_id='run_api_data_collection',
    python_callable=start_api_data_collection,
    dag=dag
)

monitor_realtime_task = PythonOperator(
    task_id='monitor_realtime_generator',
    python_callable=monitor_real_time_generator,
    dag=dag
)

analytics_task = PythonOperator(
    task_id='verify_analytics_processing',
    python_callable=start_analytics_processing,
    dag=dag
)

data_volume_check = PythonOperator(
    task_id='check_data_volume',
    python_callable=check_data_volume,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_pipeline_report',
    python_callable=generate_pipeline_report,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Task dependencies
start_task >> kafka_health_check

kafka_health_check >> [api_collection_task, monitor_realtime_task]

[api_collection_task, monitor_realtime_task] >> analytics_task

analytics_task >> data_volume_check

data_volume_check >> generate_report_task

generate_report_task >> end_task

# Additional monitoring tasks (optional)
cleanup_task = BashOperator(
    task_id='cleanup_old_logs',
    bash_command='find /opt/airflow/logs -name "*.log" -mtime +7 -delete || true',
    dag=dag
)

# Connect cleanup as parallel final task
generate_report_task >> cleanup_task >> end_task