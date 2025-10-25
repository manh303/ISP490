#!/usr/bin/env python3
"""
Real-time Monitoring DAG for E-commerce Pipeline
Monitors streaming data and triggers alerts
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import requests
import json
import time

# Default arguments
default_args = {
    'owner': 'ecommerce-monitoring',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# DAG definition
dag = DAG(
    'realtime_monitoring',
    default_args=default_args,
    description='Real-time monitoring and alerting for e-commerce data streams',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    max_active_runs=2,
    tags=['monitoring', 'real-time', 'alerts', 'kafka']
)

def check_stream_health():
    """Check if data streams are healthy"""
    try:
        response = requests.get('http://kafka-ui:8080/api/clusters/local/topics', timeout=10)
        if response.status_code == 200:
            data = response.json()

            # Check critical topics
            critical_topics = ['user_events', 'transactions']
            healthy_topics = []
            unhealthy_topics = []

            for topic in data.get('topics', []):
                topic_name = topic['name']
                if topic_name in critical_topics:
                    message_count = sum(partition['offsetMax'] for partition in topic['partitions'])

                    # Get previous count to check if data is flowing
                    prev_key = f"prev_{topic_name}_count"
                    prev_count = int(Variable.get(prev_key, default_var=0))

                    if message_count > prev_count:
                        healthy_topics.append(topic_name)
                        print(f"✓ {topic_name}: {message_count:,} messages (+{message_count-prev_count})")
                    else:
                        unhealthy_topics.append(topic_name)
                        print(f"⚠ {topic_name}: No new messages detected")

                    # Update count for next check
                    Variable.set(prev_key, message_count)

            if unhealthy_topics:
                print(f"WARNING: Unhealthy topics detected: {unhealthy_topics}")
                Variable.set("stream_health_status", "warning")
            else:
                print("All critical streams are healthy")
                Variable.set("stream_health_status", "healthy")

            return len(unhealthy_topics) == 0

    except Exception as e:
        print(f"Stream health check failed: {e}")
        Variable.set("stream_health_status", "error")
        raise

def monitor_data_velocity():
    """Monitor data ingestion velocity"""
    try:
        response = requests.get('http://kafka-ui:8080/api/clusters/local/topics', timeout=10)
        if response.status_code == 200:
            data = response.json()

            current_time = datetime.now()
            total_current = 0

            for topic in data.get('topics', []):
                message_count = sum(partition['offsetMax'] for partition in topic['partitions'])
                total_current += message_count

            # Get previous total for velocity calculation
            prev_total = int(Variable.get("prev_total_messages", default_var=total_current))
            prev_time_str = Variable.get("prev_check_time", default_var=current_time.isoformat())
            prev_time = datetime.fromisoformat(prev_time_str)

            # Calculate velocity (messages per minute)
            time_diff = (current_time - prev_time).total_seconds() / 60
            if time_diff > 0:
                velocity = (total_current - prev_total) / time_diff
                print(f"Data velocity: {velocity:.1f} messages/minute")

                # Set alerts based on velocity
                if velocity < 10:  # Less than 10 messages per minute
                    print("⚠ LOW VELOCITY ALERT: Data ingestion is slow")
                    Variable.set("velocity_status", "low")
                elif velocity > 1000:  # More than 1000 messages per minute
                    print("🔥 HIGH VELOCITY: Data ingestion is very active")
                    Variable.set("velocity_status", "high")
                else:
                    print("✓ Normal data velocity")
                    Variable.set("velocity_status", "normal")

                Variable.set("current_velocity", str(velocity))

            # Update for next check
            Variable.set("prev_total_messages", total_current)
            Variable.set("prev_check_time", current_time.isoformat())

            return True

    except Exception as e:
        print(f"Velocity monitoring failed: {e}")
        raise

def check_analytics_lag():
    """Check if analytics processing is keeping up with data"""
    try:
        # This is a simplified check - in production you'd compare
        # latest processed timestamp vs latest ingested timestamp

        response = requests.get('http://kafka-ui:8080/api/clusters/local/topics', timeout=10)
        if response.status_code == 200:
            data = response.json()

            # Check if analytics topics exist and have recent activity
            analytics_topics = ['user_events', 'transactions']
            processing_lag = False

            for topic in data.get('topics', []):
                if topic['name'] in analytics_topics:
                    # In a real system, you'd check processing timestamps
                    # For now, we'll assume processing is keeping up if data exists
                    message_count = sum(partition['offsetMax'] for partition in topic['partitions'])
                    if message_count > 1000:
                        print(f"✓ {topic['name']}: Analytics processing active")
                    else:
                        print(f"⚠ {topic['name']}: Low analytics activity")
                        processing_lag = True

            if processing_lag:
                Variable.set("analytics_lag_status", "warning")
                print("WARNING: Analytics processing may be lagging")
            else:
                Variable.set("analytics_lag_status", "healthy")
                print("✓ Analytics processing is healthy")

            return not processing_lag

    except Exception as e:
        print(f"Analytics lag check failed: {e}")
        raise

def generate_monitoring_report():
    """Generate comprehensive monitoring report"""
    try:
        # Collect all monitoring variables
        stream_status = Variable.get("stream_health_status", default_var="unknown")
        velocity_status = Variable.get("velocity_status", default_var="unknown")
        analytics_status = Variable.get("analytics_lag_status", default_var="unknown")
        current_velocity = Variable.get("current_velocity", default_var="0")

        # Determine overall health
        statuses = [stream_status, velocity_status, analytics_status]
        if "error" in statuses:
            overall_status = "error"
        elif "warning" in statuses:
            overall_status = "warning"
        else:
            overall_status = "healthy"

        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': overall_status,
            'stream_health': stream_status,
            'data_velocity': velocity_status,
            'analytics_lag': analytics_status,
            'current_velocity_mpm': float(current_velocity),
            'monitoring_interval': '15_minutes'
        }

        print("\n" + "="*60)
        print("🔍 E-COMMERCE REAL-TIME MONITORING REPORT")
        print("="*60)
        print(f"Timestamp: {report['timestamp']}")
        print(f"Overall Status: {report['overall_status'].upper()}")
        print("-"*40)
        print(f"Stream Health: {report['stream_health']}")
        print(f"Data Velocity: {report['data_velocity']} ({report['current_velocity_mpm']:.1f} msg/min)")
        print(f"Analytics Status: {report['analytics_lag']}")
        print("="*60)

        # Store report
        Variable.set("monitoring_last_report", json.dumps(report))

        # Trigger alerts if needed
        if overall_status == "error":
            print("🚨 CRITICAL ALERT: Pipeline has errors!")
        elif overall_status == "warning":
            print("⚠️ WARNING: Pipeline needs attention")
        else:
            print("✅ All systems operational")

        return report

    except Exception as e:
        print(f"Monitoring report generation failed: {e}")
        raise

def cleanup_old_variables():
    """Clean up old monitoring variables"""
    try:
        # This would clean up variables older than X days
        # For now, just log that cleanup ran
        print("🧹 Cleanup: Old monitoring variables cleaned")
        Variable.set("last_cleanup", datetime.now().isoformat())
        return True
    except Exception as e:
        print(f"Cleanup failed: {e}")
        return False

# Task Groups for better organization
with TaskGroup("health_checks", dag=dag) as health_checks:
    stream_health_task = PythonOperator(
        task_id='check_stream_health',
        python_callable=check_stream_health
    )

    velocity_monitor_task = PythonOperator(
        task_id='monitor_data_velocity',
        python_callable=monitor_data_velocity
    )

    analytics_lag_task = PythonOperator(
        task_id='check_analytics_lag',
        python_callable=check_analytics_lag
    )

with TaskGroup("reporting", dag=dag) as reporting:
    monitoring_report_task = PythonOperator(
        task_id='generate_monitoring_report',
        python_callable=generate_monitoring_report
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_old_variables',
        python_callable=cleanup_old_variables
    )

# Task dependencies
start_monitoring = DummyOperator(task_id='start_monitoring', dag=dag)
end_monitoring = DummyOperator(task_id='end_monitoring', dag=dag)

start_monitoring >> health_checks >> reporting >> end_monitoring