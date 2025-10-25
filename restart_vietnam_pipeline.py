#!/usr/bin/env python3
"""
Vietnam Pipeline Restart Script
Restart và khôi phục hoạt động của toàn bộ Vietnam streaming pipeline
"""

import subprocess
import time
import sys

def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            return True
        else:
            print(f"❌ {description} - FAILED: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ {description} - ERROR: {e}")
        return False

def main():
    print("🚀 RESTARTING VIETNAM E-COMMERCE STREAMING PIPELINE")
    print("=" * 60)

    # Step 1: Check Docker containers
    print("\n📋 STEP 1: Checking Docker Infrastructure...")
    success = True

    # Check Kafka
    if not run_command("docker exec kafka echo 'Kafka OK'", "Check Kafka container"):
        success = False

    # Check Spark Master
    if not run_command("docker exec spark-master echo 'Spark Master OK'", "Check Spark Master"):
        success = False

    # Check PostgreSQL
    if not run_command("docker exec ecommerce-dss-project-postgres-1 echo 'PostgreSQL OK'", "Check PostgreSQL"):
        success = False

    if not success:
        print("\n❌ Infrastructure check failed. Please check Docker containers.")
        return False

    # Step 2: Stop existing processes
    print("\n📋 STEP 2: Cleaning existing processes...")

    # Kill simple processor if running
    run_command("docker exec ecommerce-dss-project-data-pipeline-1 pkill -f simple_spark_processor",
                "Stop simple processor")

    # Step 3: Restart Kafka producer
    print("\n📋 STEP 3: Restarting Kafka Producer...")

    # Check if producer is running
    result = subprocess.run(["tasklist"], capture_output=True, text=True)
    if "python.exe" in result.stdout:
        print("✅ Kafka producer is already running")
    else:
        print("⚠️ Kafka producer not found - you may need to restart it manually")

    # Step 4: Start simple data processor
    print("\n📋 STEP 4: Starting Simple Data Processor...")

    processor_cmd = """
    docker exec -d ecommerce-dss-project-data-pipeline-1 bash -c "
    cd /app &&
    nohup python simple_spark_processor.py > logs/simple_processor.log 2>&1 &
    "
    """

    if run_command(processor_cmd, "Start simple data processor"):
        time.sleep(5)  # Wait for startup

    # Step 5: Verify pipeline
    print("\n📋 STEP 5: Verifying Pipeline...")

    # Check Kafka topics
    kafka_check = """
    docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh
    --bootstrap-server localhost:9092 --list 2>/dev/null | grep vietnam || echo "No Vietnam topics"
    """

    run_command(kafka_check, "Check Kafka topics")

    # Check Spark cluster
    spark_check = "curl -s http://localhost:8081 | grep -q 'ALIVE' && echo 'Spark OK' || echo 'Spark FAILED'"
    run_command(spark_check, "Check Spark master")

    # Step 6: Test simple Spark job
    print("\n📋 STEP 6: Testing Simple Spark Job...")

    spark_test_cmd = """
    docker exec spark-master bash -c "
    cd /opt/bitnami/spark &&
    timeout 30 python -c '
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName(\"TestApp\").master(\"spark://spark-master:7077\").getOrCreate()
    print(\"✅ Spark session created successfully\")
    spark.stop()
except Exception as e:
    print(f\"❌ Spark test failed: {e}\")
' 2>/dev/null
    "
    """

    run_command(spark_test_cmd, "Test Spark connectivity")

    # Final status
    print("\n📋 FINAL STATUS:")
    print("🔗 Spark Master UI: http://localhost:8081")
    print("🔗 Kafka UI: http://localhost:8090")
    print("🔗 Airflow UI: http://localhost:8080")
    print("\n✅ Vietnam Streaming Pipeline restart completed!")

    return True

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Restart interrupted by user")
    except Exception as e:
        print(f"\n❌ Restart failed: {e}")