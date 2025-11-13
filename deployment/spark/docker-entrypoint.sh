#!/bin/bash
set -e

echo "Starting Spark in ${SPARK_MODE} mode..."

# Create necessary directories
mkdir -p /app/data /app/logs /app/checkpoints

case "${SPARK_MODE}" in
    "master")
        echo "Starting Spark Master..."
        exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
            --host ${SPARK_MASTER_HOST:-spark-master} \
            --port ${SPARK_MASTER_PORT:-7077} \
            --webui-port ${SPARK_MASTER_WEBUI_PORT:-8080}
        ;;
    "worker")
        echo "Starting Spark Worker..."
        echo "Connecting to master: ${SPARK_MASTER_URL}"
        
        # Wait for master to be ready
        until nc -z spark-master 7077; do
            echo "Waiting for Spark Master..."
            sleep 2
        done
        
        echo "Master is ready, starting worker..."
        exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
            ${SPARK_MASTER_URL} \
            --cores ${SPARK_WORKER_CORES:-2} \
            --memory ${SPARK_WORKER_MEMORY:-2g} \
            --webui-port ${SPARK_WORKER_WEBUI_PORT:-8081}
        ;;
    "history-server")
        echo "Starting Spark History Server..."
        exec /opt/spark/sbin/start-history-server.sh
        ;;
    *)
        echo "Unknown SPARK_MODE: ${SPARK_MODE}"
        exit 1
        ;;
esac
