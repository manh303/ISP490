#!/bin/bash
set -e

# Function to check if port is open
check_port() {
    local port=$1
    local timeout=5
    nc -z localhost $port 2>/dev/null || return 1
}

# Function to check HTTP endpoint
check_http() {
    local url=$1
    local timeout=5
    curl -f --max-time $timeout "$url" >/dev/null 2>&1 || return 1
}

# Check based on Spark mode
case "$SPARK_MODE" in
    "master")
        echo "Checking Spark Master health..."
        if check_port 7077 && check_http "http://localhost:8080"; then
            echo "Spark Master is healthy"
            exit 0
        else
            echo "Spark Master health check failed"
            exit 1
        fi
        ;;
    "worker")
        echo "Checking Spark Worker health..."
        if pgrep -f "org.apache.spark.deploy.worker.Worker" > /dev/null; then
            echo "Spark Worker is healthy"
            exit 0
        else
            echo "Spark Worker health check failed"
            exit 1
        fi
        ;;
    "history-server")
        echo "Checking Spark History Server health..."
        if check_http "http://localhost:18080"; then
            echo "Spark History Server is healthy"
            exit 0
        else
            echo "Spark History Server health check failed"
            exit 1
        fi
        ;;
    *)
        echo "Unknown Spark mode: $SPARK_MODE"
        exit 1
        ;;
esac