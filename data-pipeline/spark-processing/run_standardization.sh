#!/bin/bash

# E-commerce DSS Data Standardization Pipeline Runner
# Runs the standardization pipeline in Spark Docker cluster

set -e

echo "🚀 Starting E-commerce DSS Data Standardization Pipeline..."

# Configuration
SPARK_MASTER_URL="spark://spark-master:7077"
SCRIPT_PATH="/opt/spark/apps/standardization_pipeline.py"
APP_NAME="EcommerceDSS-DataStandardization"

# Check if Spark cluster is running
echo "🔍 Checking Spark cluster availability..."
docker exec spark-master curl -s http://spark-master:8080 > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ Spark cluster is running"
else
    echo "❌ Spark cluster is not accessible"
    exit 1
fi

# Check required data files
echo "📂 Checking data availability..."
if docker exec spark-master ls /opt/spark/data/raw/fixed_lazada_products.json > /dev/null 2>&1; then
    echo "✅ Lazada data found"
else
    echo "⚠️  Lazada data not found"
fi

# Create output directory if not exists
echo "📁 Preparing output directory..."
docker exec spark-master mkdir -p /opt/spark/data/processed

# Install Python dependencies in Spark master
echo "📦 Installing Python dependencies..."
docker exec spark-master pip install --quiet --no-cache-dir pyspark

# Copy the standardization script to Spark master
echo "📋 Copying standardization script..."
docker cp "$(dirname "$0")/standardization_pipeline.py" spark-master:/opt/spark/apps/

# Run the standardization pipeline
echo "🔥 Executing standardization pipeline..."
docker exec spark-master python $SCRIPT_PATH

# Check execution status
if [ $? -eq 0 ]; then
    echo "✅ Data standardization pipeline completed successfully!"

    # Show output files
    echo "📊 Generated output files:"
    docker exec spark-master find /opt/spark/data/processed -name "*.json" -o -name "*.parquet" | head -10

    # Show latest report
    echo "📋 Latest processing report:"
    LATEST_REPORT=$(docker exec spark-master find /opt/spark/data/processed -name "processing_report_*.json" | sort | tail -1)
    if [ ! -z "$LATEST_REPORT" ]; then
        docker exec spark-master cat "$LATEST_REPORT" | head -20
    fi

else
    echo "❌ Data standardization pipeline failed!"
    exit 1
fi

echo "🎉 Pipeline execution completed!"