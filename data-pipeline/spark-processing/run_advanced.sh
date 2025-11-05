#!/bin/bash

# Advanced E-commerce Data Processing Runner
# Usage: ./run_advanced.sh [config_file]

set -e

echo "🚀 Starting Advanced E-commerce Data Processing..."

# Default config
CONFIG_FILE="configs/advanced_config.json"

# Use provided config if given
if [ ! -z "$1" ]; then
    CONFIG_FILE="$1"
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Config file not found: $CONFIG_FILE"
    exit 1
fi

echo "📄 Using config: $CONFIG_FILE"

# Create output directories
mkdir -p outputs/advanced_processing
mkdir -p outputs/logs
mkdir -p outputs/ml_models

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"

# Check if Spark is available
if ! command -v spark-submit &> /dev/null; then
    echo "❌ Spark not found. Please install Apache Spark and set SPARK_HOME"
    exit 1
fi

# Check available memory
TOTAL_MEM=$(free -m | awk 'NR==2{printf "%.0f", $2/1024}')
echo "🖥️  Available memory: ${TOTAL_MEM}GB"

if [ "$TOTAL_MEM" -lt 8 ]; then
    echo "⚠️  Warning: Less than 8GB RAM available. Consider adjusting Spark memory settings."
fi

# Log file
LOG_FILE="outputs/logs/advanced_processing_$(date +%Y%m%d_%H%M%S).log"

echo "📝 Logging to: $LOG_FILE"

# Run the processing with spark-submit for better resource management
echo "⚡ Running Advanced Spark processing..."

cd advanced/

# Production spark-submit command
spark-submit \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.sql.adaptive.skewJoin.enabled=true" \
    --conf "spark.sql.adaptive.localShuffleReader.enabled=true" \
    --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
    --conf "spark.sql.execution.arrow.maxRecordsPerBatch=10000" \
    --packages org.postgresql:postgresql:42.7.1 \
    advanced_ecommerce_processor.py \
    --config "../$CONFIG_FILE" 2>&1 | tee "../$LOG_FILE"

# Alternative: Simple python execution for development
# python advanced_ecommerce_processor.py --config "../$CONFIG_FILE" 2>&1 | tee "../$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Advanced processing completed successfully!"
    echo ""
    echo "📊 Results:"
    echo "  - Processed data: outputs/advanced_processing/processed_data/"
    echo "  - ML models: outputs/advanced_processing/ml_models/"
    echo "  - Analytics: outputs/advanced_processing/analytics_results.json"
    echo "  - Customer segments: outputs/advanced_processing/customer_segments/"
    echo ""
    echo "📝 Log file: $LOG_FILE"

    # Show summary statistics
    if [ -f "outputs/advanced_processing/analytics_results.json" ]; then
        echo ""
        echo "📈 Quick Summary:"
        python3 -c "
import json
try:
    with open('outputs/advanced_processing/analytics_results.json', 'r') as f:
        data = json.load(f)
    print(f'  - Market insights available: {len(data.get(\"market_insights\", {}))} categories')
    if 'trends' in data:
        print(f'  - Trend analysis: Available')
    if 'correlations' in data:
        print(f'  - Correlation analysis: Available')
except:
    print('  - Summary data parsing failed')
"
    fi
else
    echo "❌ Processing failed. Check log file: $LOG_FILE"
    echo ""
    echo "🔍 Troubleshooting tips:"
    echo "  1. Check available memory: free -h"
    echo "  2. Verify Spark installation: spark-submit --version"
    echo "  3. Check input data paths in config file"
    echo "  4. Review error messages in log file"
    exit 1
fi