#!/bin/bash

# Simple E-commerce Data Processing Runner
# Usage: ./run_simple.sh [config_file]

set -e

echo "🚀 Starting Simple E-commerce Data Processing..."

# Default config
CONFIG_FILE="configs/simple_config.yaml"

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
mkdir -p outputs/simple_processing
mkdir -p outputs/logs

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"

# Check if Spark is available
if ! command -v spark-submit &> /dev/null; then
    echo "❌ Spark not found. Please install Apache Spark and set SPARK_HOME"
    exit 1
fi

# Log file
LOG_FILE="outputs/logs/simple_processing_$(date +%Y%m%d_%H%M%S).log"

echo "📝 Logging to: $LOG_FILE"

# Run the processing
echo "⚡ Running Spark processing..."

# Simple python execution
cd simple/
python simple_ecommerce_processor.py --config "../$CONFIG_FILE" 2>&1 | tee "../$LOG_FILE"

# Alternative: Using spark-submit for production
# spark-submit \
#     --driver-memory 2g \
#     --executor-memory 4g \
#     --conf "spark.sql.adaptive.enabled=true" \
#     simple_ecommerce_processor.py \
#     --config "$CONFIG_FILE" 2>&1 | tee "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Simple processing completed successfully!"
    echo "📊 Results saved to: outputs/simple_processing/"
    echo "📝 Log file: $LOG_FILE"
else
    echo "❌ Processing failed. Check log file: $LOG_FILE"
    exit 1
fi