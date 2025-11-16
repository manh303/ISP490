#!/bin/bash

# ML Pipeline Executor
# ====================
# Usage: ./run_pipeline.sh [step]
# Steps: all, extract, prepare, train, evaluate, serve

set -e

LOG_DIR="logs"
mkdir -p $LOG_DIR

echo "=================================================="
echo "ML PIPELINE EXECUTOR"
echo "=================================================="

STEP=${1:-all}

case $STEP in
  extract)
    echo "Running Step 1: Data Extraction..."
    python 1_data_extraction.py | tee -a $LOG_DIR/pipeline.log
    ;;
  
  prepare)
    echo "Running Step 2: Data Preparation..."
    python 2_data_preparation.py | tee -a $LOG_DIR/pipeline.log
    ;;
  
  train)
    echo "Running Step 3: Model Training..."
    python 3_model_training.py | tee -a $LOG_DIR/pipeline.log
    ;;
  
  evaluate)
    echo "Running Step 4: Model Evaluation..."
    python 4_model_evaluation.py | tee -a $LOG_DIR/pipeline.log
    ;;
  
  serve)
    echo "Running Step 5: Model Serving..."
    python 5_model_serving.py | tee -a $LOG_DIR/pipeline.log
    ;;
  
  all)
    echo "Running Full Pipeline..."
    python 1_data_extraction.py | tee -a $LOG_DIR/pipeline.log
    python 2_data_preparation.py | tee -a $LOG_DIR/pipeline.log
    python 3_model_training.py | tee -a $LOG_DIR/pipeline.log
    python 4_model_evaluation.py | tee -a $LOG_DIR/pipeline.log
    echo "Pipeline completed!"
    echo "To start serving API, run: python 5_model_serving.py"
    ;;
  
  *)
    echo "Usage: $0 [extract|prepare|train|evaluate|serve|all]"
    exit 1
    ;;
esac

echo "✓ Step completed"
