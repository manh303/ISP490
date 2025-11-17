# -*- coding: utf-8 -*-
"""
Complete ML Training Pipeline
Orchestrates all model training processes
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import os
import subprocess
from pathlib import Path
from datetime import datetime
import yaml
from utils.logger import get_logger

logger = get_logger("ml_pipeline")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


def run_step(step_name, script_name):
    """
    Run a training step
    """
    logger.info("\n" + "="*70)
    logger.info(f"[STEP] {step_name}")
    logger.info("="*70)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=Path(__file__).parent,
            capture_output=False
        )
        
        if result.returncode == 0:
            logger.info(f"[✓] {step_name} completed successfully")
            return True
        else:
            logger.error(f"[✗] {step_name} failed with return code {result.returncode}")
            return False
    
    except Exception as e:
        logger.error(f"[✗] Error running {step_name}: {e}")
        return False


def run_pipeline():
    """
    Run complete ML pipeline
    """
    logger.info("\n")
    logger.info("╔" + "="*68 + "╗")
    logger.info("║" + " "*15 + "ML TRAINING PIPELINE STARTED" + " "*25 + "║")
    logger.info("║" + f" Pipeline Start Time: {datetime.now()}" + " "*30 + "║")
    logger.info("╚" + "="*68 + "╝")
    
    steps = [
        ("Step 1: Data Extraction", "1_data_extraction.py"),
        ("Step 2: Train Sentiment Classifier", "train_sentiment_classifier.py"),
        ("Step 3: Train Product Clustering", "train_product_clustering.py"),
        # ("Step 4: Train Demand Prediction", "train_demand_prediction.py"),  # Optional
    ]
    
    completed = 0
    failed = 0
    
    for step_name, script_name in steps:
        if run_step(step_name, script_name):
            completed += 1
        else:
            failed += 1
    
    # Summary
    logger.info("\n")
    logger.info("╔" + "="*68 + "╗")
    logger.info("║" + " "*15 + "ML TRAINING PIPELINE COMPLETED" + " "*22 + "║")
    logger.info("║" + f" Pipeline End Time: {datetime.now()}" + " "*33 + "║")
    logger.info("║" + f" Steps Completed: {completed}/{len(steps)}" + " "*36 + "║")
    logger.info("║" + f" Steps Failed: {failed}/{len(steps)}" + " "*39 + "║")
    logger.info("╚" + "="*68 + "╝")
    
    if failed == 0:
        logger.info("\n[✓✓✓] ALL PIPELINE STEPS COMPLETED SUCCESSFULLY ✓✓✓")
        return 0
    else:
        logger.error(f"\n[✗✗✗] PIPELINE FAILED: {failed} steps failed ✗✗✗")
        return 1


def validate_models():
    """
    Validate trained models exist
    """
    logger.info("\n" + "="*60)
    logger.info("VALIDATING TRAINED MODELS")
    logger.info("="*60)
    
    models_dir = Path(config['models']['output_dir'])
    
    required_models = [
        "sentiment_classifier.pkl",
        "sentiment_tfidf_vectorizer.pkl",
        "sentiment_label_encoder.pkl",
        "recommendation_kmeans.pkl",
        "clustering_scaler.pkl",
    ]
    
    all_exist = True
    for model_file in required_models:
        model_path = models_dir / model_file
        if model_path.exists():
            logger.info(f"[✓] {model_file} - EXISTS")
        else:
            logger.warning(f"[✗] {model_file} - MISSING")
            all_exist = False
    
    if all_exist:
        logger.info("\n[✓] All required models are present!")
        return True
    else:
        logger.warning("\n[✗] Some models are missing!")
        return False


if __name__ == "__main__":
    try:
        exit_code = run_pipeline()
        
        # Validate
        if exit_code == 0:
            validate_models()
        
        sys.exit(exit_code)
    
    except Exception as e:
        logger.error(f"[FATAL] Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
