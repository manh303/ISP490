# app/ml_runtime.py
"""
ML Model Runtime Module

Provides functions to load and cache ML models for online inference.
Models are loaded from pickle files and cached using LRU cache to avoid
repeated disk I/O.

Directory structure:
    models/
        price_forecast_rf_v1.0.pkl
        sentiment_tfidf_logreg_v1.0.pkl
        ...
"""
import os
import pickle
from functools import lru_cache
from pathlib import Path
from typing import Tuple, Any

# Read from env variable, default to "ml/models" directory
MODEL_DIR = Path(os.getenv("ML_MODEL_DIR", "ml/models"))


@lru_cache(maxsize=10)
def load_price_model(model_name: str, model_version: str) -> Any:
    """
    Load price prediction model from pickle file.
    
    File naming convention: {model_name}_{model_version}.pkl
    Example: price_forecast_rf_v1.0.pkl
    
    The model is cached to avoid repeated loading. Cache holds up to 10 models.
    
    Args:
        model_name: Name of the model (e.g., "price_forecast_rf")
        model_version: Version string (e.g., "v1.0")
        
    Returns:
        Loaded sklearn model object with predict() method
        
    Raises:
        FileNotFoundError: If model file does not exist
        Exception: If pickle loading fails
    """
    filename = f"{model_name}_{model_version}.pkl"
    path = MODEL_DIR / filename
    
    if not path.exists():
        raise FileNotFoundError(
            f"Model file not found: {path}\n"
            f"Expected location: {MODEL_DIR}\n"
            f"Available files: {list(MODEL_DIR.glob('*.pkl')) if MODEL_DIR.exists() else 'Directory does not exist'}"
        )
    
    try:
        with open(path, "rb") as f:
            model = pickle.load(f)
        return model
    except Exception as e:
        raise Exception(f"Failed to load model from {path}: {str(e)}")


@lru_cache(maxsize=10)
def load_sentiment_pipeline(model_name: str, model_version: str) -> Tuple[Any, Any]:
    """
    Load sentiment analysis pipeline from pickle file.
    
    File naming convention: {model_name}_{model_version}.pkl
    Example: sentiment_tfidf_logreg_v1.0.pkl
    
    The pipeline bundle must be a dict with keys:
        - "vectorizer": TfidfVectorizer or similar
        - "model": Classifier with predict_proba() method
    
    Args:
        model_name: Name of the model (e.g., "sentiment_tfidf_logreg")
        model_version: Version string (e.g., "v1.0")
        
    Returns:
        Tuple of (vectorizer, classifier)
        
    Raises:
        FileNotFoundError: If model file does not exist
        ValueError: If bundle format is invalid
        Exception: If pickle loading fails
    """
    filename = f"{model_name}_{model_version}.pkl"
    path = MODEL_DIR / filename
    
    if not path.exists():
        raise FileNotFoundError(
            f"Model file not found: {path}\n"
            f"Expected location: {MODEL_DIR}\n"
            f"Available files: {list(MODEL_DIR.glob('*.pkl')) if MODEL_DIR.exists() else 'Directory does not exist'}"
        )
    
    try:
        with open(path, "rb") as f:
            bundle = pickle.load(f)
    except Exception as e:
        raise Exception(f"Failed to load model from {path}: {str(e)}")
    
    # Validate bundle structure
    if not isinstance(bundle, dict):
        raise ValueError(
            f"Invalid model bundle format in {path}: expected dict, got {type(bundle)}"
        )
    
    if "vectorizer" not in bundle or "model" not in bundle:
        raise ValueError(
            f"Invalid model bundle format in {path}: missing 'vectorizer' or 'model' keys. "
            f"Found keys: {list(bundle.keys())}"
        )
    
    return bundle["vectorizer"], bundle["model"]


def get_available_models() -> list:
    """
    List all available model files in the MODEL_DIR.
    
    Returns:
        List of tuples (filename, size_in_bytes)
    """
    if not MODEL_DIR.exists():
        return []
    
    models = []
    for pkl_file in MODEL_DIR.glob("*.pkl"):
        size = pkl_file.stat().st_size
        models.append((pkl_file.name, size))
    
    return sorted(models)


def clear_model_cache():
    """
    Clear the LRU cache for all loaded models.
    
    Useful for:
    - Forcing reload of updated model files
    - Freeing memory in low-memory situations
    """
    load_price_model.cache_clear()
    load_sentiment_pipeline.cache_clear()
