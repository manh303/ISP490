# -*- coding: utf-8 -*-
"""
Step 3: Model Training
Train multiple models for Demand Prediction & Product Recommendation
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import cross_val_score
import xgboost as xgb
import lightgbm as lgb
from utils.logger import get_logger
from utils.metrics import RegressionMetrics, ClusteringMetrics, log_metrics
import yaml
import warnings

warnings.filterwarnings('ignore')

logger = get_logger("model_training")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


class DemandPredictionTrainer:
    """Train demand prediction models"""
    
    def __init__(self, config: dict):
        self.config = config
        self.models = {}
        self.feature_cols = None
    
    def train_xgboost(self, X_train, y_train):
        """Train XGBoost model"""
        logger.info("  Training XGBRegressor...")
        
        params = self.config['model_training']['demand_models'][0]['XGBRegressor']
        
        model = xgb.XGBRegressor(**params)
        model.fit(X_train, y_train)
        
        return model
    
    def train_random_forest(self, X_train, y_train):
        """Train Random Forest model"""
        logger.info("  Training RandomForestRegressor...")
        
        params = self.config['model_training']['demand_models'][1]['RandomForestRegressor']
        
        model = RandomForestRegressor(**params, n_jobs=-1)
        model.fit(X_train, y_train)
        
        return model
    
    def train_lightgbm(self, X_train, y_train):
        """Train LightGBM model"""
        logger.info("  Training LGBMRegressor...")
        
        params = self.config['model_training']['demand_models'][2]['LGBMRegressor']
        
        model = lgb.LGBMRegressor(**params, n_jobs=-1)
        model.fit(X_train, y_train)
        
        return model
    
    def train_linear(self, X_train, y_train):
        """Train Linear Regression model"""
        logger.info("  Training LinearRegression...")
        
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        return model
    
    def train_ridge(self, X_train, y_train):
        """Train Ridge Regression model"""
        logger.info("  Training Ridge...")
        
        params = self.config['model_training']['demand_models'][4]['Ridge']
        
        model = Ridge(**params)
        model.fit(X_train, y_train)
        
        return model
    
    def train_all(self, X_train, y_train) -> dict:
        """Train all demand models"""
        logger.info("\n" + "="*60)
        logger.info("TRAINING DEMAND PREDICTION MODELS")
        logger.info("="*60)
        
        self.models = {
            'xgboost': self.train_xgboost(X_train, y_train),
            'random_forest': self.train_random_forest(X_train, y_train),
            'lightgbm': self.train_lightgbm(X_train, y_train),
            'linear': self.train_linear(X_train, y_train),
            'ridge': self.train_ridge(X_train, y_train)
        }
        
        logger.info(f"\n[OK] Trained {len(self.models)} models")
        
        return self.models
    
    def evaluate(self, models: dict, X_test, y_test) -> dict:
        """Evaluate models"""
        logger.info("\n" + "="*60)
        logger.info("EVALUATING DEMAND PREDICTION MODELS")
        logger.info("="*60)
        
        results = {}
        
        for model_name, model in models.items():
            y_pred = model.predict(X_test)
            metrics = RegressionMetrics.evaluate(y_test, y_pred)
            
            results[model_name] = metrics
            
            logger.info(f"\n{model_name}:")
            for metric, value in metrics.items():
                logger.info(f"  {metric}: {value:.4f}")
        
        return results
    
    def save_models(self, models: dict, output_dir: Path):
        """Save trained models"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for model_name, model in models.items():
            path = output_dir / f'demand_{model_name}.pkl'
            joblib.dump(model, path)
            logger.info(f"[OK] Saved {model_name} to {path}")


class RecommendationTrainer:
    """Train product recommendation models"""
    
    def __init__(self, config: dict):
        self.config = config
        self.models = {}
    
    def train_kmeans(self, X):
        """Train KMeans clustering model"""
        logger.info("  Training KMeans...")
        
        params = self.config['model_training']['recommendation_models'][0]['KMeans']
        
        model = KMeans(**params)
        model.fit(X)
        
        return model
    
    def train_nearest_neighbors(self, X):
        """Train NearestNeighbors model"""
        logger.info("  Training NearestNeighbors...")
        
        params = self.config['model_training']['recommendation_models'][1]['NearestNeighbors']
        
        model = NearestNeighbors(**params)
        model.fit(X)
        
        return model
    
    def train_all(self, X) -> dict:
        """Train all recommendation models"""
        logger.info("\n" + "="*60)
        logger.info("TRAINING PRODUCT RECOMMENDATION MODELS")
        logger.info("="*60)
        
        self.models = {
            'kmeans': self.train_kmeans(X),
            'nearest_neighbors': self.train_nearest_neighbors(X)
        }
        
        logger.info(f"\n[OK] Trained {len(self.models)} models")
        
        return self.models
    
    def evaluate_kmeans(self, X, model) -> dict:
        """Evaluate KMeans"""
        labels = model.labels_
        metrics = ClusteringMetrics.evaluate(X, labels)
        
        logger.info(f"\nKMeans:")
        for metric, value in metrics.items():
            logger.info(f"  {metric}: {value:.4f}")
        
        return metrics
    
    def evaluate_nearest_neighbors(self, X, model) -> dict:
        """Evaluate NearestNeighbors"""
        # Calculate average distances to neighbors
        distances, indices = model.kneighbors(X)
        avg_distance = distances.mean()
        
        metrics = {
            'avg_neighbor_distance': avg_distance,
            'max_neighbor_distance': distances.max(),
            'min_neighbor_distance': distances.min()
        }
        
        logger.info(f"\nNearestNeighbors:")
        for metric, value in metrics.items():
            logger.info(f"  {metric}: {value:.4f}")
        
        return metrics
    
    def evaluate(self, models: dict, X):
        """Evaluate all recommendation models"""
        logger.info("\n" + "="*60)
        logger.info("EVALUATING PRODUCT RECOMMENDATION MODELS")
        logger.info("="*60)
        
        results = {}
        
        for model_name, model in models.items():
            if model_name == 'kmeans':
                results[model_name] = self.evaluate_kmeans(X, model)
            elif model_name == 'nearest_neighbors':
                results[model_name] = self.evaluate_nearest_neighbors(X, model)
        
        return results
    
    def save_models(self, models: dict, output_dir: Path):
        """Save trained models"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for model_name, model in models.items():
            path = output_dir / f'recommendation_{model_name}.pkl'
            joblib.dump(model, path)
            logger.info(f"[OK] Saved {model_name} to {path}")


def main():
    """Main pipeline"""
    try:
        logger.info("[ML PIPELINE] Step 3: Model Training")
        
        # Load prepared data
        demand_dir = Path(config['data_extraction']['demand']['output_dir'])
        recommendation_dir = Path(config['data_extraction']['recommendation']['output_dir'])
        
        # Load demand data
        train_df = pd.read_csv(demand_dir / 'train_demand_data.csv')
        test_df = pd.read_csv(demand_dir / 'test_demand_data.csv')
        
        logger.info(f"Loaded {len(train_df)} training samples")
        logger.info(f"Loaded {len(test_df)} test samples")
        
        # Identify feature columns
        feature_cols = [col for col in train_df.columns 
                       if col not in ['total_review_count', 'source_platform_std', 'agg_date']]
        
        X_train = train_df[feature_cols].values
        y_train = train_df['total_review_count'].values
        X_test = test_df[feature_cols].values
        y_test = test_df['total_review_count'].values
        
        # Train demand models
        demand_trainer = DemandPredictionTrainer(config)
        demand_models = demand_trainer.train_all(X_train, y_train)
        
        # Evaluate demand models
        demand_results = demand_trainer.evaluate(demand_models, X_test, y_test)
        
        # Save demand models
        model_dir = Path(config['output']['models_dir'])
        demand_trainer.save_models(demand_models, model_dir)
        
        # Load recommendation data
        recommendation_df = pd.read_csv(recommendation_dir / 'prepared_recommendation_data.csv')
        logger.info(f"Loaded {len(recommendation_df)} products for recommendation")
        
        # Identify feature columns (numeric only for clustering)
        rec_feature_cols = [col for col in recommendation_df.columns 
                           if col not in ['global_product_id', 'source_platform_std'] 
                           and recommendation_df[col].dtype in ['float64', 'int64']]
        
        X_rec = recommendation_df[rec_feature_cols].values
        
        # Train recommendation models
        rec_trainer = RecommendationTrainer(config)
        rec_models = rec_trainer.train_all(X_rec)
        
        # Evaluate recommendation models
        rec_results = rec_trainer.evaluate(rec_models, X_rec)
        
        # Save recommendation models
        rec_trainer.save_models(rec_models, model_dir)
        
        # Save results
        results_dir = Path(config['output']['metrics_dir'])
        results_dir.mkdir(parents=True, exist_ok=True)
        
        import json
        with open(results_dir / 'demand_results.json', 'w') as f:
            json.dump(demand_results, f, indent=2)
        
        with open(results_dir / 'recommendation_results.json', 'w') as f:
            json.dump(rec_results, f, indent=2)
        
        logger.info("\n" + "="*60)
        logger.info("[OK] MODEL TRAINING COMPLETED")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n[ERROR] FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
