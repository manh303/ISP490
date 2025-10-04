#!/usr/bin/env python3
"""
Automated Machine Learning Pipeline for E-commerce DSS
Handles model training, validation, and deployment with performance optimization
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import joblib
import json
from typing import Dict, List, Any, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# ML Libraries
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, accuracy_score, classification_report
from sklearn.cluster import KMeans
import xgboost as xgb
import lightgbm as lgb

# Database connections
from sqlalchemy import create_engine, text
import pymongo
import redis

# Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AutoMLPipeline:
    """
    Automated Machine Learning Pipeline with model selection and optimization
    """

    def __init__(self, model_storage_path: str = "/app/models"):
        self.model_storage_path = Path(model_storage_path)
        self.model_storage_path.mkdir(parents=True, exist_ok=True)

        # Database connections
        self.postgres_engine = None
        self.mongo_client = None
        self.redis_client = None

        # Model registry
        self.model_registry = {}
        self.performance_metrics = {}

        self._setup_connections()
        self._initialize_models()

    def _setup_connections(self):
        """Setup database connections"""
        try:
            # PostgreSQL
            db_url = os.getenv('DATABASE_URL', 'postgresql://admin:admin_password@postgres:5432/ecommerce_dss')
            self.postgres_engine = create_engine(db_url)

            # MongoDB
            mongo_url = os.getenv('MONGODB_URI', 'mongodb://admin:admin_password@mongodb:27017/')
            self.mongo_client = pymongo.MongoClient(mongo_url)

            # Redis
            redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
            self.redis_client = redis.from_url(redis_url)

            logger.info("✅ Database connections established")
        except Exception as e:
            logger.error(f"❌ Failed to setup database connections: {e}")
            raise

    def _initialize_models(self):
        """Initialize model configurations"""
        self.model_configs = {
            'customer_lifetime_value': {
                'type': 'regression',
                'models': {
                    'random_forest': RandomForestRegressor(n_estimators=100, random_state=42),
                    'gradient_boosting': GradientBoostingRegressor(random_state=42),
                    'xgboost': xgb.XGBRegressor(random_state=42),
                    'lightgbm': lgb.LGBMRegressor(random_state=42)
                },
                'target_column': 'customer_lifetime_value',
                'feature_columns': [
                    'total_orders', 'total_spent', 'avg_order_value', 'days_since_last_order',
                    'customer_age_days', 'preferred_category_encoded', 'customer_segment_encoded'
                ]
            },
            'customer_segmentation': {
                'type': 'clustering',
                'models': {
                    'kmeans': KMeans(n_clusters=5, random_state=42)
                },
                'feature_columns': [
                    'total_orders', 'total_spent', 'avg_order_value', 'days_since_last_order'
                ]
            },
            'product_recommendation': {
                'type': 'classification',
                'models': {
                    'random_forest': RandomForestClassifier(n_estimators=100, random_state=42),
                    'xgboost': xgb.XGBClassifier(random_state=42)
                },
                'target_column': 'product_category',
                'feature_columns': [
                    'price', 'weight_g', 'volume_cm3', 'avg_rating', 'review_count'
                ]
            },
            'demand_forecasting': {
                'type': 'regression',
                'models': {
                    'random_forest': RandomForestRegressor(n_estimators=100, random_state=42),
                    'gradient_boosting': GradientBoostingRegressor(random_state=42)
                },
                'target_column': 'monthly_sales',
                'feature_columns': [
                    'price', 'seasonal_factor', 'promotion_active', 'stock_level', 'competitor_price'
                ]
            }
        }

    def load_training_data(self, model_name: str) -> pd.DataFrame:
        """Load training data based on model type"""
        logger.info(f"📊 Loading training data for {model_name}")

        try:
            if model_name == 'customer_lifetime_value':
                query = """
                SELECT
                    customer_id,
                    total_orders,
                    total_spent,
                    avg_order_value,
                    EXTRACT(days FROM (NOW() - last_order_date)) as days_since_last_order,
                    EXTRACT(days FROM (NOW() - created_at)) as customer_age_days,
                    preferred_category,
                    customer_segment,
                    total_spent as customer_lifetime_value
                FROM customers
                WHERE total_orders > 0 AND total_spent > 0
                LIMIT 10000
                """

            elif model_name == 'customer_segmentation':
                query = """
                SELECT
                    customer_id,
                    total_orders,
                    total_spent,
                    avg_order_value,
                    EXTRACT(days FROM (NOW() - last_order_date)) as days_since_last_order
                FROM customers
                WHERE total_orders > 0
                LIMIT 10000
                """

            elif model_name == 'product_recommendation':
                query = """
                SELECT
                    product_id,
                    price,
                    weight_g,
                    volume_cm3,
                    avg_rating,
                    review_count,
                    category as product_category
                FROM products
                WHERE price > 0
                LIMIT 10000
                """

            else:
                # Generate synthetic data for demand forecasting
                return self._generate_synthetic_demand_data()

            df = pd.read_sql(query, self.postgres_engine)

            if df.empty:
                logger.warning(f"⚠️ No data found for {model_name}, generating synthetic data")
                return self._generate_synthetic_data(model_name)

            logger.info(f"✅ Loaded {len(df)} records for {model_name}")
            return df

        except Exception as e:
            logger.warning(f"⚠️ Failed to load real data for {model_name}: {e}")
            logger.info(f"🔄 Generating synthetic data for {model_name}")
            return self._generate_synthetic_data(model_name)

    def _generate_synthetic_data(self, model_name: str) -> pd.DataFrame:
        """Generate synthetic data for training"""
        np.random.seed(42)

        if model_name == 'customer_lifetime_value':
            n_samples = 5000
            data = {
                'customer_id': [f'CUST_{i:06d}' for i in range(n_samples)],
                'total_orders': np.random.randint(1, 50, n_samples),
                'total_spent': np.random.exponential(500, n_samples),
                'avg_order_value': np.random.uniform(20, 200, n_samples),
                'days_since_last_order': np.random.randint(1, 365, n_samples),
                'customer_age_days': np.random.randint(30, 1095, n_samples),
                'preferred_category': np.random.choice(['electronics', 'clothing', 'books', 'home'], n_samples),
                'customer_segment': np.random.choice(['bronze', 'silver', 'gold', 'platinum'], n_samples)
            }
            df = pd.DataFrame(data)
            # Calculate synthetic CLV
            df['customer_lifetime_value'] = (
                df['total_spent'] +
                df['total_orders'] * df['avg_order_value'] * 0.1 +
                np.random.normal(0, 50, n_samples)
            )

        elif model_name == 'customer_segmentation':
            n_samples = 3000
            data = {
                'customer_id': [f'CUST_{i:06d}' for i in range(n_samples)],
                'total_orders': np.random.randint(1, 30, n_samples),
                'total_spent': np.random.exponential(300, n_samples),
                'avg_order_value': np.random.uniform(20, 150, n_samples),
                'days_since_last_order': np.random.randint(1, 180, n_samples)
            }
            df = pd.DataFrame(data)

        elif model_name == 'product_recommendation':
            n_samples = 4000
            categories = ['electronics', 'clothing', 'books', 'home', 'sports']
            data = {
                'product_id': [f'PROD_{i:06d}' for i in range(n_samples)],
                'price': np.random.exponential(100, n_samples),
                'weight_g': np.random.exponential(500, n_samples),
                'volume_cm3': np.random.exponential(1000, n_samples),
                'avg_rating': np.random.uniform(3.0, 5.0, n_samples),
                'review_count': np.random.randint(0, 100, n_samples),
                'product_category': np.random.choice(categories, n_samples)
            }
            df = pd.DataFrame(data)

        else:  # demand_forecasting
            return self._generate_synthetic_demand_data()

        logger.info(f"🔧 Generated {len(df)} synthetic records for {model_name}")
        return df

    def _generate_synthetic_demand_data(self) -> pd.DataFrame:
        """Generate synthetic demand forecasting data"""
        np.random.seed(42)
        n_samples = 2000

        data = {
            'product_id': [f'PROD_{i:06d}' for i in range(n_samples)],
            'price': np.random.uniform(10, 500, n_samples),
            'seasonal_factor': np.random.uniform(0.8, 1.2, n_samples),
            'promotion_active': np.random.choice([0, 1], n_samples),
            'stock_level': np.random.randint(0, 1000, n_samples),
            'competitor_price': np.random.uniform(10, 600, n_samples)
        }

        df = pd.DataFrame(data)

        # Generate synthetic monthly sales
        df['monthly_sales'] = (
            (1000 / df['price']) *
            df['seasonal_factor'] *
            (1 + df['promotion_active'] * 0.3) *
            (df['stock_level'] / 500) +
            np.random.normal(0, 10, n_samples)
        ).clip(lower=0)

        return df

    def prepare_features(self, df: pd.DataFrame, model_name: str) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare features and target for training"""
        config = self.model_configs[model_name]

        # Handle categorical encoding
        df_processed = df.copy()

        # Encode categorical variables
        categorical_columns = df_processed.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            if col in config.get('feature_columns', []):
                le = LabelEncoder()
                df_processed[f'{col}_encoded'] = le.fit_transform(df_processed[col].astype(str))

                # Save encoder for later use
                encoder_path = self.model_storage_path / f"{model_name}_{col}_encoder.pkl"
                joblib.dump(le, encoder_path)

        # Select features
        feature_columns = [
            col for col in config.get('feature_columns', [])
            if col in df_processed.columns or f'{col}_encoded' in df_processed.columns
        ]

        # Update feature column names for encoded columns
        updated_feature_columns = []
        for col in feature_columns:
            if col in df_processed.columns:
                updated_feature_columns.append(col)
            elif f'{col}_encoded' in df_processed.columns:
                updated_feature_columns.append(f'{col}_encoded')

        X = df_processed[updated_feature_columns].fillna(0)

        # Prepare target
        if config['type'] == 'clustering':
            y = None
        else:
            target_col = config['target_column']
            if target_col in df_processed.columns:
                y = df_processed[target_col]
            else:
                logger.warning(f"⚠️ Target column {target_col} not found, using first numeric column")
                numeric_cols = df_processed.select_dtypes(include=[np.number]).columns
                y = df_processed[numeric_cols[0]] if len(numeric_cols) > 0 else pd.Series([0] * len(df_processed))

        return X, y

    def train_model(self, model_name: str) -> Dict[str, Any]:
        """Train a specific model"""
        logger.info(f"🚀 Training {model_name} model")

        try:
            # Load and prepare data
            df = self.load_training_data(model_name)
            X, y = self.prepare_features(df, model_name)

            config = self.model_configs[model_name]
            results = {}

            # Train each model type
            for model_type, model in config['models'].items():
                logger.info(f"🔄 Training {model_type} for {model_name}")

                try:
                    if config['type'] == 'clustering':
                        # Clustering doesn't need train/test split
                        model.fit(X)
                        labels = model.labels_

                        # Calculate silhouette score
                        from sklearn.metrics import silhouette_score
                        score = silhouette_score(X, labels)

                        results[model_type] = {
                            'model': model,
                            'silhouette_score': score,
                            'n_clusters': model.n_clusters,
                            'training_samples': len(X)
                        }

                    else:
                        # Supervised learning
                        X_train, X_test, y_train, y_test = train_test_split(
                            X, y, test_size=0.2, random_state=42
                        )

                        # Scale features if needed
                        if model_type in ['lightgbm', 'xgboost']:
                            # Tree-based models don't need scaling
                            model.fit(X_train, y_train)
                        else:
                            scaler = StandardScaler()
                            X_train_scaled = scaler.fit_transform(X_train)
                            X_test_scaled = scaler.transform(X_test)

                            model.fit(X_train_scaled, y_train)
                            X_test = X_test_scaled

                            # Save scaler
                            scaler_path = self.model_storage_path / f"{model_name}_{model_type}_scaler.pkl"
                            joblib.dump(scaler, scaler_path)

                        # Make predictions
                        y_pred = model.predict(X_test)

                        # Calculate metrics
                        if config['type'] == 'regression':
                            mae = mean_absolute_error(y_test, y_pred)
                            mse = mean_squared_error(y_test, y_pred)
                            r2 = r2_score(y_test, y_pred)

                            results[model_type] = {
                                'model': model,
                                'mae': mae,
                                'mse': mse,
                                'r2_score': r2,
                                'training_samples': len(X_train),
                                'test_samples': len(X_test)
                            }
                        else:  # classification
                            accuracy = accuracy_score(y_test, y_pred)

                            results[model_type] = {
                                'model': model,
                                'accuracy': accuracy,
                                'training_samples': len(X_train),
                                'test_samples': len(X_test)
                            }

                    logger.info(f"✅ {model_type} training completed")

                except Exception as e:
                    logger.error(f"❌ Failed to train {model_type}: {e}")
                    results[model_type] = {'error': str(e)}

            # Select best model
            best_model_info = self._select_best_model(results, config['type'])

            # Save best model
            if best_model_info:
                model_path = self.model_storage_path / f"{model_name}_best_model.pkl"
                joblib.dump(best_model_info['model'], model_path)

                # Store model metadata
                metadata = {
                    'model_name': model_name,
                    'model_type': best_model_info['type'],
                    'trained_at': datetime.now().isoformat(),
                    'training_samples': best_model_info.get('training_samples', 0),
                    'performance_metrics': {k: v for k, v in best_model_info.items() if k not in ['model', 'type']},
                    'model_path': str(model_path),
                    'feature_columns': config.get('feature_columns', [])
                }

                # Save to MongoDB
                db = self.mongo_client['ecommerce_dss']
                db.model_registry.replace_one(
                    {'model_name': model_name},
                    metadata,
                    upsert=True
                )

                # Store performance in Redis
                self.redis_client.setex(
                    f"model_performance_{model_name}",
                    timedelta(days=7),
                    json.dumps(metadata)
                )

            logger.info(f"🎯 {model_name} training completed")
            return results

        except Exception as e:
            logger.error(f"❌ Failed to train {model_name}: {e}")
            raise

    def _select_best_model(self, results: Dict[str, Any], model_type: str) -> Optional[Dict[str, Any]]:
        """Select the best performing model"""
        if not results:
            return None

        valid_results = {k: v for k, v in results.items() if 'error' not in v}
        if not valid_results:
            return None

        if model_type == 'regression':
            # Select model with lowest MAE
            best_model_name = min(valid_results.keys(), key=lambda x: valid_results[x].get('mae', float('inf')))
        elif model_type == 'classification':
            # Select model with highest accuracy
            best_model_name = max(valid_results.keys(), key=lambda x: valid_results[x].get('accuracy', 0))
        else:  # clustering
            # Select model with highest silhouette score
            best_model_name = max(valid_results.keys(), key=lambda x: valid_results[x].get('silhouette_score', 0))

        best_result = valid_results[best_model_name].copy()
        best_result['type'] = best_model_name

        return best_result

    def train_all_models(self) -> Dict[str, Any]:
        """Train all configured models"""
        logger.info("🚀 Starting automated model training pipeline")

        training_results = {}

        for model_name in self.model_configs.keys():
            try:
                logger.info(f"🔄 Training {model_name}")
                results = self.train_model(model_name)
                training_results[model_name] = results

            except Exception as e:
                logger.error(f"❌ Failed to train {model_name}: {e}")
                training_results[model_name] = {'error': str(e)}

        # Generate training summary
        summary = self._generate_training_summary(training_results)

        # Store summary in MongoDB
        db = self.mongo_client['ecommerce_dss']
        db.training_runs.insert_one({
            'run_id': f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'timestamp': datetime.now(),
            'results': training_results,
            'summary': summary
        })

        logger.info(f"🎯 Model training pipeline completed: {summary}")
        return training_results

    def _generate_training_summary(self, training_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate training summary statistics"""
        total_models = len(training_results)
        successful_models = len([r for r in training_results.values() if 'error' not in r])
        failed_models = total_models - successful_models

        return {
            'total_models': total_models,
            'successful_models': successful_models,
            'failed_models': failed_models,
            'success_rate': (successful_models / total_models) * 100 if total_models > 0 else 0,
            'training_timestamp': datetime.now().isoformat()
        }

def main():
    """Main training function"""
    logger.info("🚀 Starting automated ML training pipeline")

    try:
        # Initialize ML pipeline
        ml_pipeline = AutoMLPipeline()

        # Train all models
        results = ml_pipeline.train_all_models()

        logger.info(f"✅ Training completed successfully: {len(results)} models processed")

        # Print summary
        for model_name, model_results in results.items():
            if 'error' not in model_results:
                logger.info(f"📊 {model_name}: {len(model_results)} variants trained")
            else:
                logger.error(f"❌ {model_name}: {model_results['error']}")

    except Exception as e:
        logger.error(f"❌ Training pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
