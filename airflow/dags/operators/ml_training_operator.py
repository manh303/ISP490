#!/usr/bin/env python3
"""
ML Training Operator for Airflow
Handles model training, validation, and deployment in production pipeline
"""

import os
import json
import pickle
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

# Airflow imports
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

# Data processing and ML
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.cluster import KMeans
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    mean_squared_error, mean_absolute_error, r2_score,
    classification_report, confusion_matrix, silhouette_score
)
from sklearn.feature_selection import SelectKBest, f_classif, f_regression
import joblib

# Advanced ML libraries
try:
    import xgboost as xgb
    import lightgbm as lgb
    ADVANCED_ML_AVAILABLE = True
except ImportError:
    ADVANCED_ML_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

class MLTrainingOperator(BaseOperator):
    """
    Comprehensive ML Training Operator for automated model training and deployment
    
    Features:
    - Multiple model types support
    - Automated feature engineering
    - Hyperparameter optimization
    - Model validation and comparison
    - Model versioning and deployment
    - Performance monitoring
    """
    
    template_fields = ['model_types', 'training_data_query', 'model_config']
    
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        model_types: List[str] = None,
        training_data_query: Optional[str] = None,
        model_config: Optional[Dict[str, Any]] = None,
        models_output_path: str = '/app/models',
        validation_split: float = 0.2,
        enable_hyperparameter_tuning: bool = True,
        enable_feature_selection: bool = True,
        model_performance_threshold: float = 0.7,
        auto_deploy: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.model_types = model_types or ['customer_segmentation', 'product_recommendation']
        self.training_data_query = training_data_query
        self.model_config = model_config or {}
        self.models_output_path = Path(models_output_path)
        self.validation_split = validation_split
        self.enable_hyperparameter_tuning = enable_hyperparameter_tuning
        self.enable_feature_selection = enable_feature_selection
        self.model_performance_threshold = model_performance_threshold
        self.auto_deploy = auto_deploy
        
        # Ensure models directory exists
        self.models_output_path.mkdir(parents=True, exist_ok=True)
        
        # Training results storage
        self.training_results = {
            'timestamp': datetime.now(),
            'models_trained': 0,
            'successful_deployments': 0,
            'failed_trainings': 0,
            'model_results': {}
        }

    def execute(self, context: Context) -> Dict[str, Any]:
        """Main execution method"""
        logger.info("🤖 Starting ML model training pipeline...")
        
        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            
            # Train each model type
            for model_type in self.model_types:
                logger.info(f"🔥 Training model: {model_type}")
                
                try:
                    model_result = self._train_model(postgres_hook, model_type, context)
                    self.training_results['model_results'][model_type] = model_result
                    self.training_results['models_trained'] += 1
                    
                    # Auto-deploy if enabled and performance is good
                    if (self.auto_deploy and 
                        model_result.get('performance_score', 0) >= self.model_performance_threshold):
                        self._deploy_model(model_type, model_result, context)
                        self.training_results['successful_deployments'] += 1
                    
                except Exception as model_error:
                    logger.error(f"❌ Failed to train {model_type}: {model_error}")
                    self.training_results['failed_trainings'] += 1
                    self.training_results['model_results'][model_type] = {
                        'status': 'failed',
                        'error': str(model_error),
                        'timestamp': datetime.now()
                    }
            
            # Store training results
            self._store_training_results(postgres_hook, context)
            
            # Generate summary report
            self._generate_training_report(context)
            
            logger.info(f"✅ ML training completed. "
                       f"Trained: {self.training_results['models_trained']}, "
                       f"Deployed: {self.training_results['successful_deployments']}")
            
            return self.training_results
            
        except Exception as e:
            logger.error(f"❌ ML training pipeline failed: {e}")
            raise AirflowException(f"ML training failed: {e}")

    def _train_model(self, postgres_hook: PostgresHook, model_type: str, context: Context) -> Dict[str, Any]:
        """Train a specific model type"""
        
        # Get training data
        training_data = self._get_training_data(postgres_hook, model_type)
        
        if training_data.empty:
            raise ValueError(f"No training data available for {model_type}")
        
        logger.info(f"📊 Training data shape: {training_data.shape}")
        
        # Prepare features and target
        X, y, feature_names = self._prepare_features_and_target(training_data, model_type)
        
        # Feature selection (if enabled)
        if self.enable_feature_selection:
            X, selected_features = self._select_features(X, y, feature_names, model_type)
        else:
            selected_features = feature_names
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.validation_split, random_state=42, stratify=y if self._is_classification_task(model_type) else None
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = self._create_and_train_model(X_train_scaled, y_train, model_type)
        
        # Validate model
        performance_metrics = self._validate_model(model, X_test_scaled, y_test, model_type)
        
        # Save model artifacts
        model_artifacts = self._save_model_artifacts(
            model, scaler, selected_features, performance_metrics, model_type, context
        )
        
        return {
            'model_type': model_type,
            'status': 'success',
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'feature_count': len(selected_features),
            'selected_features': selected_features,
            'performance_metrics': performance_metrics,
            'performance_score': self._calculate_performance_score(performance_metrics, model_type),
            'model_artifacts': model_artifacts,
            'timestamp': datetime.now()
        }

    def _get_training_data(self, postgres_hook: PostgresHook, model_type: str) -> pd.DataFrame:
        """Get training data for specific model type"""
        
        # Define queries for different model types
        queries = {
            'customer_segmentation': """
                SELECT 
                    c.customer_state,
                    c.customer_segment,
                    COUNT(DISTINCT o.order_id) as total_orders,
                    SUM(o.total_amount) as total_spent,
                    AVG(o.total_amount) as avg_order_value,
                    COUNT(DISTINCT oi.product_id) as unique_products,
                    MAX(o.order_date) as last_order_date,
                    MIN(o.order_date) as first_order_date,
                    EXTRACT(days FROM (MAX(o.order_date) - MIN(o.order_date))) as customer_lifetime_days
                FROM customers_processed c
                LEFT JOIN orders_processed o ON c.customer_id = o.customer_id
                LEFT JOIN order_items_processed oi ON o.order_id = oi.order_id
                WHERE c.processed_at >= NOW() - INTERVAL '30 days'
                GROUP BY c.customer_id, c.customer_state, c.customer_segment
                HAVING COUNT(o.order_id) > 0
            """,
            
            'product_recommendation': """
                SELECT 
                    p.product_category_clean,
                    p.product_quality_score,
                    p.product_weight_g,
                    p.product_volume_cm3,
                    p.weight_category,
                    COUNT(oi.order_id) as order_frequency,
                    AVG(oi.price) as avg_price,
                    SUM(oi.quantity) as total_quantity_sold,
                    COUNT(DISTINCT oi.customer_id) as unique_customers,
                    AVG(oi.price * oi.quantity) as avg_revenue_per_order
                FROM products_processed p
                LEFT JOIN order_items_processed oi ON p.product_id = oi.product_id
                WHERE p.processed_at >= NOW() - INTERVAL '30 days'
                GROUP BY p.product_id, p.product_category_clean, p.product_quality_score, 
                         p.product_weight_g, p.product_volume_cm3, p.weight_category
            """,
            
            'demand_forecasting': """
                SELECT 
                    DATE(o.order_date) as order_date,
                    p.product_category_clean,
                    COUNT(o.order_id) as daily_orders,
                    SUM(oi.quantity) as daily_quantity,
                    AVG(oi.price) as avg_daily_price,
                    EXTRACT(dow FROM o.order_date) as day_of_week,
                    EXTRACT(hour FROM o.order_date) as hour_of_day,
                    CASE WHEN EXTRACT(dow FROM o.order_date) IN (0,6) THEN 1 ELSE 0 END as is_weekend
                FROM orders_processed o
                JOIN order_items_processed oi ON o.order_id = oi.order_id
                JOIN products_processed p ON oi.product_id = p.product_id
                WHERE o.order_date >= NOW() - INTERVAL '90 days'
                GROUP BY DATE(o.order_date), p.product_category_clean,
                         EXTRACT(dow FROM o.order_date), EXTRACT(hour FROM o.order_date)
                ORDER BY order_date
            """
        }
        
        # Use custom query if provided
        query = self.training_data_query or queries.get(model_type)
        
        if not query:
            raise ValueError(f"No training query defined for model type: {model_type}")
        
        return postgres_hook.get_pandas_df(query)

    def _prepare_features_and_target(self, data: pd.DataFrame, model_type: str) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare features and target variables"""
        
        df = data.copy()
        
        # Handle missing values
        df = df.fillna(0)  # Simple strategy, could be more sophisticated
        
        if model_type == 'customer_segmentation':
            # Features for customer segmentation
            feature_cols = ['total_orders', 'total_spent', 'avg_order_value', 'unique_products', 'customer_lifetime_days']
            
            # Create target variable (customer value tiers)
            df['customer_value_tier'] = pd.qcut(
                df['total_spent'], 
                q=4, 
                labels=['Low', 'Medium', 'High', 'Premium']
            )
            
            # Encode categorical target
            le = LabelEncoder()
            y = le.fit_transform(df['customer_value_tier'])
            
        elif model_type == 'product_recommendation':
            # Features for product recommendation
            feature_cols = ['product_quality_score', 'product_weight_g', 'product_volume_cm3', 
                          'avg_price', 'unique_customers']
            
            # Create target (high demand products)
            demand_threshold = df['order_frequency'].quantile(0.7)
            y = (df['order_frequency'] > demand_threshold).astype(int)
            
        elif model_type == 'demand_forecasting':
            # Features for demand forecasting
            feature_cols = ['day_of_week', 'hour_of_day', 'is_weekend', 'avg_daily_price']
            
            # Target is daily quantity
            y = df['daily_quantity'].values
            
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Get features
        X = df[feature_cols].values
        
        return X, y, feature_cols

    def _select_features(self, X: np.ndarray, y: np.ndarray, feature_names: List[str], model_type: str) -> Tuple[np.ndarray, List[str]]:
        """Feature selection using statistical methods"""
        
        logger.info(f"🔍 Performing feature selection for {model_type}...")
        
        # Determine if classification or regression task
        if self._is_classification_task(model_type):
            selector = SelectKBest(score_func=f_classif, k='all')
        else:
            selector = SelectKBest(score_func=f_regression, k='all')
        
        # Fit selector
        X_selected = selector.fit_transform(X, y)
        
        # Get feature scores
        feature_scores = selector.scores_
        
        # Select top features (keep features with score above median)
        score_threshold = np.median(feature_scores)
        selected_indices = feature_scores >= score_threshold
        
        # Ensure we keep at least 3 features
        if selected_indices.sum() < 3:
            top_indices = np.argsort(feature_scores)[-3:]
            selected_indices = np.zeros_like(feature_scores, dtype=bool)
            selected_indices[top_indices] = True
        
        selected_features = [feature_names[i] for i in range(len(feature_names)) if selected_indices[i]]
        X_final = X[:, selected_indices]
        
        logger.info(f"✅ Selected {len(selected_features)} features: {selected_features}")
        
        return X_final, selected_features

    def _create_and_train_model(self, X_train: np.ndarray, y_train: np.ndarray, model_type: str):
        """Create and train model with optional hyperparameter tuning"""
        
        logger.info(f"🏗️ Creating and training {model_type} model...")
        
        if model_type == 'customer_segmentation':
            # Multi-class classification
            if self.enable_hyperparameter_tuning:
                param_grid = {
                    'n_estimators': [100, 200],
                    'max_depth': [10, 20, None],
                    'min_samples_split': [2, 5],
                    'min_samples_leaf': [1, 2]
                }
                
                model = GridSearchCV(
                    RandomForestClassifier(random_state=42),
                    param_grid,
                    cv=3,
                    scoring='f1_weighted',
                    n_jobs=-1
                )
            else:
                model = RandomForestClassifier(n_estimators=100, random_state=42)
                
        elif model_type == 'product_recommendation':
            # Binary classification
            if ADVANCED_ML_AVAILABLE and self.enable_hyperparameter_tuning:
                param_grid = {
                    'n_estimators': [100, 200],
                    'max_depth': [6, 10],
                    'learning_rate': [0.1, 0.01],
                    'subsample': [0.8, 1.0]
                }
                
                model = GridSearchCV(
                    xgb.XGBClassifier(random_state=42),
                    param_grid,
                    cv=3,
                    scoring='roc_auc',
                    n_jobs=-1
                )
            else:
                model = LogisticRegression(random_state=42, max_iter=1000)
                
        elif model_type == 'demand_forecasting':
            # Regression
            if ADVANCED_ML_AVAILABLE and self.enable_hyperparameter_tuning:
                param_grid = {
                    'n_estimators': [100, 200],
                    'max_depth': [6, 10],
                    'learning_rate': [0.1, 0.01]
                }
                
                model = GridSearchCV(
                    lgb.LGBMRegressor(random_state=42),
                    param_grid,
                    cv=3,
                    scoring='neg_mean_squared_error',
                    n_jobs=-1
                )
            else:
                model = RandomForestRegressor(n_estimators=100, random_state=42)
        
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train the model
        model.fit(X_train, y_train)
        
        # If GridSearchCV was used, extract the best model
        if hasattr(model, 'best_estimator_'):
            logger.info(f"🎯 Best parameters: {model.best_params_}")
            return model.best_estimator_
        
        return model

    def _validate_model(self, model, X_test: np.ndarray, y_test: np.ndarray, model_type: str) -> Dict[str, Any]:
        """Validate model performance"""
        
        logger.info(f"✅ Validating {model_type} model...")
        
        # Make predictions
        y_pred = model.predict(X_test)
        
        metrics = {
            'model_type': model_type,
            'test_samples': len(X_test),
            'validation_timestamp': datetime.now()
        }
        
        if self._is_classification_task(model_type):
            # Classification metrics
            metrics.update({
                'accuracy': float(accuracy_score(y_test, y_pred)),
                'precision': float(precision_score(y_test, y_pred, average='weighted', zero_division=0)),
                'recall': float(recall_score(y_test, y_pred, average='weighted', zero_division=0)),
                'f1_score': float(f1_score(y_test, y_pred, average='weighted', zero_division=0))
            })
            
            # Add prediction probabilities if available
            if hasattr(model, 'predict_proba'):
                y_prob = model.predict_proba(X_test)
                # For binary classification, add AUC score
                if y_prob.shape[1] == 2:
                    from sklearn.metrics import roc_auc_score
                    metrics['roc_auc'] = float(roc_auc_score(y_test, y_prob[:, 1]))
            
            # Confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            metrics['confusion_matrix'] = cm.tolist()
            
        else:
            # Regression metrics
            metrics.update({
                'mse': float(mean_squared_error(y_test, y_pred)),
                'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
                'mae': float(mean_absolute_error(y_test, y_pred)),
                'r2_score': float(r2_score(y_test, y_pred))
            })
        
        # Cross-validation score
        try:
            if self._is_classification_task(model_type):
                cv_scores = cross_val_score(model, X_test, y_test, cv=3, scoring='f1_weighted')
            else:
                cv_scores = cross_val_score(model, X_test, y_test, cv=3, scoring='neg_mean_squared_error')
                cv_scores = -cv_scores  # Convert negative MSE to positive
            
            metrics['cross_val_mean'] = float(cv_scores.mean())
            metrics['cross_val_std'] = float(cv_scores.std())
        except Exception as cv_error:
            logger.warning(f"Cross-validation failed: {cv_error}")
        
        return metrics

    def _calculate_performance_score(self, metrics: Dict[str, Any], model_type: str) -> float:
        """Calculate single performance score for model comparison"""
        
        if self._is_classification_task(model_type):
            # For classification, use F1 score as primary metric
            return metrics.get('f1_score', 0.0)
        else:
            # For regression, use R² score (adjusted to 0-1 range)
            r2 = metrics.get('r2_score', 0.0)
            return max(0.0, min(1.0, r2))  # Clamp to [0, 1]

    def _is_classification_task(self, model_type: str) -> bool:
        """Determine if model type is classification or regression"""
        classification_tasks = ['customer_segmentation', 'product_recommendation', 'churn_prediction']
        return model_type in classification_tasks

    def _save_model_artifacts(self, model, scaler, selected_features: List[str], metrics: Dict[str, Any], 
                            model_type: str, context: Context) -> Dict[str, str]:
        """Save model and related artifacts"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        execution_date = context['execution_date'].strftime('%Y%m%d')
        
        # Create versioned directory
        model_version_dir = self.models_output_path / f"{model_type}_v{timestamp}"
        model_version_dir.mkdir(parents=True, exist_ok=True)
        
        artifacts = {}
        
        # Save main model
        model_path = model_version_dir / f"{model_type}_model.pkl"
        joblib.dump(model, model_path)
        artifacts['model_path'] = str(model_path)
        
        # Save scaler
        scaler_path = model_version_dir / f"{model_type}_scaler.pkl"
        joblib.dump(scaler, scaler_path)
        artifacts['scaler_path'] = str(scaler_path)
        
        # Save feature names
        features_path = model_version_dir / f"{model_type}_features.json"
        with open(features_path, 'w') as f:
            json.dump(selected_features, f)
        artifacts['features_path'] = str(features_path)
        
        # Save metadata
        metadata = {
            'model_type': model_type,
            'training_timestamp': datetime.now().isoformat(),
            'execution_date': execution_date,
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'selected_features': selected_features,
            'performance_metrics': metrics,
            'model_version': timestamp,
            'artifacts': artifacts
        }
        
        metadata_path = model_version_dir / f"{model_type}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, default=str, indent=2)
        artifacts['metadata_path'] = str(metadata_path)
        
        # Create symlink to latest version (for easy access)
        latest_link = self.models_output_path / f"{model_type}_latest"
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        latest_link.symlink_to(model_version_dir.name)
        
        logger.info(f"💾 Model artifacts saved to: {model_version_dir}")
        
        return artifacts

    def _deploy_model(self, model_type: str, model_result: Dict[str, Any], context: Context):
        """Deploy model to production environment"""
        
        logger.info(f"🚀 Deploying {model_type} model...")
        
        try:
            # Copy model artifacts to production directory
            production_dir = self.models_output_path / "production"
            production_dir.mkdir(parents=True, exist_ok=True)
            
            # Get artifact paths
            artifacts = model_result['model_artifacts']
            
            # Copy files to production
            import shutil
            for artifact_type, source_path in artifacts.items():
                if artifact_type.endswith('_path'):
                    source = Path(source_path)
                    target = production_dir / f"{model_type}_{source.name}"
                    shutil.copy2(source, target)
                    logger.info(f"📄 Copied {artifact_type}: {target}")
            
            # Update deployment record
            deployment_record = {
                'model_type': model_type,
                'deployment_timestamp': datetime.now().isoformat(),
                'model_version': model_result.get('timestamp'),
                'performance_score': model_result.get('performance_score'),
                'dag_run_id': context['dag_run'].run_id,
                'status': 'deployed'
            }
            
            # Save deployment record
            deployment_log = production_dir / f"{model_type}_deployment.json"
            with open(deployment_log, 'w') as f:
                json.dump(deployment_record, f, default=str, indent=2)
            
            logger.info(f"✅ {model_type} model deployed successfully")
            
        except Exception as e:
            logger.error(f"❌ Deployment failed for {model_type}: {e}")
            raise

    def _store_training_results(self, postgres_hook: PostgresHook, context: Context):
        """Store training results in database"""
        
        try:
            insert_query = """
            INSERT INTO ml_training_results (
                dag_id, task_id, execution_date, run_id,
                models_trained, successful_deployments, failed_trainings,
                training_results, created_at
            ) VALUES (
                %(dag_id)s, %(task_id)s, %(execution_date)s, %(run_id)s,
                %(models_trained)s, %(successful_deployments)s, %(failed_trainings)s,
                %(training_results)s, %(created_at)s
            )
            """
            
            params = {
                'dag_id': context['dag'].dag_id,
                'task_id': context['task'].task_id,
                'execution_date': context['execution_date'],
                'run_id': context['dag_run'].run_id,
                'models_trained': self.training_results['models_trained'],
                'successful_deployments': self.training_results['successful_deployments'],
                'failed_trainings': self.training_results['failed_trainings'],
                'training_results': json.dumps(self.training_results, default=str),
                'created_at': datetime.now()
            }
            
            postgres_hook.run(insert_query, parameters=params)
            logger.info("✅ Training results stored in database")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to store training results: {e}")

    def _generate_training_report(self, context: Context):
        """Generate comprehensive training report"""
        
        report = {
            'training_summary': {
                'execution_date': context['execution_date'].isoformat(),
                'dag_id': context['dag'].dag_id,
                'models_trained': self.training_results['models_trained'],
                'successful_deployments': self.training_results['successful_deployments'],
                'failed_trainings': self.training_results['failed_trainings'],
                'overall_success_rate': (
                    self.training_results['models_trained'] / len(self.model_types)
                    if self.model_types else 0
                )
            },
            'model_details': {}
        }
        
        # Add model-specific details
        for model_type, result in self.training_results['model_results'].items():
            if result['status'] == 'success':
                report['model_details'][model_type] = {
                    'performance_score': result.get('performance_score', 0),
                    'training_samples': result.get('training_samples', 0),
                    'feature_count': result.get('feature_count', 0),
                    'key_metrics': self._extract_key_metrics(result.get('performance_metrics', {}))
                }
            else:
                report['model_details'][model_type] = {
                    'status': 'failed',
                    'error': result.get('error', 'Unknown error')
                }
        
        # Save report
        report_path = self.models_output_path / f"training_report_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, default=str, indent=2)
        
        logger.info(f"📊 Training report saved: {report_path}")
        
        return report

    def _extract_key_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key metrics for reporting"""
        
        key_metrics = {}
        
        # Classification metrics
        if 'accuracy' in metrics:
            key_metrics['accuracy'] = metrics['accuracy']
        if 'f1_score' in metrics:
            key_metrics['f1_score'] = metrics['f1_score']
        if 'roc_auc' in metrics:
            key_metrics['roc_auc'] = metrics['roc_auc']
        
        # Regression metrics
        if 'r2_score' in metrics:
            key_metrics['r2_score'] = metrics['r2_score']
        if 'rmse' in metrics:
            key_metrics['rmse'] = metrics['rmse']
        
        # Cross-validation
        if 'cross_val_mean' in metrics:
            key_metrics['cv_score'] = metrics['cross_val_mean']
        
        return key_metrics