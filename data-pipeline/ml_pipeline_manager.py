#!/usr/bin/env python3
"""
Advanced ML Pipeline Manager for E-commerce DSS
Handles automated model training, versioning, and deployment
"""

import os
import sys
import json
import joblib
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

# ML Libraries
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, accuracy_score, classification_report
from sklearn.cluster import KMeans
# import xgboost as xgb  # Optional - fallback to sklearn

# Database connections
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

class MLPipelineManager:
    """
    Comprehensive ML Pipeline Manager for E-commerce Analytics
    """

    def __init__(self, db_config=None):
        self.models_dir = Path("./models")
        self.models_dir.mkdir(exist_ok=True)

        # Database configuration
        self.db_config = db_config or {
            'host': 'postgres',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        # Model registry
        self.model_registry = {}
        self.load_model_registry()

        # Logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_db_connection(self):
        """Get database connection"""
        try:
            conn_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            engine = create_engine(conn_string)
            return engine
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def load_data_for_training(self, query=None):
        """Load data from data warehouse for training"""
        if query is None:
            query = """
            SELECT
                product_price as price,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm,
                seller_state,
                customer_state,
                review_score,
                payment_value,
                freight_value,
                payment_type,
                product_category_name_english,
                order_status
            FROM dw_ecommerce_data
            WHERE product_price IS NOT NULL
                AND product_price > 0
                AND review_score IS NOT NULL
            LIMIT 100000
            """

        try:
            engine = self.get_db_connection()
            df = pd.read_sql(query, engine)
            self.logger.info(f"Loaded {len(df)} records for training")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load training data: {e}")
            raise

    def prepare_features(self, df):
        """Prepare features for ML training"""
        df = df.copy()

        # Handle categorical variables
        categorical_cols = ['seller_state', 'customer_state', 'payment_type',
                          'product_category_name_english', 'order_status']

        for col in categorical_cols:
            if col in df.columns:
                # Label encoding for high cardinality
                le = LabelEncoder()
                df[f'{col}_encoded'] = le.fit_transform(df[col].fillna('unknown'))

                # Save encoder
                encoder_path = self.models_dir / f'{col}_encoder.pkl'
                joblib.dump(le, encoder_path)

        # Create derived features
        if 'product_weight_g' in df.columns and 'product_length_cm' in df.columns:
            df['product_volume'] = (df['product_length_cm'] *
                                  df['product_height_cm'] *
                                  df['product_width_cm']).fillna(0)

            df['weight_to_volume_ratio'] = (df['product_weight_g'] /
                                          (df['product_volume'] + 1)).fillna(0)

        # Fill missing values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

        return df

    def train_price_prediction_model(self):
        """Train product price prediction model"""
        self.logger.info("Training price prediction model...")

        # Load and prepare data
        df = self.load_data_for_training()
        df = self.prepare_features(df)

        # Feature selection for price prediction
        feature_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm',
                       'product_width_cm', 'freight_value', 'product_volume',
                       'weight_to_volume_ratio', 'seller_state_encoded',
                       'product_category_name_english_encoded']

        # Remove any columns that don't exist
        feature_cols = [col for col in feature_cols if col in df.columns]

        if len(feature_cols) < 3:
            raise ValueError("Insufficient features for price prediction")

        X = df[feature_cols]
        y = df['price']

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        # Train Random Forest model (fallback for XGBoost)
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=6,
            random_state=42
        )

        model.fit(X_train_scaled, y_train)

        # Evaluate
        train_score = model.score(X_train_scaled, y_train)
        test_score = model.score(X_test_scaled, y_test)
        predictions = model.predict(X_test_scaled)
        rmse = np.sqrt(mean_squared_error(y_test, predictions))

        # Save model and scaler
        model_name = f'price_prediction_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        model_path = self.models_dir / f'{model_name}.pkl'
        scaler_path = self.models_dir / f'{model_name}_scaler.pkl'

        joblib.dump(model, model_path)
        joblib.dump(scaler, scaler_path)

        # Save metadata
        metadata = {
            'model_name': 'price_prediction',
            'model_type': 'RandomForestRegressor',
            'features': feature_cols,
            'train_score': float(train_score),
            'test_score': float(test_score),
            'rmse': float(rmse),
            'training_date': datetime.now().isoformat(),
            'model_path': str(model_path),
            'scaler_path': str(scaler_path),
            'data_size': len(df)
        }

        metadata_path = self.models_dir / f'{model_name}_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Update registry
        self.model_registry['price_prediction'] = metadata
        self.save_model_registry()

        self.logger.info(f"Price prediction model trained successfully!")
        self.logger.info(f"Train R²: {train_score:.3f}, Test R²: {test_score:.3f}, RMSE: {rmse:.2f}")

        return metadata

    def train_customer_segmentation_model(self):
        """Train customer segmentation model using RFM analysis"""
        self.logger.info("Training customer segmentation model...")

        # Load customer data for RFM analysis
        query = """
        SELECT
            customer_unique_id,
            COUNT(order_id) as frequency,
            SUM(payment_value) as monetary,
            MAX(order_purchase_timestamp) as last_purchase,
            CURRENT_DATE - MAX(order_purchase_timestamp::date) as recency_days
        FROM dw_ecommerce_data
        GROUP BY customer_unique_id
        HAVING COUNT(order_id) > 0 AND SUM(payment_value) > 0
        """

        engine = self.get_db_connection()
        df = pd.read_sql(query, engine)

        # Prepare RFM features
        df['recency'] = df['recency_days'].dt.days
        df['frequency'] = df['frequency']
        df['monetary'] = df['monetary']

        # Remove outliers
        for col in ['recency', 'frequency', 'monetary']:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            df = df[(df[col] >= Q1 - 1.5*IQR) & (df[col] <= Q3 + 1.5*IQR)]

        # Scale features
        features = ['recency', 'frequency', 'monetary']
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(df[features])

        # K-means clustering
        kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
        df['cluster'] = kmeans.fit_predict(X_scaled)

        # Analyze segments
        segment_analysis = df.groupby('cluster').agg({
            'recency': 'mean',
            'frequency': 'mean',
            'monetary': 'mean',
            'customer_unique_id': 'count'
        }).round(2)

        self.logger.info("Customer segments:")
        print(segment_analysis)

        # Save model
        model_name = f'customer_segmentation_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        model_path = self.models_dir / f'{model_name}.pkl'
        scaler_path = self.models_dir / f'{model_name}_scaler.pkl'

        joblib.dump(kmeans, model_path)
        joblib.dump(scaler, scaler_path)

        # Save metadata
        metadata = {
            'model_name': 'customer_segmentation',
            'model_type': 'KMeans',
            'features': features,
            'n_clusters': 5,
            'training_date': datetime.now().isoformat(),
            'model_path': str(model_path),
            'scaler_path': str(scaler_path),
            'data_size': len(df),
            'segment_analysis': segment_analysis.to_dict()
        }

        metadata_path = self.models_dir / f'{model_name}_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Update registry
        self.model_registry['customer_segmentation'] = metadata
        self.save_model_registry()

        self.logger.info("Customer segmentation model trained successfully!")
        return metadata

    def train_churn_prediction_model(self):
        """Train customer churn prediction model"""
        self.logger.info("Training churn prediction model...")

        # Load customer data with churn labels
        query = """
        SELECT
            customer_unique_id,
            COUNT(order_id) as total_orders,
            SUM(payment_value) as total_spent,
            AVG(review_score) as avg_review_score,
            MAX(order_purchase_timestamp) as last_order_date,
            MIN(order_purchase_timestamp) as first_order_date,
            CURRENT_DATE - MAX(order_purchase_timestamp::date) as days_since_last_order,
            CASE
                WHEN CURRENT_DATE - MAX(order_purchase_timestamp::date) > 180 THEN 1
                ELSE 0
            END as churned
        FROM dw_ecommerce_data
        GROUP BY customer_unique_id
        HAVING COUNT(order_id) > 1
        """

        engine = self.get_db_connection()
        df = pd.read_sql(query, engine)

        # Feature engineering
        df['days_as_customer'] = (pd.to_datetime(df['last_order_date']) -
                                 pd.to_datetime(df['first_order_date'])).dt.days
        df['avg_order_value'] = df['total_spent'] / df['total_orders']
        df['order_frequency'] = df['total_orders'] / (df['days_as_customer'] + 1)

        # Prepare features
        feature_cols = ['total_orders', 'total_spent', 'avg_review_score',
                       'days_since_last_order', 'days_as_customer',
                       'avg_order_value', 'order_frequency']

        # Remove missing values
        df = df.dropna(subset=feature_cols + ['churned'])

        X = df[feature_cols]
        y = df['churned']

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        # Train Random Forest classifier
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'
        )

        model.fit(X_train_scaled, y_train)

        # Evaluate
        train_score = model.score(X_train_scaled, y_train)
        test_score = model.score(X_test_scaled, y_test)
        predictions = model.predict(X_test_scaled)

        # Save model
        model_name = f'churn_prediction_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        model_path = self.models_dir / f'{model_name}.pkl'
        scaler_path = self.models_dir / f'{model_name}_scaler.pkl'

        joblib.dump(model, model_path)
        joblib.dump(scaler, scaler_path)

        # Feature importance
        feature_importance = dict(zip(feature_cols, model.feature_importances_))

        # Save metadata
        metadata = {
            'model_name': 'churn_prediction',
            'model_type': 'RandomForestClassifier',
            'features': feature_cols,
            'train_score': float(train_score),
            'test_score': float(test_score),
            'training_date': datetime.now().isoformat(),
            'model_path': str(model_path),
            'scaler_path': str(scaler_path),
            'data_size': len(df),
            'feature_importance': feature_importance,
            'churn_rate': float(y.mean())
        }

        metadata_path = self.models_dir / f'{model_name}_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Update registry
        self.model_registry['churn_prediction'] = metadata
        self.save_model_registry()

        self.logger.info(f"Churn prediction model trained successfully!")
        self.logger.info(f"Train Accuracy: {train_score:.3f}, Test Accuracy: {test_score:.3f}")
        self.logger.info(f"Overall churn rate: {metadata['churn_rate']:.3f}")

        return metadata

    def load_model_registry(self):
        """Load model registry from file"""
        registry_path = self.models_dir / 'model_registry.json'
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                self.model_registry = json.load(f)
        else:
            self.model_registry = {}

    def save_model_registry(self):
        """Save model registry to file"""
        registry_path = self.models_dir / 'model_registry.json'
        with open(registry_path, 'w') as f:
            json.dump(self.model_registry, f, indent=2)

    def get_model_info(self, model_name=None):
        """Get information about models"""
        if model_name:
            return self.model_registry.get(model_name)
        return self.model_registry

    def train_all_models(self):
        """Train all available models"""
        self.logger.info("Starting comprehensive ML training pipeline...")

        results = {}

        try:
            results['price_prediction'] = self.train_price_prediction_model()
        except Exception as e:
            self.logger.error(f"Price prediction training failed: {e}")
            results['price_prediction'] = {'error': str(e)}

        try:
            results['customer_segmentation'] = self.train_customer_segmentation_model()
        except Exception as e:
            self.logger.error(f"Customer segmentation training failed: {e}")
            results['customer_segmentation'] = {'error': str(e)}

        try:
            results['churn_prediction'] = self.train_churn_prediction_model()
        except Exception as e:
            self.logger.error(f"Churn prediction training failed: {e}")
            results['churn_prediction'] = {'error': str(e)}

        self.logger.info("ML training pipeline completed!")
        return results

def main():
    """Main function to run ML pipeline"""
    pipeline = MLPipelineManager()

    # Train all models
    results = pipeline.train_all_models()

    # Print summary
    print("\n" + "="*50)
    print("ML PIPELINE TRAINING SUMMARY")
    print("="*50)

    for model_name, result in results.items():
        print(f"\n{model_name.upper()}:")
        if 'error' in result:
            print(f"  ❌ Training failed: {result['error']}")
        else:
            print(f"  ✅ Training successful")
            if 'test_score' in result:
                print(f"  📊 Test Score: {result['test_score']:.3f}")
            if 'data_size' in result:
                print(f"  📈 Training Data: {result['data_size']:,} records")

if __name__ == "__main__":
    main()