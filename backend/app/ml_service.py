#!/usr/bin/env python3
"""
Advanced ML Service for Real-time Predictions
Integrates with the main FastAPI backend
"""

import os
import json
import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# ML Libraries
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

# Database
from databases import Database
import asyncio

class MLInferenceService:
    """
    Production-ready ML Inference Service
    Handles real-time predictions for various models
    """

    def __init__(self, models_path: str = "/app/models"):
        self.models_path = Path(models_path)
        self.models = {}
        self.scalers = {}
        self.metadata = {}

        # Ensure models directory exists
        self.models_path.mkdir(exist_ok=True)

        # Load all available models
        self.load_models()

    def load_models(self):
        """Load all available ML models and their metadata"""
        try:
            # Load model registry if exists
            registry_path = self.models_path / 'model_registry.json'
            if registry_path.exists():
                with open(registry_path, 'r') as f:
                    registry = json.load(f)

                for model_name, info in registry.items():
                    self.load_single_model(model_name, info)

            # Also load models from data-pipeline directory
            pipeline_models_path = Path("./data-pipeline/models")
            if pipeline_models_path.exists():
                self.load_pipeline_models(pipeline_models_path)

        except Exception as e:
            print(f"Error loading models: {e}")

    def load_pipeline_models(self, models_path: Path):
        """Load existing models from data-pipeline"""
        try:
            # Load CLV model
            clv_model_path = models_path / "clv_model.pkl"
            if clv_model_path.exists():
                self.models['clv_prediction'] = joblib.load(clv_model_path)
                print("✅ Loaded CLV prediction model")

            # Load churn model
            churn_model_path = models_path / "churn_model.pkl"
            if churn_model_path.exists():
                self.models['churn_prediction'] = joblib.load(churn_model_path)
                print("✅ Loaded churn prediction model")

            # Load clustering model
            clustering_model_path = models_path / "customer_clustering_model.pkl"
            clustering_scaler_path = models_path / "clustering_scaler.pkl"
            if clustering_model_path.exists():
                self.models['customer_segmentation'] = joblib.load(clustering_model_path)
                if clustering_scaler_path.exists():
                    self.scalers['customer_segmentation'] = joblib.load(clustering_scaler_path)
                print("✅ Loaded customer segmentation model")

        except Exception as e:
            print(f"Error loading pipeline models: {e}")

    def load_single_model(self, model_name: str, model_info: Dict):
        """Load a single model with its metadata"""
        try:
            model_path = Path(model_info['model_path'])
            if model_path.exists():
                self.models[model_name] = joblib.load(model_path)

                # Load scaler if exists
                if 'scaler_path' in model_info:
                    scaler_path = Path(model_info['scaler_path'])
                    if scaler_path.exists():
                        self.scalers[model_name] = joblib.load(scaler_path)

                # Store metadata
                self.metadata[model_name] = model_info
                print(f"✅ Loaded model: {model_name}")

        except Exception as e:
            print(f"Error loading model {model_name}: {e}")

    async def predict_customer_lifetime_value(self, customer_features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict Customer Lifetime Value
        """
        if 'clv_prediction' not in self.models:
            raise ValueError("CLV prediction model not available")

        try:
            # Expected features for CLV prediction
            expected_features = [
                'recency', 'frequency', 'monetary', 'avg_order_value',
                'days_as_customer', 'avg_review_score', 'total_orders'
            ]

            # Create feature vector
            features = {}
            for feature in expected_features:
                features[feature] = customer_features.get(feature, 0)

            # Convert to DataFrame
            df = pd.DataFrame([features])

            # Apply scaling if scaler exists
            if 'clv_prediction' in self.scalers:
                df_scaled = self.scalers['clv_prediction'].transform(df)
                df = pd.DataFrame(df_scaled, columns=df.columns)

            # Make prediction
            model = self.models['clv_prediction']
            prediction = model.predict(df)[0]

            # Calculate confidence intervals (simplified)
            confidence = min(max(features['frequency'] / 10, 0.5), 0.95)

            return {
                'predicted_clv': float(prediction),
                'confidence': float(confidence),
                'risk_level': 'low' if prediction > 500 else 'medium' if prediction > 200 else 'high',
                'recommendation': self._get_clv_recommendation(prediction),
                'features_used': features,
                'model_timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            raise ValueError(f"CLV prediction failed: {str(e)}")

    async def predict_churn_probability(self, customer_features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict customer churn probability
        """
        if 'churn_prediction' not in self.models:
            raise ValueError("Churn prediction model not available")

        try:
            # Expected features for churn prediction
            expected_features = [
                'total_orders', 'total_spent', 'avg_review_score',
                'days_since_last_order', 'days_as_customer',
                'avg_order_value', 'order_frequency'
            ]

            # Create feature vector
            features = {}
            for feature in expected_features:
                features[feature] = customer_features.get(feature, 0)

            # Convert to DataFrame
            df = pd.DataFrame([features])

            # Apply scaling if scaler exists
            if 'churn_prediction' in self.scalers:
                df_scaled = self.scalers['churn_prediction'].transform(df)
                df = pd.DataFrame(df_scaled, columns=df.columns)

            # Make prediction
            model = self.models['churn_prediction']
            churn_proba = model.predict_proba(df)[0][1]  # Probability of churn (class 1)
            churn_prediction = model.predict(df)[0]

            # Risk categorization
            if churn_proba >= 0.7:
                risk_level = 'high'
            elif churn_proba >= 0.4:
                risk_level = 'medium'
            else:
                risk_level = 'low'

            return {
                'churn_probability': float(churn_proba),
                'churn_prediction': bool(churn_prediction),
                'risk_level': risk_level,
                'recommendation': self._get_churn_recommendation(churn_proba),
                'key_factors': self._analyze_churn_factors(features),
                'features_used': features,
                'model_timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            raise ValueError(f"Churn prediction failed: {str(e)}")

    async def predict_customer_segment(self, customer_features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict customer segment using RFM clustering
        """
        if 'customer_segmentation' not in self.models:
            raise ValueError("Customer segmentation model not available")

        try:
            # Expected features for segmentation
            expected_features = ['recency', 'frequency', 'monetary']

            # Create feature vector
            features = {}
            for feature in expected_features:
                features[feature] = customer_features.get(feature, 0)

            # Convert to DataFrame
            df = pd.DataFrame([features])

            # Apply scaling
            if 'customer_segmentation' in self.scalers:
                df_scaled = self.scalers['customer_segmentation'].transform(df)
            else:
                df_scaled = df.values

            # Make prediction
            model = self.models['customer_segmentation']
            segment = model.predict(df_scaled)[0]

            # Map segment to business meaning
            segment_mapping = {
                0: {'name': 'Champions', 'description': 'High value, recent, frequent customers'},
                1: {'name': 'Loyal Customers', 'description': 'Regular customers with good value'},
                2: {'name': 'Potential Loyalists', 'description': 'Recent customers with potential'},
                3: {'name': 'At Risk', 'description': 'Previously good customers, now declining'},
                4: {'name': 'Lost Customers', 'description': 'Haven\'t purchased recently, low frequency'}
            }

            segment_info = segment_mapping.get(segment, {
                'name': f'Segment {segment}',
                'description': 'Custom segment based on RFM analysis'
            })

            return {
                'segment_id': int(segment),
                'segment_name': segment_info['name'],
                'segment_description': segment_info['description'],
                'marketing_strategy': self._get_segment_strategy(segment),
                'rfm_scores': features,
                'model_timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            raise ValueError(f"Customer segmentation failed: {str(e)}")

    async def get_product_recommendations(self, customer_id: str, product_features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate product recommendations (collaborative filtering simulation)
        """
        try:
            # This is a simplified recommendation system
            # In production, you'd use collaborative filtering, content-based, or hybrid approaches

            # Simulate recommendations based on customer segment and product features
            recommendations = []

            # Mock popular products by category
            popular_products = {
                'electronics': ['smartphone_pro', 'laptop_ultra', 'headphones_premium'],
                'fashion': ['summer_dress', 'running_shoes', 'leather_jacket'],
                'home': ['smart_tv', 'coffee_maker', 'vacuum_robot'],
                'books': ['data_science_book', 'fiction_bestseller', 'cooking_guide']
            }

            # Get customer preferences (mock data)
            preferred_category = product_features.get('category', 'electronics')
            budget = product_features.get('budget', 500)

            # Generate recommendations
            category_products = popular_products.get(preferred_category, popular_products['electronics'])

            for i, product in enumerate(category_products):
                score = np.random.uniform(0.7, 0.95)  # Mock relevance score
                recommendations.append({
                    'product_id': product,
                    'relevance_score': float(score),
                    'estimated_price': float(budget * np.random.uniform(0.5, 1.5)),
                    'reason': f'Based on your preferences in {preferred_category}',
                    'rank': i + 1
                })

            return {
                'customer_id': customer_id,
                'recommendations': recommendations,
                'algorithm': 'hybrid_collaborative_content',
                'recommendation_timestamp': datetime.now().isoformat(),
                'total_recommendations': len(recommendations)
            }

        except Exception as e:
            raise ValueError(f"Product recommendation failed: {str(e)}")

    def _get_clv_recommendation(self, clv_value: float) -> str:
        """Get CLV-based recommendation"""
        if clv_value > 1000:
            return "VIP treatment - offer premium services and exclusive deals"
        elif clv_value > 500:
            return "High-value customer - provide personalized offers and priority support"
        elif clv_value > 200:
            return "Growing customer - encourage more purchases with targeted promotions"
        else:
            return "Development opportunity - focus on engagement and value demonstration"

    def _get_churn_recommendation(self, churn_prob: float) -> str:
        """Get churn-based recommendation"""
        if churn_prob >= 0.7:
            return "Immediate intervention needed - offer significant incentives to retain"
        elif churn_prob >= 0.4:
            return "Moderate risk - engage with personalized offers and improved service"
        else:
            return "Low risk - maintain regular engagement and monitor satisfaction"

    def _analyze_churn_factors(self, features: Dict[str, Any]) -> List[str]:
        """Analyze key factors contributing to churn risk"""
        factors = []

        if features.get('days_since_last_order', 0) > 90:
            factors.append("Long time since last purchase")

        if features.get('avg_review_score', 5) < 3:
            factors.append("Low satisfaction scores")

        if features.get('order_frequency', 0) < 0.1:
            factors.append("Low purchase frequency")

        if features.get('total_orders', 0) < 2:
            factors.append("Limited purchase history")

        return factors or ["No significant risk factors identified"]

    def _get_segment_strategy(self, segment_id: int) -> str:
        """Get marketing strategy for customer segment"""
        strategies = {
            0: "Reward loyalty, offer exclusive products, ask for referrals",
            1: "Upsell premium products, loyalty programs, VIP treatment",
            2: "Nurture with educational content, targeted offers, build loyalty",
            3: "Re-engagement campaigns, win-back offers, address concerns",
            4: "Aggressive win-back campaigns, deep discounts, survey feedback"
        }
        return strategies.get(segment_id, "Analyze customer behavior and create targeted strategy")

    def get_available_models(self) -> Dict[str, Any]:
        """Get information about available models"""
        model_info = {}
        for model_name in self.models.keys():
            model_info[model_name] = {
                'available': True,
                'has_scaler': model_name in self.scalers,
                'metadata': self.metadata.get(model_name, {})
            }
        return model_info

    async def run_model_health_check(self) -> Dict[str, Any]:
        """Run health check on all models"""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'total_models': len(self.models),
            'models': {}
        }

        for model_name, model in self.models.items():
            try:
                # Simple test prediction with dummy data
                if hasattr(model, 'predict'):
                    # Create dummy input based on model type
                    if model_name == 'customer_segmentation':
                        dummy_input = np.array([[30, 5, 500]])  # recency, frequency, monetary
                    else:
                        dummy_input = np.array([[1, 2, 3, 4, 5]])  # generic dummy input

                    prediction = model.predict(dummy_input)
                    health_status['models'][model_name] = {
                        'status': 'healthy',
                        'test_prediction': str(prediction[0]) if len(prediction) > 0 else 'none'
                    }
                else:
                    health_status['models'][model_name] = {
                        'status': 'warning',
                        'message': 'Model does not have predict method'
                    }

            except Exception as e:
                health_status['models'][model_name] = {
                    'status': 'error',
                    'error': str(e)
                }

        return health_status

# Global ML service instance
ml_service = MLInferenceService()

async def get_ml_service():
    """Dependency injection for FastAPI"""
    return ml_service