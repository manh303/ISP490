import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, cross_val_score
import joblib
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path

class SalesPredictor:
    def __init__(self, model_path: str = None):
        self.models = {
            'rf': RandomForestRegressor(n_estimators=100, random_state=42),
            'gb': GradientBoostingRegressor(n_estimators=100, random_state=42),
            'lr': LinearRegression()
        }
        self.best_model = None
        self.best_model_name = None
        self.feature_importance = None
        self.model_path = Path(model_path) if model_path else Path('../data/models')
        self.model_path.mkdir(parents=True, exist_ok=True)
    
    def prepare_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data cho sales forecasting
        """
        logger.info("Preparing sales data...")
        
        # Tạo synthetic sales data từ product data
        if 'price' in df.columns and 'num_reviews' in df.columns:
            # Estimate sales từ price và popularity
            df['estimated_sales'] = (
                df['num_reviews'] * 0.1 +  # Assume 10% của reviews là actual sales
                (1000 / df['price']) * np.random.uniform(0.5, 2.0, len(df))  # Price elasticity
            ).round(0)
            
            # Add seasonality patterns
            if 'month' in df.columns:
                seasonal_multiplier = df['month'].map({
                    1: 0.8, 2: 0.9, 3: 1.1, 4: 1.0, 5: 1.0, 6: 1.2,
                    7: 1.3, 8: 1.1, 9: 1.0, 10: 1.1, 11: 1.4, 12: 1.6  # Holiday season
                })
                df['estimated_sales'] = df['estimated_sales'] * seasonal_multiplier
        
        # Aggregate by time period cho forecasting
        if 'scraped_date' in df.columns:
            df['date'] = pd.to_datetime(df['scraped_date'])
            daily_sales = df.groupby(['date', 'category']).agg({
                'estimated_sales': 'sum',
                'price': 'mean',
                'rating': 'mean',
                'num_reviews': 'sum'
            }).reset_index()
            
            return daily_sales
        
        return df
    
    def create_time_series_features(self, df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
        """
        Tạo time series features cho forecasting
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
        
        # Time-based features
        df['year'] = df[date_col].dt.year
        df['month'] = df[date_col].dt.month
        df['day'] = df[date_col].dt.day
        df['weekday'] = df[date_col].dt.weekday
        df['is_weekend'] = df['weekday'].isin([5, 6]).astype(int)
        
        # Lag features (previous sales)
        for lag in [1, 7, 30]:  # 1 day, 1 week, 1 month ago
            df[f'sales_lag_{lag}'] = df.groupby('category')['estimated_sales'].shift(lag)
        
        # Rolling averages
        for window in [7, 30]:  # 1 week, 1 month
            df[f'sales_rolling_{window}'] = df.groupby('category')['estimated_sales'].rolling(window).mean().reset_index(0, drop=True)
        
        # Remove rows với missing lag features
        df = df.dropna()
        
        return df
    
    def train_models(self, df: pd.DataFrame, target_col: str = 'estimated_sales'):
        """
        Train multiple models và chọn best performer
        """
        logger.info("Training sales forecasting models...")
        
        # Prepare features
        feature_columns = [col for col in df.columns if col not in [target_col, 'date', 'category']]
        X = df[feature_columns]
        y = df[target_col]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model_scores = {}
        
        # Train và evaluate từng model
        for name, model in self.models.items():
            logger.info(f"Training {name}...")
            
            # Train model
            model.fit(X_train, y_train)
            
            # Predictions
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Metrics
            train_mae = mean_absolute_error(y_train, y_pred_train)
            test_mae = mean_absolute_error(y_test, y_pred_test)
            test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
            test_r2 = r2_score(y_test, y_pred_test)
            
            # Cross validation
            cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='neg_mean_absolute_error')
            cv_mae = -cv_scores.mean()
            
            model_scores[name] = {
                'train_mae': train_mae,
                'test_mae': test_mae,
                'test_rmse': test_rmse,
                'test_r2': test_r2,
                'cv_mae': cv_mae
            }
            
            logger.info(f"{name} - Test MAE: {test_mae:.2f}, Test R2: {test_r2:.3f}")
        
        # Select best model based on cross-validation MAE
        best_name = min(model_scores.keys(), key=lambda x: model_scores[x]['cv_mae'])
        self.best_model = self.models[best_name]
        self.best_model_name = best_name
        
        # Get feature importance
        if hasattr(self.best_model, 'feature_importances_'):
            self.feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'importance': self.best_model.feature_importances_
            }).sort_values('importance', ascending=False)
        
        logger.success(f"Best model: {best_name} with CV MAE: {model_scores[best_name]['cv_mae']:.2f}")
        return model_scores
    
    def predict_sales(self, df: pd.DataFrame, days_ahead: int = 30):
        """
        Predict sales cho future dates
        """
        if self.best_model is None:
            raise ValueError("Model chưa được train. Run train_models() trước.")
        
        logger.info(f"Predicting sales for {days_ahead} days ahead...")
        
        # Generate future dates
        last_date = df['date'].max()
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=days_ahead, freq='D')
        
        predictions = []
        categories = df['category'].unique()
        
        for category in categories:
            category_data = df[df['category'] == category].copy()
            
            for future_date in future_dates:
                # Create features cho future date
                future_features = self._create_future_features(category_data, future_date)
                
                # Predict
                prediction = self.best_model.predict([future_features])[0]
                
                predictions.append({
                    'date': future_date,
                    'category': category,
                    'predicted_sales': max(0, prediction)  # Ensure non-negative
                })
        
        predictions_df = pd.DataFrame(predictions)
        logger.info(f"Generated {len(predictions_df)} predictions")
        return predictions_df
    
    def _create_future_features(self, category_data, future_date):
        """
        Create features cho future prediction
        """
        # Basic time features
        features = {
            'year': future_date.year,
            'month': future_date.month,
            'day': future_date.day,
            'weekday': future_date.weekday(),
            'is_weekend': int(future_date.weekday() in [5, 6])
        }
        
        # Use recent data for lags và rolling averages
        recent_sales = category_data['estimated_sales'].tail(30).mean()
        features.update({
            'sales_lag_1': recent_sales,
            'sales_lag_7': recent_sales,
            'sales_lag_30': recent_sales,
            'sales_rolling_7': recent_sales,
            'sales_rolling_30': recent_sales,
            'price': category_data['price'].mean(),
            'rating': category_data['rating'].mean(),
            'num_reviews': category_data['num_reviews'].mean()
        })
        
        return list(features.values())
    
    def save_model(self, filename: str = None):
        """Save trained model"""
        if not filename:
            filename = f"sales_predictor_{self.best_model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        
        model_path = self.model_path / filename
        joblib.dump({
            'model': self.best_model,
            'model_name': self.best_model_name,
            'feature_importance': self.feature_importance
        }, model_path)
        
        logger.info(f"Model saved to: {model_path}")
        return model_path
    
    def load_model(self, model_path: str):
        """Load trained model"""
        loaded = joblib.load(model_path)
        self.best_model = loaded['model']
        self.best_model_name = loaded['model_name']
        self.feature_importance = loaded['feature_importance']
        
        logger.info(f"Model loaded: {self.best_model_name}")