#!/usr/bin/env python3
"""
Step 4: Train basic ML models với engineered features
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_absolute_error, r2_score, classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
import joblib
from utils.database import DatabaseManager
from loguru import logger
import warnings
warnings.filterwarnings('ignore')

def load_ml_datasets():
    """Load ML-ready datasets"""
    logger.info("📥 Loading ML-ready datasets...")
    
    db_manager = DatabaseManager()
    datasets = {}
    
    # Try to load each ML dataset
    dataset_types = ['price_prediction', 'rating_prediction', 'category_classification', 'analytics']
    
    for dataset_type in dataset_types:
        try:
            table_name = f'ml_ready_{dataset_type}'
            df = db_manager.load_from_postgres(f"SELECT * FROM {table_name}")
            
            if not df.empty:
                datasets[dataset_type] = df
                logger.info(f"   ✅ Loaded {dataset_type}: {len(df)} samples")
            else:
                logger.warning(f"   ⚠️ Empty dataset: {dataset_type}")
                
        except Exception as e:
            logger.warning(f"   ⚠️ Could not load {dataset_type}: {e}")
    
    return datasets

def train_price_prediction_model(df):
    """Train price prediction model"""
    logger.info("💰 Training Price Prediction Model...")
    
    try:
        # Prepare features và target
        feature_cols = [col for col in df.columns if col not in ['price', 'log_price']]
        X = df[feature_cols].select_dtypes(include=[np.number])  # Only numeric features
        y = df['price']
        
        # Handle missing values
        X = X.fillna(X.median())
        
        logger.info(f"   📊 Features: {len(X.columns)}, Samples: {len(X)}")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train models
        models = {
            'RandomForest': RandomForestRegressor(n_estimators=100, random_state=42),
            'LinearRegression': LinearRegression()
        }
        
        results = {}
        
        for name, model in models.items():
            logger.info(f"   🔄 Training {name}...")
            
            # Train
            model.fit(X_train, y_train)
            
            # Predict
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Evaluate
            train_mae = mean_absolute_error(y_train, y_pred_train)
            test_mae = mean_absolute_error(y_test, y_pred_test)
            train_r2 = r2_score(y_train, y_pred_train)
            test_r2 = r2_score(y_test, y_pred_test)
            
            results[name] = {
                'model': model,
                'train_mae': train_mae,
                'test_mae': test_mae,
                'train_r2': train_r2,
                'test_r2': test_r2,
                'features': list(X.columns)
            }
            
            logger.info(f"     📈 {name}: Test MAE={test_mae:.2f}, Test R²={test_r2:.3f}")
        
        # Select best model
        best_model_name = min(results.keys(), key=lambda x: results[x]['test_mae'])
        best_result = results[best_model_name]
        
        # Save best model
        model_path = Path('../data/models') / f'price_prediction_{best_model_name.lower()}.pkl'
        model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(best_result['model'], model_path)
        
        logger.success(f"   ✅ Best model: {best_model_name} (MAE: {best_result['test_mae']:.2f})")
        logger.info(f"   💾 Model saved: {model_path}")
        
        return best_result
        
    except Exception as e:
        logger.error(f"   ❌ Price prediction training failed: {e}")
        return None

def train_category_classification_model(df):
    """Train category classification model"""
    logger.info("📂 Training Category Classification Model...")
    
    try:
        # Prepare features và target
        feature_cols = [col for col in df.columns if col not in ['category', 'category_encoded']]
        X = df[feature_cols].select_dtypes(include=[np.number])
        
        # Handle target encoding
        if 'category_encoded' in df.columns:
            y = df['category_encoded']
        else:
            le = LabelEncoder()
            y = le.fit_transform(df['category'])
        
        # Handle missing values
        X = X.fillna(X.median())
        
        logger.info(f"   📊 Features: {len(X.columns)}, Classes: {len(np.unique(y))}, Samples: {len(X)}")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        
        # Train models
        models = {
            'RandomForest': RandomForestClassifier(n_estimators=100, random_state=42),
            'LogisticRegression': LogisticRegression(max_iter=1000, random_state=42)
        }
        
        results = {}
        
        for name, model in models.items():
            logger.info(f"   🔄 Training {name}...")
            
            # Train
            model.fit(X_train, y_train)
            
            # Predict
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Evaluate
            train_acc = accuracy_score(y_train, y_pred_train)
            test_acc = accuracy_score(y_test, y_pred_test)
            
            results[name] = {
                'model': model,
                'train_accuracy': train_acc,
                'test_accuracy': test_acc,
                'features': list(X.columns)
            }
            
            logger.info(f"     📈 {name}: Test Accuracy={test_acc:.3f}")
        
        # Select best model
        best_model_name = max(results.keys(), key=lambda x: results[x]['test_accuracy'])
        best_result = results[best_model_name]
        
        # Save best model
        model_path = Path('../data/models') / f'category_classification_{best_model_name.lower()}.pkl'
        joblib.dump(best_result['model'], model_path)
        
        logger.success(f"   ✅ Best model: {best_model_name} (Accuracy: {best_result['test_accuracy']:.3f})")
        logger.info(f"   💾 Model saved: {model_path}")
        
        return best_result
        
    except Exception as e:
        logger.error(f"   ❌ Category classification training failed: {e}")
        return None

def create_synthetic_sales_data(df_analytics):
    """Create synthetic sales data for forecasting"""
    logger.info("📈 Creating synthetic sales data...")
    
    try:
        # Generate daily sales data
        np.random.seed(42)
        
        # Create date range
        date_range = pd.date_range('2023-01-01', '2024-12-31', freq='D')
        
        sales_data = []
        
        # Get unique categories
        if 'category' in df_analytics.columns:
            categories = df_analytics['category'].dropna().unique()[:10]  # Limit to top 10
        else:
            categories = ['Electronics', 'Clothing', 'Books', 'Home']
        
        for date in date_range[:365]:  # One year of data
            for category in categories:
                # Base sales với seasonal patterns
                base_sales = 100
                
                # Seasonal effects
                if date.month in [11, 12]:  # Holiday season
                    seasonal_multiplier = 1.5
                elif date.month in [6, 7, 8]:  # Summer
                    seasonal_multiplier = 1.2
                else:
                    seasonal_multiplier = 1.0
                
                # Weekend effect
                if date.weekday() >= 5:
                    weekend_multiplier = 0.8
                else:
                    weekend_multiplier = 1.0
                
                # Random variation
                random_factor = np.random.normal(1.0, 0.2)
                
                # Calculate sales
                daily_sales = base_sales * seasonal_multiplier * weekend_multiplier * random_factor
                daily_sales = max(0, daily_sales)  # No negative sales
                
                sales_data.append({
                    'date': date,
                    'category': category,
                    'daily_sales': round(daily_sales, 2),
                    'is_weekend': int(date.weekday() >= 5),
                    'month': date.month,
                    'quarter': (date.month - 1) // 3 + 1,
                    'day_of_year': date.timetuple().tm_yday
                })
        
        sales_df = pd.DataFrame(sales_data)
        
        # Add lag features
        for category in categories:
            cat_mask = sales_df['category'] == category
            cat_data = sales_df[cat_mask].copy().sort_values('date')
            
            # 7-day and 30-day lags
            sales_df.loc[cat_mask, 'sales_lag_7'] = cat_data['daily_sales'].shift(7)
            sales_df.loc[cat_mask, 'sales_lag_30'] = cat_data['daily_sales'].shift(30)
            
            # Rolling averages
            sales_df.loc[cat_mask, 'sales_ma_7'] = cat_data['daily_sales'].rolling(7).mean()
            sales_df.loc[cat_mask, 'sales_ma_30'] = cat_data['daily_sales'].rolling(30).mean()
        
        # Remove rows với missing lag features
        sales_df = sales_df.dropna()
        
        logger.info(f"   ✅ Created {len(sales_df)} sales records across {len(categories)} categories")
        
        return sales_df
        
    except Exception as e:
        logger.error(f"   ❌ Synthetic sales data creation failed: {e}")
        return pd.DataFrame()

def train_sales_forecasting_model(sales_df):
    """Train sales forecasting model"""
    logger.info("📈 Training Sales Forecasting Model...")
    
    try:
        # Prepare features
        feature_cols = ['is_weekend', 'month', 'quarter', 'day_of_year', 
                       'sales_lag_7', 'sales_lag_30', 'sales_ma_7', 'sales_ma_30']
        
        # Encode category
        le = LabelEncoder()
        sales_df['category_encoded'] = le.fit_transform(sales_df['category'])
        feature_cols.append('category_encoded')
        
        X = sales_df[feature_cols]
        y = sales_df['daily_sales']
        
        logger.info(f"   📊 Features: {len(X.columns)}, Samples: {len(X)}")
        
        # Split data (time series split)
        split_date = sales_df['date'].quantile(0.8)
        train_mask = sales_df['date'] <= split_date
        
        X_train, X_test = X[train_mask], X[~train_mask]
        y_train, y_test = y[train_mask], y[~train_mask]
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        # Evaluate
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info(f"   📈 Performance: Test MAE={test_mae:.2f}, Test R²={test_r2:.3f}")
        logger.info(f"   🎯 Top features: {', '.join(feature_importance.head(3)['feature'].tolist())}")
        
        # Save model và label encoder
        model_path = Path('../data/models') / 'sales_forecasting_rf.pkl'
        encoder_path = Path('../data/models') / 'sales_category_encoder.pkl'
        
        joblib.dump(model, model_path)
        joblib.dump(le, encoder_path)
        
        logger.success(f"   ✅ Sales forecasting model trained (MAE: {test_mae:.2f})")
        logger.info(f"   💾 Model saved: {model_path}")
        
        return {
            'model': model,
            'encoder': le,
            'test_mae': test_mae,
            'test_r2': test_r2,
            'feature_importance': feature_importance,
            'features': feature_cols
        }
        
    except Exception as e:
        logger.error(f"   ❌ Sales forecasting training failed: {e}")
        return None

def save_model_results(results):
    """Save model results to database"""
    logger.info("💾 Saving model results...")
    
    db_manager = DatabaseManager()
    
    # Compile model performance
    model_performance = []
    
    for model_type, result in results.items():
        if result is not None:
            performance_record = {
                'model_type': model_type,
                'model_name': type(result['model']).__name__,
                'created_at': pd.Timestamp.now(),
                'features_count': len(result.get('features', [])),
                'training_completed': True
            }
            
            # Add specific metrics based on model type
            if 'test_mae' in result:
                performance_record.update({
                    'test_mae': result['test_mae'],
                    'test_r2': result.get('test_r2', None)
                })
            
            if 'test_accuracy' in result:
                performance_record['test_accuracy'] = result['test_accuracy']
            
            model_performance.append(performance_record)
    
    if model_performance:
        performance_df = pd.DataFrame(model_performance)
        db_manager.save_to_postgres(performance_df, 'model_performance')
        logger.success("   ✅ Model performance saved to database")

def main_ml_pipeline():
    """Main ML modeling pipeline"""
    logger.info("🚀 Starting ML Modeling Pipeline...")
    
    # Load datasets
    datasets = load_ml_datasets()
    
    if not datasets:
        logger.error("❌ No ML datasets found. Run step3_feature_engineering.py first")
        return
    
    results = {}
    
    # Train price prediction model
    if 'price_prediction' in datasets:
        price_result = train_price_prediction_model(datasets['price_prediction'])
        if price_result:
            results['price_prediction'] = price_result
    
    # Train category classification model
    if 'category_classification' in datasets:
        category_result = train_category_classification_model(datasets['category_classification'])
        if category_result:
            results['category_classification'] = category_result
    
    # Train sales forecasting model
    if 'analytics' in datasets:
        sales_data = create_synthetic_sales_data(datasets['analytics'])
        if not sales_data.empty:
            sales_result = train_sales_forecasting_model(sales_data)
            if sales_result:
                results['sales_forecasting'] = sales_result
                
                # Save sales data for future use
                db_manager = DatabaseManager()
                db_manager.save_to_postgres(sales_data, 'synthetic_sales_data')
    
    # Save results
    if results:
        save_model_results(results)
        
        # Summary
        logger.info("\n🤖 ML MODELING SUMMARY:")
        logger.info(f"   Models trained: {len(results)}")
        
        for model_type, result in results.items():
            logger.info(f"   {model_type}:")
            if 'test_mae' in result:
                logger.info(f"     - MAE: {result['test_mae']:.3f}")
            if 'test_r2' in result:
                logger.info(f"     - R²: {result['test_r2']:.3f}")
            if 'test_accuracy' in result:
                logger.info(f"     - Accuracy: {result['test_accuracy']:.3f}")
        
        logger.info("\n🎯 Next Steps:")
        logger.info("1. Review model performance in database")
        logger.info("2. Run step5_model_validation.py")
        logger.info("3. Test predictions with new data")
        
        return results
    else:
        logger.error("❌ No models trained successfully")
        return {}

if __name__ == "__main__":
    main_ml_pipeline()