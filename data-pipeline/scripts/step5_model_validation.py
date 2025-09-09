#!/usr/bin/env python3
"""
Step 5: Validate trained models và create predictions
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
import numpy as np
import joblib
from utils.database import DatabaseManager
from loguru import logger

def load_trained_models():
    """Load all trained models"""
    logger.info("📥 Loading trained models...")
    
    models_dir = Path('../data/models')
    models = {}
    
    # Load each model type
    model_files = {
        'price_prediction': 'price_prediction_randomforest.pkl',
        'category_classification': 'category_classification_randomforest.pkl',
        'sales_forecasting': 'sales_forecasting_rf.pkl'
    }
    
    for model_name, filename in model_files.items():
        model_path = models_dir / filename
        
        if model_path.exists():
            try:
                model = joblib.load(model_path)
                models[model_name] = model
                logger.info(f"   ✅ Loaded {model_name}")
            except Exception as e:
                logger.warning(f"   ⚠️ Could not load {model_name}: {e}")
        else:
            logger.warning(f"   ⚠️ Model file not found: {filename}")
    
    # Load category encoder if exists
    encoder_path = models_dir / 'sales_category_encoder.pkl'
    if encoder_path.exists():
        models['category_encoder'] = joblib.load(encoder_path)
        logger.info("   ✅ Loaded category encoder")
    
    return models

def test_price_predictions(model, test_data):
    """Test price prediction model"""
    logger.info("💰 Testing Price Prediction Model...")
    
    try:
        # Prepare test features
        feature_cols = [col for col in test_data.columns 
                       if col not in ['price', 'log_price', 'product_id', 'product_name']]
        
        X_test = test_data[feature_cols].select_dtypes(include=[np.number])
        X_test = X_test.fillna(X_test.median())
        
        # Make predictions
        predictions = model.predict(X_test)
        
        # Create results
        results = pd.DataFrame({
            'product_id': test_data.get('product_id', range(len(predictions))),
            'actual_price': test_data.get('price', np.nan),
            'predicted_price': predictions,
            'prediction_error': test_data.get('price', np.nan) - predictions if 'price' in test_data.columns else np.nan
        })
        
        # Calculate metrics if we have actual prices
        if 'price' in test_data.columns:
            mae = np.abs(results['prediction_error']).mean()
            mape = (np.abs(results['prediction_error']) / results['actual_price'] * 100).mean()
            
            logger.info(f"   📊 Mean Absolute Error: ${mae:.2f}")
            logger.info(f"   📊 Mean Absolute Percentage Error: {mape:.1f}%")
        
        logger.info(f"   ✅ Generated {len(predictions)} price predictions")
        return results
        
    except Exception as e:
        logger.error(f"   ❌ Price prediction test failed: {e}")
        return pd.DataFrame()

def test_sales_forecasting(model, encoder, historical_data):
    """Test sales forecasting model"""
    logger.info("📈 Testing Sales Forecasting Model...")
    
    try:
        # Create future dates (next 30 days)
        last_date = pd.to_datetime(historical_data['date']).max()
        future_dates = pd.date_range(
            start=last_date + pd.Timedelta(days=1), 
            periods=30, 
            freq='D'
        )
        
        # Get unique categories
        categories = historical_data['category'].unique()
        
        forecasts = []
        
        for date in future_dates:
            for category in categories:
                # Get recent data for this category
                cat_data = historical_data[
                    historical_data['category'] == category
                ].sort_values('date').tail(30)
                
                if len(cat_data) > 0:
                    # Create features
                    features = {
                        'is_weekend': int(date.weekday() >= 5),
                        'month': date.month,
                        'quarter': (date.month - 1) // 3 + 1,
                        'day_of_year': date.timetuple().tm_yday,
                        'sales_lag_7': cat_data['daily_sales'].iloc[-7] if len(cat_data) >= 7 else cat_data['daily_sales'].mean(),
                        'sales_lag_30': cat_data['daily_sales'].iloc[-30] if len(cat_data) >= 30 else cat_data['daily_sales'].mean(),
                        'sales_ma_7': cat_data['daily_sales'].tail(7).mean(),
                        'sales_ma_30': cat_data['daily_sales'].mean(),
                        'category_encoded': encoder.transform([category])[0]
                    }
                    
                    # Make prediction
                    X_pred = np.array(list(features.values())).reshape(1, -1)
                    predicted_sales = model.predict(X_pred)[0]
                    
                    forecasts.append({
                        'date': date,
                        'category': category,
                        'predicted_sales': max(0, predicted_sales),  # No negative sales
                        'is_weekend': features['is_weekend'],
                        'month': features['month']
                    })
        
        forecast_df = pd.DataFrame(forecasts)
        
        logger.info(f"   ✅ Generated {len(forecasts)} sales forecasts")
        logger.info(f"   📊 Average daily forecast: {forecast_df['predicted_sales'].mean():.2f}")
        
        return forecast_df
        
    except Exception as e:
        logger.error(f"   ❌ Sales forecasting test failed: {e}")
        return pd.DataFrame()

def create_business_insights(predictions_data):
    """Create business insights từ predictions"""
    logger.info("💡 Creating Business Insights...")
    
    insights = {}
    
    try:
        # Price insights
        if 'price_predictions' in predictions_data and not predictions_data['price_predictions'].empty:
            price_df = predictions_data['price_predictions']
            
            insights['price_insights'] = {
                'total_products_analyzed': len(price_df),
                'avg_predicted_price': price_df['predicted_price'].mean(),
                'price_range': {
                    'min': price_df['predicted_price'].min(),
                    'max': price_df['predicted_price'].max()
                }
            }
            
            if 'prediction_error' in price_df.columns:
                insights['price_insights']['avg_prediction_error'] = np.abs(price_df['prediction_error']).mean()
        
        # Sales insights
        if 'sales_forecasts' in predictions_data and not predictions_data['sales_forecasts'].empty:
            sales_df = predictions_data['sales_forecasts']
            
            # Weekly patterns
            weekly_pattern = sales_df.groupby('is_weekend')['predicted_sales'].mean()
            
            # Monthly patterns
            monthly_pattern = sales_df.groupby('month')['predicted_sales'].mean()
            
            # Category performance
            category_performance = sales_df.groupby('category')['predicted_sales'].agg(['mean', 'sum']).round(2)
            
            insights['sales_insights'] = {
                'total_forecast_days': len(sales_df['date'].unique()),
                'total_categories': len(sales_df['category'].unique()),
                'avg_daily_sales': sales_df['predicted_sales'].mean(),
                'total_predicted_revenue': sales_df['predicted_sales'].sum(),
                'weekend_vs_weekday': {
                    'weekday_avg': weekly_pattern.get(0, 0),
                    'weekend_avg': weekly_pattern.get(1, 0)
                },
                'top_performing_categories': category_performance.nlargest(3, 'sum').to_dict(),
                'seasonal_trends': monthly_pattern.to_dict()
            }
        
        logger.info(f"   ✅ Created insights for {len(insights)} model types")
        return insights
        
    except Exception as e:
        logger.error(f"   ❌ Business insights creation failed: {e}")
        return {}

def save_predictions_and_insights(predictions_data, insights):
    """Save predictions và insights to database"""
    logger.info("💾 Saving predictions and insights...")
    
    db_manager = DatabaseManager()
    
    try:
        # Save predictions
        for pred_type, pred_data in predictions_data.items():
            if not pred_data.empty:
                table_name = f'predictions_{pred_type}'
                db_manager.save_to_postgres(pred_data, table_name)
                logger.info(f"   ✅ Saved {pred_type}: {len(pred_data)} records")
        
        # Save insights as JSON records
        if insights:
            insights_records = []
            for insight_type, insight_data in insights.items():
                insights_records.append({
                    'insight_type': insight_type,
                    'insight_data': str(insight_data),  # Store as string for simplicity
                    'created_at': pd.Timestamp.now()
                })
            
            insights_df = pd.DataFrame(insights_records)
            db_manager.save_to_postgres(insights_df, 'business_insights')
            logger.info(f"   ✅ Saved insights: {len(insights_records)} records")
        
    except Exception as e:
        logger.error(f"   ❌ Error saving predictions: {e}")

def main_validation_pipeline():
    """Main model validation pipeline"""
    logger.info("🚀 Starting Model Validation Pipeline...")
    
    # Load models
    models = load_trained_models()
    
    if not models:
        logger.error("❌ No trained models found. Run step4_ml_modeling.py first")
        return
    
    # Load test data
    db_manager = DatabaseManager()
    predictions_data = {}
    
    # Test price prediction
    if 'price_prediction' in models:
        try:
            test_data = db_manager.load_from_postgres("SELECT * FROM ml_ready_price_prediction LIMIT 100")
            if not test_data.empty:
                price_predictions = test_price_predictions(models['price_prediction'], test_data)
                predictions_data['price_predictions'] = price_predictions
        except Exception as e:
            logger.warning(f"Could not test price predictions: {e}")
    
    # Test sales forecasting
    if 'sales_forecasting' in models and 'category_encoder' in models:
        try:
            sales_data = db_manager.load_from_postgres("SELECT * FROM synthetic_sales_data")
            if not sales_data.empty:
                sales_forecasts = test_sales_forecasting(
                    models['sales_forecasting'], 
                    models['category_encoder'], 
                    sales_data
                )
                predictions_data['sales_forecasts'] = sales_forecasts
        except Exception as e:
            logger.warning(f"Could not test sales forecasting: {e}")
    
    # Create business insights
    insights = create_business_insights(predictions_data)
    
    # Save everything
    save_predictions_and_insights(predictions_data, insights)
    
    # Summary
    logger.info("\n🔮 VALIDATION SUMMARY:")
    logger.info(f"   Models tested: {len(models)}")
    logger.info(f"   Prediction types: {len(predictions_data)}")
    logger.info(f"   Business insights: {len(insights)}")
    
    # Display key insights
    if insights:
        logger.info("\n💡 KEY BUSINESS INSIGHTS:")
        
        if 'price_insights' in insights:
            price_insights = insights['price_insights']
            logger.info(f"   💰 Price Analysis:")
            logger.info(f"     - Products analyzed: {price_insights.get('total_products_analyzed', 0)}")
            logger.info(f"     - Avg predicted price: ${price_insights.get('avg_predicted_price', 0):.2f}")
        
        if 'sales_insights' in insights:
            sales_insights = insights['sales_insights']
            logger.info(f"   📈 Sales Forecast:")
            logger.info(f"     - Forecast period: {sales_insights.get('total_forecast_days', 0)} days")
            logger.info(f"     - Avg daily sales: {sales_insights.get('avg_daily_sales', 0):.2f}")
            logger.info(f"     - Total predicted revenue: {sales_insights.get('total_predicted_revenue', 0):.2f}")
    
    logger.info("\n🎯 Next Steps:")
    logger.info("1. Review predictions in database tables")
    logger.info("2. Analyze business insights")
    logger.info("3. Set up automated retraining pipeline")
    logger.info("4. Integrate with backend API")
    
    return predictions_data, insights

if __name__ == "__main__":
    main_validation_pipeline()