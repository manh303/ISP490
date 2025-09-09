from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'dss-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'weekly_ml_training',
    default_args=default_args,
    description='Weekly ML model training and deployment',
    schedule_interval='0 2 * * 0',  # Weekly on Sunday at 2 AM
    catchup=False,
    tags=['ml', 'training', 'weekly']
)

def feature_engineering(**context):
    """Run feature engineering on warehouse data"""
    from processors.feature_engineer import FeatureEngineer
    from utils.database import DatabaseManager
    
    db_manager = DatabaseManager()
    fe = FeatureEngineer()
    
    # Load latest warehouse data
    df = db_manager.load_from_postgres("""
        SELECT * FROM dw_ecommerce_data 
        WHERE processed_date >= CURRENT_DATE - INTERVAL '30 days'
    """)
    
    if df.empty:
        raise Exception("No recent data found for feature engineering")
    
    # Create features
    df_features = fe.create_ecommerce_features(df)
    
    # Save featured data
    db_manager.save_to_postgres(df_features, 'ml_featured_data')
    
    print(f"Generated features for {len(df_features)} records")
    return f"Feature engineering completed: {len(df_features)} records"

def train_models(**context):
    """Train ML models"""
    import joblib
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split
    from utils.database import DatabaseManager
    
    db_manager = DatabaseManager()
    
    # Load featured data
    df = db_manager.load_from_postgres("SELECT * FROM ml_featured_data")
    
    if len(df) < 1000:
        raise Exception("Insufficient data for training")
    
    # Prepare features for price prediction (example)
    numeric_features = df.select_dtypes(include=['number']).columns
    feature_cols = [col for col in numeric_features if col not in ['price', 'id']]
    
    if 'price' in df.columns and len(feature_cols) > 0:
        X = df[feature_cols].fillna(0)
        y = df['price'].fillna(df['price'].median())
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Save model
        model_path = f'/opt/airflow/models/price_model_{datetime.now().strftime("%Y%m%d")}.pkl'
        os.makedirs('/opt/airflow/models', exist_ok=True)
        joblib.dump(model, model_path)
        
        # Evaluate
        train_score = model.score(X_train, y_train)
        test_score = model.score(X_test, y_test)
        
        print(f"Model trained - Train R²: {train_score:.3f}, Test R²: {test_score:.3f}")
        
        # Save model metadata
        model_info = pd.DataFrame([{
            'model_name': 'price_prediction_rf',
            'train_score': train_score,
            'test_score': test_score,
            'features_count': len(feature_cols),
            'training_date': datetime.now(),
            'model_path': model_path
        }])
        
        db_manager.save_to_postgres(model_info, 'ml_model_registry')
        
        return f"Model trained successfully: R² = {test_score:.3f}"
    
    raise Exception("Unable to train model - missing required columns")

# Task definitions
feature_engineering_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering,
    dag=dag
)

model_training_task = PythonOperator(
    task_id='train_models',
    python_callable=train_models,
    dag=dag
)

# Task dependencies
feature_engineering_task >> model_training_task