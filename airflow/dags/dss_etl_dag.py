from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import sys
import os

# Add project path
sys.path.append('/opt/airflow/dags')
sys.path.append('/app/src')
sys.path.append('/app/src/utils')

# Default arguments
default_args = {
    'owner': 'dss-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'dss_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for DSS ecommerce project',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    max_active_runs=1,
    tags=['etl', 'dss', 'ecommerce']
)

def check_data_sources(**context):
    """Check if data sources are available"""
    from utils.database import DatabaseManager
    import os
    from pathlib import Path
    import shutil
    
    db_manager = DatabaseManager()
    
    # Define paths
    config_dir = Path('/opt/airflow/config')
    kaggle_path = config_dir / 'kaggle.json'
    
    # Create config directory if it doesn't exist
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy kaggle.json to airflow user's home directory
    home_kaggle_dir = Path.home() / '.kaggle'
    home_kaggle_dir.mkdir(parents=True, exist_ok=True)
    
    if kaggle_path.exists():
        # Copy to user's .kaggle directory
        shutil.copy2(kaggle_path, home_kaggle_dir / 'kaggle.json')
        # Set proper permissions
        os.chmod(home_kaggle_dir / 'kaggle.json', 0o600)
        os.environ['KAGGLE_CONFIG_DIR'] = str(home_kaggle_dir)
    else:
        raise Exception(f"Kaggle credentials not found at {kaggle_path}")
    
    # Check PostgreSQL connection
    try:
        test_query = "SELECT 1"
        result = db_manager.load_from_postgres(test_query)
        print("✅ PostgreSQL connection successful")
    except Exception as e:
        raise Exception(f"PostgreSQL connection failed: {e}")
    
    # Check Kaggle API with specific credentials
    try:
        import kaggle
        # Use max-size instead of deprecated size parameter
        datasets = kaggle.api.dataset_list(max_size=1000)
        if len(datasets) > 0:
            print("✅ Kaggle API connection successful")
        else:
            raise Exception("No datasets returned from Kaggle API")
    except Exception as e:
        raise Exception(f"Kaggle API failed: {e}")
    
    return "Data sources check completed"

def extract_kaggle_data(**context):
    """Extract data from Kaggle"""
    from collectors.kaggle_collector import KaggleCollector
    from utils.database import DatabaseManager
    
    collector = KaggleCollector()
    db_manager = DatabaseManager()
    
    # Priority datasets
    datasets = [
        'carrie1/ecommerce-data',
        'olistbr/brazilian-ecommerce'
    ]
    
    download_summary = []
    
    for dataset_name in datasets:
        try:
            print(f"Downloading: {dataset_name}")
            download_path = collector.download_dataset(dataset_name)
            
            if download_path:
                files = collector.list_files_in_dataset(download_path)
                csv_files = [f for f in files if f.suffix == '.csv']
                
                for csv_file in csv_files:
                    df_sample = collector.load_csv_file(csv_file, nrows=1000)
                    if df_sample is not None:
                        download_summary.append({
                            'dataset_name': dataset_name,
                            'file_name': csv_file.name,
                            'file_path': str(csv_file),
                            'shape_rows': len(df_sample),
                            'shape_cols': len(df_sample.columns),
                            'columns': list(df_sample.columns),
                            'download_date': datetime.now()
                        })
        except Exception as e:
            print(f"Warning: Could not download {dataset_name}: {e}")
    
    # Save download summary
    if download_summary:
        summary_df = pd.DataFrame(download_summary)
        db_manager.save_to_postgres(summary_df, 'airflow_data_downloads')
        print(f"Downloaded {len(download_summary)} datasets")
        
        # Store in XCom for next task
        context['task_instance'].xcom_push(key='download_count', value=len(download_summary))
        return f"Successfully downloaded {len(download_summary)} datasets"
    
    raise Exception("No datasets downloaded")

def transform_clean_data(**context):
    """Transform and clean the extracted data"""
    from processors.advanced_data_cleaner import AdvancedDataCleaner
    from utils.database import DatabaseManager
    import pandas as pd
    
    db_manager = DatabaseManager()
    cleaner = AdvancedDataCleaner()
    
    # Get download summary
    downloads_df = db_manager.load_from_postgres(
        "SELECT * FROM airflow_data_downloads WHERE download_date::date = CURRENT_DATE"
    )
    
    if downloads_df.empty:
        raise Exception("No data downloads found for today")
    
    cleaned_datasets = []
    
    for _, row in downloads_df.iterrows():
        file_path = row['file_path']
        
        # Convert Windows paths to container paths
        docker_path = file_path.replace('C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data', '/app/data')
        docker_path = docker_path.replace('\\', '/')
        
        try:
            if os.path.exists(docker_path):
                # Load and clean data
                df = pd.read_csv(docker_path)
                df_clean = cleaner.comprehensive_clean(df)
                
                # Add metadata
                df_clean['dataset_source'] = row['dataset_name']
                df_clean['file_source'] = row['file_name']
                df_clean['processed_date'] = datetime.now()
                
                # Save to staging table
                table_name = f"staging_{row['file_name'].replace('.csv', '').replace('-', '_')}"
                db_manager.save_to_postgres(df_clean, table_name)
                
                cleaned_datasets.append({
                    'file_name': row['file_name'],
                    'original_rows': len(df),
                    'cleaned_rows': len(df_clean),
                    'table_name': table_name
                })
                
                print(f"Cleaned {row['file_name']}: {len(df)} → {len(df_clean)} rows")
        
        except Exception as e:
            print(f"Error processing {row['file_name']}: {e}")
    
    # Save cleaning summary
    if cleaned_datasets:
        summary_df = pd.DataFrame(cleaned_datasets)
        summary_df['processing_date'] = datetime.now()
        db_manager.save_to_postgres(summary_df, 'airflow_cleaning_summary')
        
        context['task_instance'].xcom_push(key='cleaned_count', value=len(cleaned_datasets))
        return f"Successfully cleaned {len(cleaned_datasets)} datasets"
    
    raise Exception("No datasets were cleaned")

def load_to_warehouse(**context):
    """Load cleaned data to data warehouse tables"""
    from utils.database import DatabaseManager
    import gc  # For garbage collection
    
    db_manager = DatabaseManager()
    
    # Get today's cleaning summary
    cleaning_summary = db_manager.load_from_postgres(
        "SELECT * FROM airflow_cleaning_summary WHERE processing_date::date = CURRENT_DATE"
    )
    
    processed_count = 0
    batch_size = 100000  # Process 100k rows at a time
    
    for _, row in cleaning_summary.iterrows():
        table_name = row['table_name']
        
        try:
            # Get total count first
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            total_rows = db_manager.load_from_postgres(count_query)['count'].iloc[0]
            
            # Process in batches
            for offset in range(0, total_rows, batch_size):
                # Load data in chunks
                query = f"""
                    SELECT * FROM {table_name} 
                    LIMIT {batch_size} OFFSET {offset}
                """
                df_chunk = db_manager.load_from_postgres(query)
                
                if not df_chunk.empty:
                    # Add standardization columns
                    df_chunk['data_type'] = 'general'
                    if 'customer' in table_name:
                        df_chunk['data_type'] = 'customer'
                    elif 'product' in table_name:
                        df_chunk['data_type'] = 'product'
                    elif 'order' in table_name:
                        df_chunk['data_type'] = 'transaction'
                    
                    # Insert directly to warehouse
                    db_manager.save_to_postgres(
                        df_chunk, 
                        'dw_ecommerce_data',
                        if_exists='append'
                    )
                    
                    processed_count += len(df_chunk)
                    print(f"Processed {processed_count}/{total_rows} rows from {table_name}")
                    
                    # Clear memory
                    del df_chunk
                    gc.collect()
                
        except Exception as e:
            print(f"Error processing {table_name}: {e}")
            continue
    
    if processed_count > 0:
        print(f"Loaded {processed_count} records to data warehouse")
        return f"Successfully loaded {processed_count} records"
    
    raise Exception("No data to load to warehouse")

def run_data_quality_checks(**context):
    """Run data quality checks on the loaded data"""
    from utils.database import DatabaseManager
    
    db_manager = DatabaseManager()
    
    quality_checks = []
    
    # Check 1: Row count
    row_count = db_manager.load_from_postgres(
        "SELECT COUNT(*) as count FROM dw_ecommerce_data WHERE processed_date::date = CURRENT_DATE"
    )['count'].iloc[0]
    
    quality_checks.append({
        'check_name': 'row_count',
        'result': row_count,
        'status': 'pass' if row_count > 0 else 'fail',
        'check_date': datetime.now()
    })
    
    # Check 2: Null percentage
    null_check = db_manager.load_from_postgres("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(*) - COUNT(dataset_source) as null_count
        FROM dw_ecommerce_data 
        WHERE processed_date::date = CURRENT_DATE
    """)
    
    null_percentage = (null_check['null_count'].iloc[0] / null_check['total_rows'].iloc[0]) * 100
    
    quality_checks.append({
        'check_name': 'null_percentage',
        'result': null_percentage,
        'status': 'pass' if null_percentage < 50 else 'fail',
        'check_date': datetime.now()
    })
    
    # Check 3: Data freshness
    freshness_check = db_manager.load_from_postgres("""
        SELECT MAX(processed_date) as latest_date
        FROM dw_ecommerce_data
    """)
    
    latest_date = pd.to_datetime(freshness_check['latest_date'].iloc[0])
    hours_old = (datetime.now() - latest_date).total_seconds() / 3600
    
    quality_checks.append({
        'check_name': 'data_freshness_hours',
        'result': hours_old,
        'status': 'pass' if hours_old < 24 else 'fail',
        'check_date': datetime.now()
    })
    
    # Save quality check results
    quality_df = pd.DataFrame(quality_checks)
    db_manager.save_to_postgres(quality_df, 'airflow_data_quality_checks')
    
    # Check if any tests failed
    failed_checks = quality_df[quality_df['status'] == 'fail']
    if not failed_checks.empty:
        raise Exception(f"Data quality checks failed: {failed_checks['check_name'].tolist()}")
    
    print("All data quality checks passed")
    return "Data quality checks completed successfully"

# Task definitions
check_sources = PythonOperator(
    task_id='check_data_sources',
    python_callable=check_data_sources,
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_kaggle_data',
    python_callable=extract_kaggle_data,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_clean_data',
    python_callable=transform_clean_data,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

quality_check = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

# Create cleanup task
cleanup_staging = PostgresOperator(
    task_id='cleanup_staging_tables',
    postgres_conn_id='postgres_default',
    sql="""
    -- Keep only last 7 days of staging data
    DELETE FROM airflow_data_downloads 
    WHERE download_date < CURRENT_DATE - INTERVAL '7 days';
    
    DELETE FROM airflow_cleaning_summary 
    WHERE processing_date < CURRENT_DATE - INTERVAL '7 days';
    """,
    dag=dag
)

# Task dependencies
check_sources >> extract_data >> transform_data >> load_data >> quality_check >> cleanup_staging