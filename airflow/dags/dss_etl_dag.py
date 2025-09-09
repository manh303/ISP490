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
    
    db_manager = DatabaseManager()
    
    # Check PostgreSQL connection
    try:
        test_query = "SELECT 1"
        result = db_manager.load_from_postgres(test_query)
        print("✅ PostgreSQL connection successful")
    except Exception as e:
        raise Exception(f"PostgreSQL connection failed: {e}")
    
    # Check Kaggle API
    try:
        import kaggle
        datasets = kaggle.api.dataset_list(page=1, size=1)
        print("✅ Kaggle API connection successful")
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
    
    db_manager = DatabaseManager()
    
    # Get today's cleaning summary
    cleaning_summary = db_manager.load_from_postgres(
        "SELECT * FROM airflow_cleaning_summary WHERE processing_date::date = CURRENT_DATE"
    )
    
    consolidated_data = []
    
    for _, row in cleaning_summary.iterrows():
        table_name = row['table_name']
        
        # Load cleaned data
        df = db_manager.load_from_postgres(f"SELECT * FROM {table_name}")
        
        if not df.empty:
            # Standardize columns based on data type
            if 'customer' in table_name:
                df['data_type'] = 'customer'
            elif 'product' in table_name:
                df['data_type'] = 'product'
            elif 'order' in table_name:
                df['data_type'] = 'transaction'
            else:
                df['data_type'] = 'general'
            
            consolidated_data.append(df)
    
    if consolidated_data:
        # Combine all data
        final_df = pd.concat(consolidated_data, ignore_index=True, sort=False)
        
        # Load to final warehouse table
        db_manager.save_to_postgres(final_df, 'dw_ecommerce_data')
        
        print(f"Loaded {len(final_df)} records to data warehouse")
        return f"Successfully loaded {len(final_df)} records"
    
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