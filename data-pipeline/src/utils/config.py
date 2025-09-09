import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Kaggle API
    KAGGLE_USERNAME = os.getenv('nguyenducmanhk17hl')
    KAGGLE_KEY = os.getenv('44008be3ffb2fdea340a073a43388ab4')
    
    # Database
    POSTGRES_HOST = os.getenv('DB_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('DB_PORT', '5432')
    POSTGRES_DB = os.getenv('DB_NAME', 'ecommerce_dss')
    POSTGRES_USER = os.getenv('DB_USER', 'dss_user')
    POSTGRES_PASSWORD = os.getenv('DB_PASSWORD', 'dss_password_123')
    
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = os.getenv('MONGO_PORT', '27017')
    MONGO_DB = os.getenv('MONGO_DB', 'ecommerce_dss')
    
    # Paths
    RAW_DATA_PATH = '../data/raw'
    PROCESSED_DATA_PATH = '../data/processed'
    MODELS_PATH = '../data/models'
    EXPORTS_PATH = '../data/exports'
    
    # Spark
    SPARK_APP_NAME = "EcommerceDSS"
    SPARK_MASTER = "local[*]"