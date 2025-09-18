import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Kaggle API - FIXED: Should use environment variable names, not values
    KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME', 'nguyenducmanhk17hl')
    KAGGLE_KEY = os.getenv('KAGGLE_KEY', '44008be3ffb2fdea340a073a43388ab4')
    
    # PostgreSQL Database Configuration
    POSTGRES_HOST = os.getenv('DB_HOST', 'postgres')
    POSTGRES_PORT = int(os.getenv('DB_PORT', 5432))  # Convert to int
    POSTGRES_DB = os.getenv('DB_NAME', 'ecommerce_dss')
    POSTGRES_USER = os.getenv('DB_USER', 'dss_user')
    POSTGRES_PASSWORD = os.getenv('DB_PASSWORD', 'dss_password_123')
    
    # MongoDB Configuration - ADDED missing authentication fields
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))  # Convert to int
    MONGO_DB = os.getenv('MONGO_DB', 'ecommerce_dss')
    MONGO_USER = os.getenv('MONGO_USER', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin_password')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
    
    # Feature Flags - ADDED
    ENABLE_MONGO_BACKUP = os.getenv('ENABLE_MONGO_BACKUP', 'false').lower() == 'true'
    
    # Data Paths - IMPROVED: Use Path objects and absolute paths
    BASE_PATH = Path(__file__).parent.parent  # Project root
    RAW_DATA_PATH = BASE_PATH / 'data' / 'raw'
    PROCESSED_DATA_PATH = BASE_PATH / 'data' / 'processed'
    MODELS_PATH = BASE_PATH / 'data' / 'models'
    EXPORTS_PATH = BASE_PATH / 'data' / 'exports'
    STAGING_PATH = BASE_PATH / 'data' / 'staging'
    
    # Processing Configuration - ADDED
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10000))
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', 3))
    
    # Spark Configuration
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'EcommerceDSS')
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    
    # Logging Configuration - ADDED
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = os.getenv('LOG_FORMAT', '{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}')
    
    @classmethod
    def create_directories(cls):
        """Create necessary directories if they don't exist"""
        directories = [
            cls.RAW_DATA_PATH,
            cls.PROCESSED_DATA_PATH,
            cls.MODELS_PATH,
            cls.EXPORTS_PATH,
            cls.STAGING_PATH
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_postgres_connection_string(cls):
        """Get PostgreSQL connection string"""
        return (
            f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@"
            f"{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
        )
    
    @classmethod
    def get_mongo_connection_string(cls):
        """Get MongoDB connection string with authentication"""
        if cls.MONGO_USER and cls.MONGO_PASSWORD:
            return (
                f"mongodb://{cls.MONGO_USER}:{cls.MONGO_PASSWORD}@"
                f"{cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}"
                f"?authSource={cls.MONGO_AUTH_SOURCE}"
            )
        else:
            return f"mongodb://{cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}"
    
    @classmethod
    def validate_config(cls):
        """Validate critical configuration values"""
        issues = []
        
        # Check required PostgreSQL settings
        if not cls.POSTGRES_HOST:
            issues.append("POSTGRES_HOST is required")
        if not cls.POSTGRES_DB:
            issues.append("POSTGRES_DB is required")
        if not cls.POSTGRES_USER:
            issues.append("POSTGRES_USER is required")
        if not cls.POSTGRES_PASSWORD:
            issues.append("POSTGRES_PASSWORD is required")
        
        # Check Kaggle API if needed
        if not cls.KAGGLE_USERNAME or not cls.KAGGLE_KEY:
            issues.append("Kaggle API credentials missing (KAGGLE_USERNAME, KAGGLE_KEY)")
        
        if issues:
            raise ValueError(f"Configuration validation failed: {', '.join(issues)}")
        
        return True