import os

# Database Configuration
DB_URL = os.getenv('DB_URL', 'postgresql+psycopg2://admin:admin@database-postgres:5432/ecom')
DATABASE_URL = os.getenv('DATABASE_URL', DB_URL)
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://admin:admin_password@mongodb:27017/')

# Airflow Configuration
AIRFLOW_URL = os.getenv('AIRFLOW_URL', 'http://localhost:8080')
AIRFLOW_USERNAME = os.getenv('AIRFLOW_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'admin')

# Security
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')
JWT_SECRET = os.getenv('JWT_SECRET', 'your-jwt-secret-here')

# CORS
CORS_ORIGINS = os.getenv('CORS_ORIGINS', '["*"]')

class Settings:
    """Application settings"""
    database_url: str = DATABASE_URL
    mongo_url: str = MONGO_URL
    airflow_url: str = AIRFLOW_URL
    airflow_username: str = AIRFLOW_USERNAME
    airflow_password: str = AIRFLOW_PASSWORD
    secret_key: str = SECRET_KEY
    jwt_secret: str = JWT_SECRET

settings = Settings()
