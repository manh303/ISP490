import os
from app.db_config import DATABASE_URL

# Database Configuration
DB_URL = os.getenv('DB_URL', 'postgresql+psycopg2://admin:admin@database-postgres:5432/ecom')
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://admin:admin_password@mongodb:27017/')

# Airflow Configuration
AIRFLOW_URL = os.getenv('AIRFLOW_URL', 'http://localhost:8080')
AIRFLOW_USERNAME = os.getenv('AIRFLOW_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'admin')

# Security & JWT
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')
JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', os.getenv('JWT_SECRET', 'sY-A335Mj9qloyUE94maevhmrg25MZ3RxbVhBYAhmu5QnIS1qsCKIiiGjRshkZA4OSwZN2k2O5VSzDn3XdZo5A'))
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
JWT_EXPIRATION_HOURS = int(os.getenv('JWT_EXPIRATION_HOURS', '24'))

# CORS
CORS_ORIGINS = os.getenv('CORS_ORIGINS', '["*"]')

# Version
VERSION = os.getenv('VERSION', '1.0.0')

class Settings:
    """Application settings"""
    database_url: str = DATABASE_URL
    mongo_url: str = MONGO_URL
    airflow_url: str = AIRFLOW_URL
    airflow_username: str = AIRFLOW_USERNAME
    airflow_password: str = AIRFLOW_PASSWORD
    secret_key: str = SECRET_KEY
    jwt_secret_key: str = JWT_SECRET_KEY
    JWT_SECRET_KEY: str = JWT_SECRET_KEY  # Alias for compatibility
    jwt_algorithm: str = JWT_ALGORITHM
    JWT_ALGORITHM: str = JWT_ALGORITHM  # Alias for compatibility
    jwt_expiration_hours: int = JWT_EXPIRATION_HOURS
    version: str = VERSION

settings = Settings()
