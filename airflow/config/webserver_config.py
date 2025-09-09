import os
from airflow import configuration as conf
from flask_appbuilder.security.manager import AUTH_DB

# Security configuration
AUTH_TYPE = AUTH_DB

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# The default user self registration role
AUTH_USER_REGISTRATION_ROLE = "Public"

# Config for Flask-WTF flag for CSRF
SECRET_KEY = os.getenv('AIRFLOW__WEBSERVER__SECRET_KEY', 'your-secret-key-here')

# The SQLAlchemy connection string
SQLALCHEMY_DATABASE_URI = conf.get('database', 'SQL_ALCHEMY_CONN')

# Flask-AppBuilder's Security Manager
SECURITY_MANAGER_CLASS = 'airflow.www.security.AirflowSecurityManager'

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow': {
            'format': '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers': {
        'airflow.processor': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO'
    }
}