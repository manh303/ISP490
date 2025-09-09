import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from loguru import logger
from src.utils.config import Config

class DatabaseManager:
    def __init__(self):
        self.config = Config()
        self.postgres_engine = None
        self.mongo_client = None
    
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        if not self.postgres_engine:
            connection_string = (
                f"postgresql://{self.config.POSTGRES_USER}:"
                f"{self.config.POSTGRES_PASSWORD}@"
                f"{self.config.POSTGRES_HOST}:"
                f"{self.config.POSTGRES_PORT}/"
                f"{self.config.POSTGRES_DB}"
            )
            self.postgres_engine = create_engine(connection_string)
            logger.info("Connected to PostgreSQL")
        return self.postgres_engine
    
    def get_mongo_connection(self):
        """Get MongoDB connection"""
        if not self.mongo_client:
            connection_string = f"mongodb://{self.config.MONGO_HOST}:{self.config.MONGO_PORT}"
            self.mongo_client = MongoClient(connection_string)
            logger.info("Connected to MongoDB")
        return self.mongo_client
    
    def save_to_postgres(self, df: pd.DataFrame, table_name: str, if_exists='replace'):
        """Save DataFrame to PostgreSQL"""
        try:
            engine = self.get_postgres_connection()
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            logger.info(f"Saved {len(df)} records to PostgreSQL table: {table_name}")
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {e}")
    
    def save_to_mongo(self, data, collection_name: str, database_name=None):
        """Save data to MongoDB"""
        try:
            client = self.get_mongo_connection()
            db_name = database_name or self.config.MONGO_DB
            db = client[db_name]
            collection = db[collection_name]
            
            if isinstance(data, pd.DataFrame):
                records = data.to_dict('records')
            else:
                records = data
            
            result = collection.insert_many(records)
            logger.info(f"Saved {len(result.inserted_ids)} records to MongoDB: {collection_name}")
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}")
    
    def load_from_postgres(self, query: str) -> pd.DataFrame:
        """Load data from PostgreSQL"""
        try:
            engine = self.get_postgres_connection()
            df = pd.read_sql(query, engine)
            logger.info(f"Loaded {len(df)} records from PostgreSQL")
            return df
        except Exception as e:
            logger.error(f"Error loading from PostgreSQL: {e}")
            return pd.DataFrame()