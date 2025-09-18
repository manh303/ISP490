import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from loguru import logger
from utils.config import Config

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
        """Get MongoDB connection with authentication"""
        if not self.mongo_client:
            try:
                # Check if MongoDB auth is configured
                if hasattr(self.config, 'MONGO_USER') and hasattr(self.config, 'MONGO_PASSWORD'):
                    # Use authenticated connection
                    auth_source = getattr(self.config, 'MONGO_AUTH_SOURCE', 'admin')
                    self.mongo_client = MongoClient(
                        host=self.config.MONGO_HOST,
                        port=self.config.MONGO_PORT,
                        username=self.config.MONGO_USER,
                        password=self.config.MONGO_PASSWORD,
                        authSource=auth_source,
                        serverSelectionTimeoutMS=5000
                    )
                    # Test the connection
                    self.mongo_client.admin.command('ping')
                    logger.info("Connected to MongoDB with authentication")
                else:
                    # Fallback to connection without auth (for local development)
                    connection_string = f"mongodb://{self.config.MONGO_HOST}:{self.config.MONGO_PORT}"
                    self.mongo_client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
                    # Test the connection
                    self.mongo_client.admin.command('ping')
                    logger.info("Connected to MongoDB without authentication")
                    
            except Exception as e:
                logger.error(f"MongoDB connection failed: {e}")
                # If MongoDB is optional, you can set it to None
                self.mongo_client = None
                
        return self.mongo_client
    
    def save_to_postgres(self, df: pd.DataFrame, table_name: str, if_exists='replace'):
        """Save DataFrame to PostgreSQL"""
        try:
            engine = self.get_postgres_connection()
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            logger.info(f"Saved {len(df)} records to PostgreSQL table: {table_name}")
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {e}")
            raise  # Re-raise since PostgreSQL is critical
    
    def save_to_mongo(self, data, collection_name: str, database_name=None):
        """Save data to MongoDB"""
        try:
            client = self.get_mongo_connection()
            
            # Skip if MongoDB connection failed
            if client is None:
                logger.warning("MongoDB connection not available, skipping backup")
                return
            
            db_name = database_name or self.config.MONGO_DB
            db = client[db_name]
            collection = db[collection_name]
            
            if isinstance(data, pd.DataFrame):
                records = data.to_dict('records')
            else:
                records = data
            
            # Clear existing data and insert new (optional - you can modify this)
            collection.delete_many({})
            
            if records:  # Only insert if there are records
                result = collection.insert_many(records)
                logger.info(f"Saved {len(result.inserted_ids)} records to MongoDB: {collection_name}")
            else:
                logger.info(f"No records to save to MongoDB collection: {collection_name}")
                
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}")
            # Don't raise - MongoDB is backup, not critical
    
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
    
    def load_from_mongo(self, collection_name: str, database_name=None, query=None) -> pd.DataFrame:
        """Load data from MongoDB"""
        try:
            client = self.get_mongo_connection()
            
            if client is None:
                logger.warning("MongoDB connection not available")
                return pd.DataFrame()
            
            db_name = database_name or self.config.MONGO_DB
            db = client[db_name]
            collection = db[collection_name]
            
            # Use query filter if provided, otherwise get all documents
            cursor = collection.find(query or {})
            data = list(cursor)
            
            if data:
                df = pd.DataFrame(data)
                # Remove MongoDB's _id column if present
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)
                logger.info(f"Loaded {len(df)} records from MongoDB collection: {collection_name}")
                return df
            else:
                logger.info(f"No records found in MongoDB collection: {collection_name}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error loading from MongoDB: {e}")
            return pd.DataFrame()
    
    def close_connections(self):
        """Close database connections"""
        if self.postgres_engine:
            self.postgres_engine.dispose()
            logger.info("PostgreSQL connection closed")
        
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")