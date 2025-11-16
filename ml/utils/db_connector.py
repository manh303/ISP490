# -*- coding: utf-8 -*-
"""
Database Connector for DWH
Handles connection to PostgreSQL and data fetching
"""

import logging
from typing import Optional, List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import os
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)


class DWHConnector:
    """PostgreSQL DWH Connection Manager"""
    
    @staticmethod
    def _load_config() -> dict:
        """Load configuration from config.yaml"""
        config_path = Path(__file__).parent.parent / 'config.yaml'
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def __init__(self, 
                 host: str = None, 
                 port: int = None,
                 database: str = None,
                 user: str = None,
                 password: str = None):
        """
        Initialize DWH connection
        
        Args:
            host: Database host
            port: Database port (default: 5432)
            database: Database name
            user: Database user
            password: Database password
        """
        # Load config from YAML
        config = self._load_config()
        db_config = config.get('database', {})
        
        # Use provided values, then config file, then env vars, then defaults
        self.host = host or db_config.get('host') or os.getenv('DB_HOST')
        self.port = port or db_config.get('port') or int(os.getenv('DB_PORT', 5432))
        self.database = database or db_config.get('database') or os.getenv('DB_NAME')
        self.user = user or db_config.get('user') or os.getenv('DB_USER')
        self.password = password or db_config.get('password') or os.getenv('DB_PASSWORD')
        
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError(
                "Missing database credentials. Please set in config.yaml or environment variables:\n"
                f"  host: {self.host}\n"
                f"  port: {self.port}\n"
                f"  database: {self.database}\n"
                f"  user: {self.user}\n"
                f"  password: {'***' if self.password else 'NOT SET'}"
            )
        
        self.engine = None
        self.connect()
    
    def connect(self):
        """Create database connection"""
        try:
            connection_string = (
                f"postgresql://{self.user}:{self.password}@"
                f"{self.host}:{self.port}/{self.database}"
            )
            
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                echo=False
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"✓ Connected to DWH: {self.host}:{self.port}/{self.database}")
        
        except Exception as e:
            logger.error(f"✗ Failed to connect to DWH: {e}")
            raise
    
    def query(self, sql: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Execute query and return DataFrame
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            pd.DataFrame: Query result
        """
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql_query(
                    text(sql),
                    conn,
                    params=params
                )
            logger.info(f"✓ Query executed: {len(result)} rows")
            return result
        
        except Exception as e:
            logger.error(f"✗ Query failed: {e}")
            raise
    
    def insert(self, table_name: str, df: pd.DataFrame):
        """
        Insert DataFrame to table
        
        Args:
            table_name: Target table
            df: DataFrame to insert
        """
        try:
            rows = df.to_sql(
                table_name,
                self.engine,
                if_exists='append',
                index=False
            )
            logger.info(f"✓ Inserted {rows} rows to {table_name}")
        
        except Exception as e:
            logger.error(f"✗ Insert failed: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        sql = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = :table_name
        ORDER BY ordinal_position
        """
        return self.query(sql, {'table_name': table_name})
    
    def close(self):
        """Close connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("✓ Connection closed")


# Utility functions
def get_dwh_connector() -> DWHConnector:
    """Get DWH connector instance"""
    return DWHConnector()


def fetch_demand_data(days: int = 180) -> pd.DataFrame:
    """
    Fetch demand data for all products
    
    Args:
        days: Lookback period in days
        
    Returns:
        pd.DataFrame: Demand data
    """
    conn = DWHConnector()
    
    sql = f"""
    SELECT 
        fac.date_sk,
        fac.product_sk,
        prod.product_name,
        cat.category_name,
        dim_date.date_value,
        fac.price_current,
        fac.price_original,
        fac.discount_pct,
        fac.rating_avg,
        fac.rating_count,
        fac.review_count,
        fac.sold_count,
        fac.is_available
    FROM dwh_fact_product_daily fac
    JOIN dwh_dim_product prod ON fac.product_sk = prod.product_sk
    JOIN dwh_dim_category cat ON prod.category_sk = cat.category_sk
    JOIN dwh_dim_date dim_date ON fac.date_sk = dim_date.date_sk
    WHERE dim_date.date_value >= CURRENT_DATE - INTERVAL '{days} days'
    AND fac.sold_count IS NOT NULL
    AND fac.sold_count > 0
    ORDER BY fac.date_sk DESC
    """
    
    try:
        df = conn.query(sql)
        logger.info(f"✓ Fetched {len(df)} demand records")
        return df
    
    finally:
        conn.close()


def fetch_recommendation_data(days: int = 90) -> pd.DataFrame:
    """
    Fetch product review data for recommendation system
    
    Args:
        days: Lookback period in days
        
    Returns:
        pd.DataFrame: Product review data
    """
    conn = DWHConnector()
    
    sql = f"""
    SELECT 
        fac.product_sk,
        prod.product_name,
        cat.category_sk,
        cat.category_name,
        fac.rating_avg,
        fac.review_count,
        fac.total_reviews,
        fac.sentiment_score,
        fac.positive_reviews,
        fac.negative_reviews
    FROM dwh_fact_review_summary fac
    JOIN dwh_dim_product prod ON fac.product_sk = prod.product_sk
    JOIN dwh_dim_category cat ON prod.category_sk = cat.category_sk
    JOIN dwh_dim_date dim_date ON fac.date_sk = dim_date.date_sk
    WHERE dim_date.date_value >= CURRENT_DATE - INTERVAL '{days} days'
    AND fac.total_reviews > 0
    GROUP BY fac.product_sk, prod.product_name, 
             cat.category_sk, cat.category_name,
             fac.rating_avg, fac.review_count, 
             fac.total_reviews, fac.sentiment_score,
             fac.positive_reviews, fac.negative_reviews
    ORDER BY fac.total_reviews DESC
    """
    
    try:
        df = conn.query(sql)
        logger.info(f"✓ Fetched {len(df)} products for recommendation")
        return df
    
    finally:
        conn.close()
