#!/usr/bin/env python3
"""
Production-Ready FastAPI Backend for Big Data Streaming
Supports real-time data, WebSockets, ML inference, and dual database access
"""

import os
import sys
import time
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

# FastAPI and async
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn

# Database connections
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from motor.motor_asyncio import AsyncIOMotorClient
import redis.asyncio as redis
from databases import Database

# Data processing
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field, validator
from typing_extensions import Annotated

# Kafka for real-time streaming
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import aiofiles

# ML and Analytics
import joblib
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor

# Monitoring and logging
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import structlog
from loguru import logger

# Add project paths
sys.path.append('/app/backend')
sys.path.append('/app/app')  # Add app directory for simple_auth module
sys.path.append('/app/data-pipeline/src')
sys.path.append('/app/streaming')

# Import security and performance modules
try:
    from security import (
        SecurityConfig, AuthenticationService, SecurityMiddleware,
        RateLimiter, SecurityMonitor, create_database_tables
    )
    from performance import (
        CacheManager, QueryOptimizer, PerformanceMonitor,
        PerformanceMiddleware, OptimizationUtils
    )
    from auth_endpoints import auth_router
    SECURITY_ENABLED = True
    logger.info("✅ Security and Performance modules loaded")
except ImportError as e:
    logger.warning(f"⚠️ Security/Performance modules not available: {e}")
    SECURITY_ENABLED = False

# ====================================
# CONFIGURATION
# ====================================
class Settings:
    # Database URLs
    POSTGRES_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://dss_user:dss_password_123@postgres:5432/ecommerce_dss")
    MONGODB_URL: str = os.getenv("MONGODB_URL", "mongodb://admin:admin_password@mongodb:27017/")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379")
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_SERVERS", "kafka:29092")
    KAFKA_TOPICS = {
        'products': 'products_stream',
        'customers': 'customers_stream', 
        'orders': 'orders_stream',
        'transactions': 'transactions_stream',
        'analytics': 'analytics_stream'
    }
    
    # API Configuration
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "Big Data Streaming Analytics API"
    VERSION: str = "2.0.0"
    DESCRIPTION: str = "Production-ready API for real-time big data analytics"
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-super-secret-key-change-in-production")
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "your-super-secret-jwt-key-change-this-in-production")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Rate Limiting
    RATE_LIMIT_ENABLED: bool = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"

    # Performance
    CACHE_ENABLED: bool = os.getenv("CACHE_ENABLED", "true").lower() == "true"
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "300"))
    
    # Performance
    MAX_CONNECTIONS_COUNT: int = 10
    MIN_CONNECTIONS_COUNT: int = 1
    
    # ML Models Path
    MODELS_PATH: str = "/app/models"

settings = Settings()

# ====================================
# PYDANTIC MODELS
# ====================================
class StreamingDataRequest(BaseModel):
    topic: str = Field(..., description="Kafka topic name")
    limit: int = Field(default=100, ge=1, le=1000)
    time_window: int = Field(default=60, description="Time window in seconds")

class AnalyticsQuery(BaseModel):
    metric: str = Field(..., description="Metric to analyze")
    time_range: str = Field(default="1h", description="Time range: 1h, 6h, 24h, 7d")
    aggregation: str = Field(default="avg", description="sum, avg, min, max, count")
    filters: Optional[Dict[str, Any]] = None

class PredictionRequest(BaseModel):
    model_name: str = Field(..., description="ML model name")
    features: Dict[str, Any] = Field(..., description="Input features")
    return_probabilities: bool = Field(default=False)

class RealTimeAlert(BaseModel):
    alert_id: str
    severity: str = Field(..., regex="^(low|medium|high|critical)$")
    message: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None

# ====================================
# DATABASE CONNECTIONS
# ====================================
class DatabaseManager:
    def __init__(self):
        self.postgres_engine = None
        self.postgres_database = None
        self.mongodb_client = None
        self.mongodb_db = None
        self.redis_client = None

        # Performance and Security components
        self.cache_manager = None
        self.query_optimizer = None
        self.performance_monitor = None
        self.auth_service = None
        self.security_monitor = None

    async def connect_postgres(self):
        """Connect to PostgreSQL"""
        self.postgres_engine = create_async_engine(
            settings.POSTGRES_URL,
            pool_size=settings.MAX_CONNECTIONS_COUNT,
            pool_pre_ping=True,
            echo=False
        )
        self.postgres_database = Database(settings.POSTGRES_URL)
        await self.postgres_database.connect()
        logger.info("Connected to PostgreSQL")

    async def connect_mongodb(self):
        """Connect to MongoDB"""
        self.mongodb_client = AsyncIOMotorClient(settings.MONGODB_URL)
        self.mongodb_db = self.mongodb_client.dss_streaming
        # Test connection
        await self.mongodb_client.admin.command('ping')
        logger.info("Connected to MongoDB")

    async def connect_redis(self):
        """Connect to Redis"""
        self.redis_client = redis.from_url(settings.REDIS_URL)
        # Test connection
        await self.redis_client.ping()
        logger.info("Connected to Redis")

        # Initialize performance and security components
        if SECURITY_ENABLED:
            self.cache_manager = CacheManager(self.redis_client, settings.CACHE_TTL)
            self.query_optimizer = QueryOptimizer(self.postgres_database)
            self.performance_monitor = PerformanceMonitor(self.cache_manager, self.query_optimizer)
            self.auth_service = AuthenticationService(self.postgres_database, self.redis_client)
            self.security_monitor = SecurityMonitor(self.redis_client)
            logger.info("✅ Performance and Security components initialized")

    async def disconnect_all(self):
        """Disconnect all databases"""
        if self.postgres_database:
            await self.postgres_database.disconnect()
        if self.mongodb_client:
            self.mongodb_client.close()
        if self.redis_client:
            await self.redis_client.close()

# ====================================
# KAFKA STREAMING MANAGER
# ====================================
class KafkaStreamingManager:
    def __init__(self):
        self.consumers = {}
        self.producer = None
        self.active_streams = set()

    async def get_producer(self):
        """Get Kafka producer"""
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
        return self.producer

    async def get_consumer(self, topic: str, group_id: str = None):
        """Get Kafka consumer for specific topic"""
        consumer_key = f"{topic}_{group_id or 'default'}"
        
        if consumer_key not in self.consumers:
            self.consumers[consumer_key] = KafkaConsumer(
                topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=group_id or f"api_consumer_{topic}",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )
        
        return self.consumers[consumer_key]

    async def stream_messages(self, topic: str, limit: int = 100):
        """Stream messages from Kafka topic"""
        consumer = await self.get_consumer(topic, f"stream_{datetime.now().timestamp()}")
        messages = []
        
        try:
            for message in consumer:
                messages.append({
                    'key': message.key,
                    'value': message.value,
                    'timestamp': message.timestamp,
                    'offset': message.offset,
                    'partition': message.partition
                })
                
                if len(messages) >= limit:
                    break
                    
        except Exception as e:
            logger.error(f"Error streaming from {topic}: {e}")
        finally:
            consumer.close()
            
        return messages

# ====================================
# ML MODEL MANAGER
# ====================================
class MLModelManager:
    def __init__(self):
        self.models = {}
        self.scalers = {}
        self.model_metadata = {}

    async def load_models(self):
        """Load all ML models"""
        models_path = Path(settings.MODELS_PATH)
        if not models_path.exists():
            logger.warning(f"Models path {models_path} does not exist")
            return

        for model_file in models_path.glob("*.pkl"):
            model_name = model_file.stem
            try:
                # Load model
                with open(model_file, 'rb') as f:
                    self.models[model_name] = pickle.load(f)
                
                # Load scaler if exists
                scaler_file = models_path / f"{model_name}_scaler.pkl"
                if scaler_file.exists():
                    with open(scaler_file, 'rb') as f:
                        self.scalers[model_name] = pickle.load(f)

                # Load metadata if exists
                metadata_file = models_path / f"{model_name}_metadata.json"
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        self.model_metadata[model_name] = json.load(f)

                logger.info(f"Loaded ML model: {model_name}")
                
            except Exception as e:
                logger.error(f"Error loading model {model_name}: {e}")

    async def predict(self, model_name: str, features: Dict[str, Any], return_probabilities: bool = False):
        """Make prediction using specified model"""
        if model_name not in self.models:
            raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

        try:
            model = self.models[model_name]
            
            # Convert features to DataFrame
            df = pd.DataFrame([features])
            
            # Apply scaling if scaler exists
            if model_name in self.scalers:
                scaler = self.scalers[model_name]
                df_scaled = pd.DataFrame(
                    scaler.transform(df),
                    columns=df.columns,
                    index=df.index
                )
            else:
                df_scaled = df

            # Make prediction
            prediction = model.predict(df_scaled)
            
            result = {
                'model': model_name,
                'prediction': prediction.tolist(),
                'timestamp': datetime.now().isoformat()
            }

            # Add probabilities if requested and supported
            if return_probabilities and hasattr(model, 'predict_proba'):
                probabilities = model.predict_proba(df_scaled)
                result['probabilities'] = probabilities.tolist()

            return result

        except Exception as e:
            logger.error(f"Prediction error for {model_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

# ====================================
# WEBSOCKET MANAGER
# ====================================
class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.subscribers: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, topic: str = "general"):
        """Connect WebSocket client"""
        await websocket.accept()
        self.active_connections.append(websocket)
        
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(websocket)
        
        logger.info(f"WebSocket connected to topic: {topic}")

    def disconnect(self, websocket: WebSocket, topic: str = "general"):
        """Disconnect WebSocket client"""
        self.active_connections.remove(websocket)
        if topic in self.subscribers:
            self.subscribers[topic].remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send message to specific WebSocket"""
        await websocket.send_text(message)

    async def broadcast_to_topic(self, message: Dict[str, Any], topic: str):
        """Broadcast message to all subscribers of a topic"""
        if topic in self.subscribers:
            disconnected = []
            for connection in self.subscribers[topic]:
                try:
                    await connection.send_json(message)
                except WebSocketDisconnect:
                    disconnected.append(connection)
                except Exception as e:
                    logger.error(f"Error sending WebSocket message: {e}")
                    disconnected.append(connection)
            
            # Remove disconnected clients
            for conn in disconnected:
                self.disconnect(conn, topic)

# ====================================
# INITIALIZE MANAGERS
# ====================================
db_manager = DatabaseManager()
kafka_manager = KafkaStreamingManager()
ml_manager = MLModelManager()
websocket_manager = WebSocketManager()

# Prometheus metrics
REQUEST_COUNT = Counter('api_requests_total', 'Total API requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('api_request_duration_seconds', 'API request duration')
ACTIVE_CONNECTIONS = Gauge('websocket_connections_active', 'Active WebSocket connections')
STREAMING_MESSAGES = Counter('streaming_messages_total', 'Total streaming messages processed', ['topic'])

# ====================================
# FASTAPI APPLICATION
# ====================================
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add security and performance middleware
if SECURITY_ENABLED:
    # Add security middleware
    @app.middleware("http")
    async def security_middleware(request, call_next):
        security_mw = SecurityMiddleware(app, db_manager.redis_client)
        return await security_mw(request, call_next)

    # Add performance middleware
    @app.middleware("http")
    async def performance_middleware(request, call_next):
        if db_manager.performance_monitor:
            perf_mw = PerformanceMiddleware(db_manager.performance_monitor)
            return await perf_mw(request, call_next)
        return await call_next(request)

# Security
security = HTTPBearer()

# ====================================
# STARTUP/SHUTDOWN EVENTS
# ====================================
@app.on_event("startup")
async def startup_event():
    """Initialize connections and load models"""
    logger.info("Starting Big Data Streaming API...")

    # Set startup time for metrics
    app.start_time = time.time()

    # Connect to databases
    await db_manager.connect_postgres()
    await db_manager.connect_mongodb()
    await db_manager.connect_redis()

    # Initialize database tables for security if enabled
    if SECURITY_ENABLED:
        try:
            tables_sql = create_database_tables()
            await db_manager.postgres_database.execute(tables_sql)
            logger.info("✅ Security database tables initialized")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize security tables: {e}")

    # Load ML models
    await ml_manager.load_models()

    logger.info("API startup completed successfully!")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections"""
    logger.info("Shutting down API...")
    await db_manager.disconnect_all()
    logger.info("API shutdown completed")

# ====================================
# DEPENDENCY INJECTION
# ====================================
async def get_postgres_session():
    """Get PostgreSQL session"""
    return db_manager.postgres_database

async def get_mongodb():
    """Get MongoDB database"""
    return db_manager.mongodb_db

async def get_redis():
    """Get Redis client"""
    return db_manager.redis_client

async def get_cache_manager():
    """Get cache manager"""
    return db_manager.cache_manager

async def get_auth_service():
    """Get authentication service"""
    return db_manager.auth_service

# ====================================
# HEALTH CHECK & METRICS
# ====================================
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    # Check PostgreSQL
    try:
        db = await get_postgres_session()
        await db.execute("SELECT 1")
        health_status["services"]["postgresql"] = "healthy"
    except Exception as e:
        health_status["services"]["postgresql"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check MongoDB
    try:
        mongodb = await get_mongodb()
        await mongodb.command("ping")
        health_status["services"]["mongodb"] = "healthy"
    except Exception as e:
        health_status["services"]["mongodb"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Redis
    try:
        redis_client = await get_redis()
        await redis_client.ping()
        health_status["services"]["redis"] = "healthy"
    except Exception as e:
        health_status["services"]["redis"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status

@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    try:
        # Generate Prometheus metrics
        metrics_output = generate_latest()
        return Response(content=metrics_output, media_type="text/plain; charset=utf-8")
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        # Return basic metrics if prometheus client fails
        basic_metrics = f'''# HELP backend_health Backend health status
# TYPE backend_health gauge
backend_health{{status="ok"}} 1

# HELP backend_requests_total Total HTTP requests
# TYPE backend_requests_total counter
backend_requests_total{{method="GET",endpoint="/health"}} {REQUEST_COUNT.labels(method="GET", endpoint="/health")._value._value}

# HELP backend_active_connections Active connections
# TYPE backend_active_connections gauge
backend_active_connections {{}} {len(websocket_manager.active_connections)}

# HELP backend_uptime_seconds Backend uptime in seconds
# TYPE backend_uptime_seconds counter
backend_uptime_seconds {{}} {time.time() - app.start_time if hasattr(app, 'start_time') else 0}
'''
        return Response(content=basic_metrics, media_type="text/plain; charset=utf-8")

# ====================================
# SECURITY & PERFORMANCE ENDPOINTS
# ====================================
if SECURITY_ENABLED:
    @app.get("/api/v1/security/stats")
    async def get_security_stats():
        """Get security monitoring statistics"""
        if db_manager.security_monitor:
            return await db_manager.security_monitor.get_security_stats()
        return {"error": "Security monitoring not available"}

    @app.get("/api/v1/performance/report")
    async def get_performance_report():
        """Get comprehensive performance report"""
        if db_manager.performance_monitor:
            return await db_manager.performance_monitor.get_performance_report()
        return {"error": "Performance monitoring not available"}

    @app.get("/api/v1/cache/stats")
    async def get_cache_stats():
        """Get cache statistics"""
        if db_manager.cache_manager:
            return await db_manager.cache_manager.get_stats()
        return {"error": "Cache not available"}

    @app.post("/api/v1/cache/clear")
    async def clear_cache(pattern: str = "*"):
        """Clear cache by pattern"""
        if db_manager.cache_manager:
            cleared = await db_manager.cache_manager.clear_pattern(pattern)
            return {"cleared_keys": cleared, "pattern": pattern}
        return {"error": "Cache not available"}

# ====================================
# STREAMING DATA ENDPOINTS
# ====================================
@app.get(f"{settings.API_V1_PREFIX}/streaming/topics")
async def get_kafka_topics():
    """Get available Kafka topics"""
    return {
        "topics": list(settings.KAFKA_TOPICS.values()),
        "topic_mapping": settings.KAFKA_TOPICS
    }

@app.post(f"{settings.API_V1_PREFIX}/streaming/data")
async def get_streaming_data(request: StreamingDataRequest):
    """Get real-time streaming data from Kafka"""
    REQUEST_COUNT.labels(method="POST", endpoint="/streaming/data").inc()
    
    topic = request.topic
    if topic not in settings.KAFKA_TOPICS.values():
        raise HTTPException(status_code=400, detail=f"Invalid topic: {topic}")
    
    try:
        with REQUEST_DURATION.time():
            messages = await kafka_manager.stream_messages(topic, request.limit)
            STREAMING_MESSAGES.labels(topic=topic).inc(len(messages))
            
        return {
            "topic": topic,
            "message_count": len(messages),
            "time_window": request.time_window,
            "messages": messages,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching streaming data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_PREFIX}/streaming/live/{{topic}}")
async def stream_live_data(topic: str):
    """Stream live data via Server-Sent Events"""
    if topic not in settings.KAFKA_TOPICS.values():
        raise HTTPException(status_code=400, detail=f"Invalid topic: {topic}")
    
    async def event_generator():
        consumer = await kafka_manager.get_consumer(topic, f"sse_{datetime.now().timestamp()}")
        
        try:
            while True:
                message_batch = consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        data = {
                            'topic': topic,
                            'key': message.key,
                            'value': message.value,
                            'timestamp': message.timestamp,
                            'offset': message.offset
                        }
                        yield f"data: {json.dumps(data)}\n\n"
                        
                await asyncio.sleep(0.1)  # Small delay
                
        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for topic: {topic}")
        finally:
            consumer.close()
    
    return StreamingResponse(
        event_generator(),
        media_type="text/plain",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )

# ====================================
# ANALYTICS ENDPOINTS
# ====================================
@app.post(f"{settings.API_V1_PREFIX}/analytics/query")
async def run_analytics_query(query: AnalyticsQuery, db: Database = Depends(get_postgres_session)):
    """Run analytics query on processed data"""
    REQUEST_COUNT.labels(method="POST", endpoint="/analytics/query").inc()
    
    # Parse time range
    time_mapping = {
        "1h": timedelta(hours=1),
        "6h": timedelta(hours=6), 
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30)
    }
    
    if query.time_range not in time_mapping:
        raise HTTPException(status_code=400, detail="Invalid time range")
    
    end_time = datetime.now()
    start_time = end_time - time_mapping[query.time_range]
    
    try:
        # Build SQL query based on metric and aggregation
        sql_query = f"""
        SELECT 
            DATE_TRUNC('hour', created_at) as time_bucket,
            {query.aggregation}({query.metric}) as value
        FROM analytics_summary 
        WHERE created_at BETWEEN :start_time AND :end_time
        """
        
        # Add filters if provided
        if query.filters:
            filter_conditions = []
            for key, value in query.filters.items():
                filter_conditions.append(f"{key} = '{value}'")
            
            if filter_conditions:
                sql_query += " AND " + " AND ".join(filter_conditions)
        
        sql_query += " GROUP BY time_bucket ORDER BY time_bucket"
        
        result = await db.fetch_all(
            sql_query,
            {"start_time": start_time, "end_time": end_time}
        )
        
        return {
            "metric": query.metric,
            "aggregation": query.aggregation,
            "time_range": query.time_range,
            "data_points": len(result),
            "results": [dict(row) for row in result],
            "query_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Analytics query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ====================================
# ML PREDICTION ENDPOINTS
# ====================================
# Include authentication endpoints if security is enabled
if SECURITY_ENABLED:
    try:
        app.include_router(auth_router, prefix="/api/v1/auth")
        logger.info("✅ Authentication endpoints loaded")
    except Exception as e:
        logger.warning(f"⚠️ Auth endpoints not available: {e}")

# Try to load ML endpoints
try:
    import sys
    sys.path.append('/app/backend/app')
    from ml_endpoints import ml_router
    app.include_router(ml_router)
    logger.info("✅ ML endpoints loaded successfully")
except Exception as e:
    logger.warning(f"⚠️ ML endpoints not available: {e}")

    # Create fallback ML status endpoint
    @app.get("/api/v1/ml/status")
    async def ml_status():
        return {
            "status": "ML endpoints not available",
            "error": str(e),
            "available_models": [],
            "timestamp": datetime.now().isoformat()
        }

# Load Simple Authentication endpoints
try:
    from simple_auth import simple_auth_router
    app.include_router(simple_auth_router)
    logger.info("✅ Simple Authentication endpoints loaded successfully")
except Exception as e:
    logger.warning(f"⚠️ Simple Auth endpoints not available: {e}")

# Load Simple DSS endpoints
try:
    from simple_dss import simple_dss_router
    app.include_router(simple_dss_router)
    logger.info("✅ Simple DSS endpoints loaded successfully")
except Exception as e:
    logger.warning(f"⚠️ Simple DSS endpoints not available: {e}")

# ====================================
# ADVANCED ANALYTICS ENDPOINTS
# ====================================
@app.get(f"{settings.API_V1_PREFIX}/analytics/dashboard")
async def get_analytics_dashboard(
    db: Database = Depends(get_postgres_session),
    current_user: dict = Depends(lambda: get_auth_service()) if SECURITY_ENABLED else None
):
    """Get comprehensive analytics dashboard data"""
    try:
        # Real-time metrics
        metrics = {}

        # Total customers
        result = await db.fetch_one("SELECT COUNT(DISTINCT customer_id) as total FROM vietnam_customers_large")
        metrics['total_customers'] = result['total'] if result else 0

        # Total orders
        result = await db.fetch_one("SELECT COUNT(DISTINCT order_id) as total FROM vietnam_orders_large")
        metrics['total_orders'] = result['total'] if result else 0

        # Total revenue
        result = await db.fetch_one("SELECT SUM(total_amount_vnd) as total FROM vietnam_orders_large WHERE total_amount_vnd IS NOT NULL")
        metrics['total_revenue'] = float(result['total']) if result and result['total'] else 0

        # Average order value
        result = await db.fetch_one("SELECT AVG(total_amount_vnd) as avg FROM vietnam_orders_large WHERE total_amount_vnd IS NOT NULL")
        metrics['avg_order_value'] = float(result['avg']) if result and result['avg'] else 0

        # Recent activity (last 30 days simulation)
        recent_activity = await db.fetch_all("""
            SELECT
                DATE(order_date) as date,
                COUNT(DISTINCT order_id) as orders,
                SUM(total_amount_vnd) as revenue
            FROM vietnam_orders_large
            WHERE order_date IS NOT NULL
            AND total_amount_vnd IS NOT NULL
            GROUP BY DATE(order_date)
            ORDER BY date DESC
            LIMIT 30
        """)

        # Top products
        top_products = await db.fetch_all("""
            SELECT
                category_l1,
                COUNT(*) as sales_count,
                SUM(price_vnd) as total_revenue,
                AVG(rating) as avg_rating
            FROM vietnam_products_large
            WHERE category_l1 IS NOT NULL
            AND price_vnd IS NOT NULL
            GROUP BY category_l1
            ORDER BY sales_count DESC
            LIMIT 10
        """)

        # Customer segments (simulation)
        customer_segments = await db.fetch_all("""
            SELECT
                CASE
                    WHEN COUNT(order_id) >= 5 THEN 'Champions'
                    WHEN COUNT(order_id) >= 3 THEN 'Loyal Customers'
                    WHEN COUNT(order_id) >= 2 THEN 'Potential Loyalists'
                    ELSE 'New Customers'
                END as segment,
                COUNT(DISTINCT customer_id) as customer_count,
                AVG(total_amount_vnd) as avg_order_value
            FROM vietnam_orders_large
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
            HAVING COUNT(order_id) > 0
        """)

        return {
            "success": True,
            "overview_metrics": metrics,
            "recent_activity": [dict(row) for row in recent_activity],
            "top_products": [dict(row) for row in top_products],
            "customer_segments": [dict(row) for row in customer_segments],
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Dashboard analytics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_PREFIX}/analytics/real-time-stats")
async def get_real_time_stats(
    redis_client: redis.Redis = Depends(get_redis),
    db: Database = Depends(get_postgres_session),
    current_user: dict = Depends(lambda: get_auth_service()) if SECURITY_ENABLED else None
):
    """Get real-time system statistics"""
    try:
        stats = {
            "timestamp": datetime.now().isoformat(),
            "system_health": "healthy",
            "active_connections": len(websocket_manager.active_connections),
            "cache_stats": {}
        }

        # Redis stats
        try:
            redis_info = await redis_client.info()
            stats["cache_stats"] = {
                "connected_clients": redis_info.get("connected_clients", 0),
                "used_memory": redis_info.get("used_memory_human", "0B"),
                "keyspace_hits": redis_info.get("keyspace_hits", 0),
                "keyspace_misses": redis_info.get("keyspace_misses", 0)
            }
        except Exception:
            stats["cache_stats"] = {"status": "unavailable"}

        # Database quick stats
        try:
            result = await db.fetch_one("SELECT COUNT(*) as count FROM vietnam_products_large")
            stats["database_records"] = result['count'] if result else 0
        except Exception:
            stats["database_records"] = 0

        return stats

    except Exception as e:
        logger.error(f"Real-time stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ====================================
# WEBSOCKET ENDPOINTS
# ====================================
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """WebSocket endpoint for real-time data streaming"""
    await websocket_manager.connect(websocket, client_id)

    try:
        while True:
            # Wait for messages from client
            data = await websocket.receive_text()
            message_data = json.loads(data)

            # Handle different message types
            if message_data.get("type") == "subscribe":
                topic = message_data.get("topic", "general")
                await websocket_manager.connect(websocket, topic)
                await websocket.send_json({
                    "type": "subscription_confirmed",
                    "topic": topic,
                    "timestamp": datetime.now().isoformat()
                })

            elif message_data.get("type") == "request_data":
                # Send real-time analytics data
                try:
                    redis_client = await get_redis()
                    db = await get_postgres_session()
                    stats = await get_real_time_stats(redis_client, db)

                    await websocket.send_json({
                        "type": "real_time_data",
                        "data": stats,
                        "timestamp": datetime.now().isoformat()
                    })
                except Exception as e:
                    await websocket.send_json({
                        "type": "error",
                        "message": str(e),
                        "timestamp": datetime.now().isoformat()
                    })

            # Echo back other messages
            else:
                await websocket.send_json({
                    "type": "echo",
                    "original_message": message_data,
                    "timestamp": datetime.now().isoformat()
                })

    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, client_id)
        logger.info(f"WebSocket client {client_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}")
        websocket_manager.disconnect(websocket, client_id)

# Continue with more endpoints...
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1
    )