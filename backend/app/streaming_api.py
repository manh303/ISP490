#!/usr/bin/env python3
"""
Vietnam E-commerce Streaming API
===============================
Real-time streaming API endpoints for Vietnam e-commerce data warehouse
Provides WebSocket and HTTP endpoints for real-time data streaming

Features:
- Real-time sales data streaming
- Customer activity tracking
- Product performance monitoring
- Platform analytics
- Vietnam market insights
- WebSocket connections for live updates

Author: DSS Team
Version: 1.0.0
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, AsyncGenerator
import uuid
from contextlib import asynccontextmanager

# FastAPI imports
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn

# Async database
import asyncpg
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient

# Kafka integration
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import aiokafka

# Data processing
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Monitoring
import time
import psutil

# Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================================================
# CONFIGURATION
# ====================================================================

class StreamingConfig:
    """Streaming API Configuration"""

    # Database connections
    POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://dss_user:dss_password_123@postgres:5432/ecommerce_dss")
    MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://admin:admin_password@mongodb:27017/")
    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(",")

    # Streaming settings
    MAX_WEBSOCKET_CONNECTIONS = int(os.getenv("MAX_WEBSOCKET_CONNECTIONS", "100"))
    STREAM_BUFFER_SIZE = int(os.getenv("STREAM_BUFFER_SIZE", "1000"))
    CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "vietnam_streaming_api")

    # Performance settings
    BATCH_SIZE = int(os.getenv("STREAMING_BATCH_SIZE", "100"))
    FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL", "1.0"))  # seconds

    # Security
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key")
    API_KEY = os.getenv("STREAMING_API_KEY", "vietnam-ecommerce-streaming")

config = StreamingConfig()

# ====================================================================
# DATA MODELS
# ====================================================================

class StreamingMetrics(BaseModel):
    """Streaming metrics model"""
    timestamp: datetime
    active_connections: int
    messages_per_second: float
    total_messages: int
    memory_usage_mb: float
    cpu_usage_percent: float
    kafka_lag: int

class SalesEvent(BaseModel):
    """Sales event model"""
    event_id: str
    order_id: str
    customer_id: str
    product_id: str
    platform: str
    total_amount_vnd: int
    total_amount_usd: float
    payment_method: str
    region: str
    province: str
    is_tet_order: bool = False
    is_festival_order: bool = False
    timestamp: datetime

class CustomerActivity(BaseModel):
    """Customer activity model"""
    activity_id: str
    customer_id: str
    activity_type: str
    platform: str
    device_type: str
    duration_seconds: int
    timestamp: datetime

class ProductPerformance(BaseModel):
    """Product performance model"""
    product_id: str
    product_name: str
    category: str
    brand: str
    views: int
    purchases: int
    revenue_vnd: int
    platform: str
    timestamp: datetime

class StreamingRequest(BaseModel):
    """Streaming request model"""
    topics: List[str] = Field(default=["vietnam_sales_events"])
    filters: Dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=100, ge=1, le=1000)
    format: str = Field(default="json", pattern="^(json|csv)$")

# ====================================================================
# DATABASE CONNECTIONS
# ====================================================================

class DatabaseManager:
    """Async database connection manager"""

    def __init__(self):
        self.postgres_engine = None
        self.mongodb_client = None
        self.redis_client = None
        self.kafka_consumer = None

    async def initialize(self):
        """Initialize all database connections"""
        try:
            # PostgreSQL
            self.postgres_engine = create_async_engine(
                config.POSTGRES_URL,
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )

            # MongoDB
            self.mongodb_client = AsyncIOMotorClient(config.MONGODB_URL)

            # Redis
            self.redis_client = aioredis.from_url(
                config.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )

            # Test connections
            async with self.postgres_engine.begin() as conn:
                await conn.execute("SELECT 1")

            await self.mongodb_client.admin.command('ping')
            await self.redis_client.ping()

            logger.info("✅ All database connections initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize databases: {e}")
            raise

    async def close(self):
        """Close all database connections"""
        try:
            if self.postgres_engine:
                await self.postgres_engine.dispose()
            if self.mongodb_client:
                self.mongodb_client.close()
            if self.redis_client:
                await self.redis_client.close()
            if self.kafka_consumer:
                await self.kafka_consumer.stop()

            logger.info("✅ All database connections closed")

        except Exception as e:
            logger.error(f"❌ Error closing databases: {e}")

# Global database manager
db_manager = DatabaseManager()

# ====================================================================
# WEBSOCKET CONNECTION MANAGER
# ====================================================================

class ConnectionManager:
    """WebSocket connection manager"""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_topics: Dict[str, List[str]] = {}
        self.connection_filters: Dict[str, Dict] = {}

    async def connect(self, websocket: WebSocket, connection_id: str, topics: List[str], filters: Dict = None):
        """Add new WebSocket connection"""
        await websocket.accept()
        self.active_connections[connection_id] = websocket
        self.connection_topics[connection_id] = topics
        self.connection_filters[connection_id] = filters or {}

        logger.info(f"🔌 WebSocket connected: {connection_id}, Topics: {topics}")

        # Send welcome message
        await self.send_to_connection(connection_id, {
            "type": "connection_established",
            "connection_id": connection_id,
            "topics": topics,
            "timestamp": datetime.now().isoformat(),
            "message": "Connected to Vietnam E-commerce Streaming API"
        })

    def disconnect(self, connection_id: str):
        """Remove WebSocket connection"""
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
            del self.connection_topics[connection_id]
            del self.connection_filters[connection_id]
            logger.info(f"🔌 WebSocket disconnected: {connection_id}")

    async def send_to_connection(self, connection_id: str, message: Dict):
        """Send message to specific connection"""
        if connection_id in self.active_connections:
            try:
                websocket = self.active_connections[connection_id]
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"❌ Error sending to {connection_id}: {e}")
                self.disconnect(connection_id)

    async def broadcast_to_topic(self, topic: str, message: Dict):
        """Broadcast message to all connections subscribed to topic"""
        disconnected = []

        for connection_id, topics in self.connection_topics.items():
            if topic in topics:
                try:
                    # Apply filters if any
                    filters = self.connection_filters.get(connection_id, {})
                    if self._message_matches_filters(message, filters):
                        await self.send_to_connection(connection_id, message)
                except Exception as e:
                    logger.error(f"❌ Error broadcasting to {connection_id}: {e}")
                    disconnected.append(connection_id)

        # Clean up disconnected connections
        for connection_id in disconnected:
            self.disconnect(connection_id)

    def _message_matches_filters(self, message: Dict, filters: Dict) -> bool:
        """Check if message matches connection filters"""
        if not filters:
            return True

        for key, value in filters.items():
            if key in message:
                if isinstance(value, list):
                    if message[key] not in value:
                        return False
                else:
                    if message[key] != value:
                        return False

        return True

    def get_stats(self) -> Dict:
        """Get connection manager statistics"""
        topic_counts = {}
        for topics in self.connection_topics.values():
            for topic in topics:
                topic_counts[topic] = topic_counts.get(topic, 0) + 1

        return {
            "total_connections": len(self.active_connections),
            "topic_subscriptions": topic_counts,
            "active_connection_ids": list(self.active_connections.keys())
        }

# Global connection manager
connection_manager = ConnectionManager()

# ====================================================================
# KAFKA STREAMING CONSUMER
# ====================================================================

class VietnamKafkaStreamer:
    """Kafka consumer for Vietnam e-commerce streaming"""

    def __init__(self):
        self.consumer = None
        self.running = False
        self.stats = {
            "messages_consumed": 0,
            "messages_per_second": 0.0,
            "last_message_time": None,
            "kafka_lag": 0
        }

    async def initialize(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = aiokafka.AIOKafkaConsumer(
                *["vietnam_sales_events", "vietnam_customers", "vietnam_products", "vietnam_user_activities"],
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.CONSUMER_GROUP_ID,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                consumer_timeout_ms=1000,
                max_poll_records=config.BATCH_SIZE,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
            )

            await self.consumer.start()
            logger.info("✅ Kafka consumer initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
            raise

    async def start_streaming(self):
        """Start Kafka message streaming"""
        if self.running:
            return

        self.running = True
        logger.info("🚀 Starting Kafka streaming...")

        message_count = 0
        start_time = time.time()

        try:
            async for message in self.consumer:
                if not self.running:
                    break

                # Update statistics
                message_count += 1
                current_time = time.time()
                elapsed = current_time - start_time

                self.stats["messages_consumed"] = message_count
                self.stats["messages_per_second"] = message_count / elapsed if elapsed > 0 else 0
                self.stats["last_message_time"] = datetime.now().isoformat()

                # Process message based on topic
                await self._process_message(message)

                # Update Redis with latest stats every 100 messages
                if message_count % 100 == 0:
                    await self._update_stats_in_redis()

        except Exception as e:
            logger.error(f"❌ Error in Kafka streaming: {e}")
        finally:
            await self.stop_streaming()

    async def _process_message(self, message):
        """Process individual Kafka message"""
        try:
            topic = message.topic
            data = message.value

            if not data:
                return

            # Add streaming metadata
            streaming_message = {
                "topic": topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": datetime.now().isoformat(),
                "data": data
            }

            # Transform based on topic
            if topic == "vietnam_sales_events":
                transformed = await self._transform_sales_event(data)
                await connection_manager.broadcast_to_topic("sales", transformed)

            elif topic == "vietnam_customers":
                transformed = await self._transform_customer_data(data)
                await connection_manager.broadcast_to_topic("customers", transformed)

            elif topic == "vietnam_products":
                transformed = await self._transform_product_data(data)
                await connection_manager.broadcast_to_topic("products", transformed)

            elif topic == "vietnam_user_activities":
                transformed = await self._transform_activity_data(data)
                await connection_manager.broadcast_to_topic("activities", transformed)

            # Store in Redis for recent data access
            await self._cache_recent_data(topic, transformed)

        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")

    async def _transform_sales_event(self, data: Dict) -> Dict:
        """Transform sales event for streaming"""
        return {
            "type": "sales_event",
            "event_id": data.get("event_id"),
            "order_id": data.get("order_id"),
            "customer_id": data.get("customer_id"),
            "product_id": data.get("product_id"),
            "platform": data.get("platform"),
            "total_amount_vnd": data.get("total_amount_vnd", 0),
            "total_amount_usd": data.get("total_amount_usd", 0),
            "payment_method": data.get("payment_method"),
            "shipping_region": data.get("shipping_region"),
            "shipping_province": data.get("shipping_province"),
            "is_cod": data.get("is_cod", False),
            "is_tet_order": data.get("is_tet_order", False),
            "is_festival_order": data.get("is_festival_order", False),
            "device_type": data.get("device_type"),
            "timestamp": data.get("event_timestamp", datetime.now().isoformat())
        }

    async def _transform_customer_data(self, data: Dict) -> Dict:
        """Transform customer data for streaming"""
        return {
            "type": "customer_update",
            "customer_id": data.get("customer_id"),
            "region": data.get("region"),
            "province": data.get("province"),
            "customer_segment": data.get("customer_segment"),
            "preferred_platform": data.get("preferred_platform"),
            "preferred_payment": data.get("preferred_payment"),
            "timestamp": data.get("created_at", datetime.now().isoformat())
        }

    async def _transform_product_data(self, data: Dict) -> Dict:
        """Transform product data for streaming"""
        return {
            "type": "product_update",
            "product_id": data.get("product_id"),
            "product_name": data.get("product_name_vn"),
            "brand": data.get("brand"),
            "category": data.get("category_l1"),
            "price_vnd": data.get("price_vnd", 0),
            "rating": data.get("rating", 0),
            "available_platforms": data.get("available_platforms", []),
            "made_in_vietnam": data.get("made_in_vietnam", False),
            "timestamp": data.get("created_at", datetime.now().isoformat())
        }

    async def _transform_activity_data(self, data: Dict) -> Dict:
        """Transform activity data for streaming"""
        return {
            "type": "user_activity",
            "activity_id": data.get("activity_id"),
            "customer_id": data.get("customer_id"),
            "activity_type": data.get("activity_type"),
            "platform": data.get("platform"),
            "device_type": data.get("device_type"),
            "duration_seconds": data.get("duration_seconds", 0),
            "timestamp": data.get("timestamp", datetime.now().isoformat())
        }

    async def _cache_recent_data(self, topic: str, data: Dict):
        """Cache recent data in Redis"""
        try:
            # Store in Redis list (keep last 1000 items per topic)
            redis_key = f"recent_{topic}"
            await db_manager.redis_client.lpush(redis_key, json.dumps(data))
            await db_manager.redis_client.ltrim(redis_key, 0, 999)  # Keep only last 1000
            await db_manager.redis_client.expire(redis_key, 3600)  # Expire in 1 hour

        except Exception as e:
            logger.error(f"❌ Error caching data: {e}")

    async def _update_stats_in_redis(self):
        """Update streaming statistics in Redis"""
        try:
            stats_key = "vietnam_streaming_stats"
            await db_manager.redis_client.hset(stats_key, mapping=self.stats)
            await db_manager.redis_client.expire(stats_key, 300)  # Expire in 5 minutes

        except Exception as e:
            logger.error(f"❌ Error updating stats: {e}")

    async def stop_streaming(self):
        """Stop Kafka streaming"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        logger.info("🛑 Kafka streaming stopped")

    def get_stats(self) -> Dict:
        """Get streaming statistics"""
        return self.stats.copy()

# Global Kafka streamer
kafka_streamer = VietnamKafkaStreamer()

# ====================================================================
# FASTAPI APPLICATION
# ====================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("🚀 Starting Vietnam E-commerce Streaming API...")

    try:
        # Initialize database connections
        await db_manager.initialize()

        # Initialize Kafka streaming
        await kafka_streamer.initialize()

        # Start Kafka streaming in background
        asyncio.create_task(kafka_streamer.start_streaming())

        logger.info("✅ Streaming API started successfully")

    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        raise

    yield

    # Shutdown
    logger.info("🛑 Shutting down Vietnam E-commerce Streaming API...")

    try:
        await kafka_streamer.stop_streaming()
        await db_manager.close()

        logger.info("✅ Application shutdown complete")

    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

# Create FastAPI application
app = FastAPI(
    title="Vietnam E-commerce Streaming API",
    description="Real-time streaming API for Vietnam e-commerce data warehouse",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API key"""
    if credentials.credentials != config.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials.credentials

# ====================================================================
# API ENDPOINTS
# ====================================================================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Vietnam E-commerce Streaming API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "websocket": "/ws/stream",
            "health": "/health",
            "metrics": "/metrics",
            "recent_data": "/recent/{topic}",
            "stats": "/stats"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connections
        await db_manager.redis_client.ping()

        # Check Kafka consumer
        kafka_stats = kafka_streamer.get_stats()

        # Check system resources
        memory_usage = psutil.virtual_memory().percent
        cpu_usage = psutil.cpu_percent()

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "connections": {
                "redis": "connected",
                "kafka": "connected" if kafka_streamer.running else "disconnected"
            },
            "metrics": {
                "memory_usage_percent": memory_usage,
                "cpu_usage_percent": cpu_usage,
                "active_websockets": len(connection_manager.active_connections),
                "kafka_messages_consumed": kafka_stats.get("messages_consumed", 0),
                "kafka_messages_per_second": kafka_stats.get("messages_per_second", 0)
            }
        }

    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/metrics")
async def get_metrics():
    """Get streaming metrics"""
    try:
        # System metrics
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent()

        # Connection metrics
        connection_stats = connection_manager.get_stats()

        # Kafka metrics
        kafka_stats = kafka_streamer.get_stats()

        # Redis metrics
        redis_info = await db_manager.redis_client.info()

        return StreamingMetrics(
            timestamp=datetime.now(),
            active_connections=connection_stats["total_connections"],
            messages_per_second=kafka_stats.get("messages_per_second", 0),
            total_messages=kafka_stats.get("messages_consumed", 0),
            memory_usage_mb=memory.used / 1024 / 1024,
            cpu_usage_percent=cpu_percent,
            kafka_lag=kafka_stats.get("kafka_lag", 0)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting metrics: {e}")

@app.get("/stats")
async def get_stats():
    """Get detailed statistics"""
    try:
        return {
            "streaming": kafka_streamer.get_stats(),
            "connections": connection_manager.get_stats(),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting stats: {e}")

@app.get("/recent/{topic}")
async def get_recent_data(topic: str, limit: int = 100):
    """Get recent data from specific topic"""
    try:
        redis_key = f"recent_{topic}"

        # Get recent data from Redis
        recent_data = await db_manager.redis_client.lrange(redis_key, 0, limit - 1)

        if not recent_data:
            return {
                "topic": topic,
                "data": [],
                "count": 0,
                "timestamp": datetime.now().isoformat()
            }

        # Parse JSON data
        parsed_data = []
        for item in recent_data:
            try:
                parsed_data.append(json.loads(item))
            except json.JSONDecodeError:
                continue

        return {
            "topic": topic,
            "data": parsed_data,
            "count": len(parsed_data),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting recent data: {e}")

@app.post("/stream/data")
async def stream_data(request: StreamingRequest, api_key: str = Depends(verify_api_key)):
    """Stream data as HTTP response"""
    async def generate_stream():
        try:
            for topic in request.topics:
                redis_key = f"recent_{topic}"

                # Get data from Redis
                data = await db_manager.redis_client.lrange(redis_key, 0, request.limit - 1)

                for item in data:
                    try:
                        parsed_item = json.loads(item)

                        # Apply filters
                        if request.filters:
                            match = True
                            for key, value in request.filters.items():
                                if key in parsed_item and parsed_item[key] != value:
                                    match = False
                                    break
                            if not match:
                                continue

                        # Format output
                        if request.format == "json":
                            yield f"data: {json.dumps(parsed_item)}\n\n"
                        else:  # CSV format
                            # Convert to CSV-like format
                            yield f"{','.join(str(v) for v in parsed_item.values())}\n"

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    if request.format == "json":
        return StreamingResponse(
            generate_stream(),
            media_type="text/plain",
            headers={"Cache-Control": "no-cache"}
        )
    else:
        return StreamingResponse(
            generate_stream(),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=vietnam_ecommerce_data.csv",
                "Cache-Control": "no-cache"
            }
        )

# ====================================================================
# WEBSOCKET ENDPOINTS
# ====================================================================

@app.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    topics: str = "sales",  # Comma-separated topics
    filters: str = "{}"  # JSON string of filters
):
    """WebSocket endpoint for real-time streaming"""
    connection_id = f"ws_{uuid.uuid4().hex[:8]}"

    try:
        # Parse parameters
        topic_list = [t.strip() for t in topics.split(",")]
        filter_dict = json.loads(filters) if filters != "{}" else {}

        # Validate topics
        valid_topics = ["sales", "customers", "products", "activities"]
        topic_list = [t for t in topic_list if t in valid_topics]

        if not topic_list:
            await websocket.close(code=4000, reason="No valid topics specified")
            return

        # Check connection limit
        if len(connection_manager.active_connections) >= config.MAX_WEBSOCKET_CONNECTIONS:
            await websocket.close(code=4001, reason="Maximum connections exceeded")
            return

        # Connect
        await connection_manager.connect(websocket, connection_id, topic_list, filter_dict)

        # Keep connection alive and handle client messages
        while True:
            try:
                # Wait for client message or timeout
                message = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)

                # Handle client commands
                try:
                    command = json.loads(message)

                    if command.get("type") == "ping":
                        await connection_manager.send_to_connection(connection_id, {
                            "type": "pong",
                            "timestamp": datetime.now().isoformat()
                        })

                    elif command.get("type") == "update_filters":
                        new_filters = command.get("filters", {})
                        connection_manager.connection_filters[connection_id] = new_filters
                        await connection_manager.send_to_connection(connection_id, {
                            "type": "filters_updated",
                            "filters": new_filters,
                            "timestamp": datetime.now().isoformat()
                        })

                except json.JSONDecodeError:
                    # Invalid JSON, ignore
                    pass

            except asyncio.TimeoutError:
                # Send heartbeat
                await connection_manager.send_to_connection(connection_id, {
                    "type": "heartbeat",
                    "connection_id": connection_id,
                    "timestamp": datetime.now().isoformat()
                })

            except WebSocketDisconnect:
                break

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
    finally:
        connection_manager.disconnect(connection_id)

# ====================================================================
# BACKGROUND TASKS
# ====================================================================

@app.on_event("startup")
async def startup_tasks():
    """Background startup tasks"""

    # Send periodic connection stats
    async def update_connection_stats():
        while True:
            try:
                stats = connection_manager.get_stats()
                await db_manager.redis_client.hset(
                    "websocket_stats",
                    mapping={
                        "total_connections": stats["total_connections"],
                        "topic_subscriptions": json.dumps(stats["topic_subscriptions"]),
                        "timestamp": datetime.now().isoformat()
                    }
                )
                await asyncio.sleep(10)  # Update every 10 seconds

            except Exception as e:
                logger.error(f"❌ Error updating connection stats: {e}")
                await asyncio.sleep(30)

    # Start background task
    asyncio.create_task(update_connection_stats())

# ====================================================================
# MAIN EXECUTION
# ====================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Vietnam E-commerce Streaming API')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8001, help='Port to bind to')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload')
    parser.add_argument('--workers', type=int, default=1, help='Number of worker processes')

    args = parser.parse_args()

    uvicorn.run(
        "streaming_api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers,
        log_level="info"
    )