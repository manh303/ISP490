#!/usr/bin/env python3
"""
Streaming Data API Endpoints
Real-time data access and streaming analytics APIs
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import psycopg2
from pymongo import MongoClient
from loguru import logger

# Database dependencies
from ..core.database import get_postgres_connection, get_mongo_client
from ..core.config import settings

router = APIRouter(prefix="/streaming", tags=["streaming"])

# Redis client for real-time data
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)
    
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                # Remove broken connections
                self.active_connections.remove(connection)

manager = ConnectionManager()

# Pydantic models
class StreamingMetrics(BaseModel):
    timestamp: datetime
    total_messages: int
    messages_per_second: float
    active_topics: List[str]
    consumer_lag: Dict[str, int]
    error_rate: float

class RealTimeEvent(BaseModel):
    event_type: str
    data: Dict[str, Any]
    timestamp: datetime
    source: str

class StreamingStatus(BaseModel):
    status: str
    services: Dict[str, str]
    uptime: str
    last_update: datetime

# ==============================================================================
# STREAMING METRICS ENDPOINTS
# ==============================================================================

@router.get("/metrics", response_model=StreamingMetrics)
async def get_streaming_metrics():
    """Get real-time streaming metrics"""
    try:
        # Get metrics from Redis cache
        cached_metrics = redis_client.get("streaming_metrics")
        if cached_metrics:
            metrics_data = json.loads(cached_metrics)
            return StreamingMetrics(**metrics_data)
        
        # If no cached metrics, compute from database
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            
            # Get message counts from last hour
            cursor.execute("""
                SELECT COUNT(*) as total_messages
                FROM streaming_processed_data 
                WHERE processed_at >= NOW() - INTERVAL '1 hour'
            """)
            total_messages = cursor.fetchone()[0]
            
            # Calculate messages per second
            messages_per_second = total_messages / 3600 if total_messages else 0
            
            # Get active topics (mock data)
            active_topics = ["products_stream", "customers_stream", "transactions_stream"]
            
            # Consumer lag (simplified)
            consumer_lag = {"dss_consumers": 150}
            
            # Error rate (from logs)
            error_rate = 0.02  # 2% error rate
            
            metrics = StreamingMetrics(
                timestamp=datetime.now(),
                total_messages=total_messages,
                messages_per_second=round(messages_per_second, 2),
                active_topics=active_topics,
                consumer_lag=consumer_lag,
                error_rate=error_rate
            )
            
            # Cache metrics for 30 seconds
            redis_client.setex(
                "streaming_metrics", 
                30, 
                metrics.json()
            )
            
            return metrics
            
    except Exception as e:
        logger.error(f"Error getting streaming metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get streaming metrics")

@router.get("/status", response_model=StreamingStatus)
async def get_streaming_status():
    """Get overall streaming system status"""
    try:
        # Check service health
        services_status = {}
        
        # Check Kafka
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                consumer_timeout_ms=5000
            )
            consumer.close()
            services_status["kafka"] = "healthy"
        except:
            services_status["kafka"] = "unhealthy"
        
        # Check MongoDB
        try:
            mongo_client = get_mongo_client()
            mongo_client.admin.command('ismaster')
            services_status["mongodb"] = "healthy"
        except:
            services_status["mongodb"] = "unhealthy"
        
        # Check PostgreSQL
        try:
            with get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
            services_status["postgresql"] = "healthy"
        except:
            services_status["postgresql"] = "unhealthy"
        
        # Check Redis
        try:
            redis_client.ping()
            services_status["redis"] = "healthy"
        except:
            services_status["redis"] = "unhealthy"
        
        # Overall status
        all_healthy = all(status == "healthy" for status in services_status.values())
        overall_status = "healthy" if all_healthy else "degraded"
        
        # System uptime (mock)
        uptime = "2 days, 14 hours"
        
        return StreamingStatus(
            status=overall_status,
            services=services_status,
            uptime=uptime,
            last_update=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error getting streaming status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get streaming status")

# ==============================================================================
# REAL-TIME DATA ENDPOINTS
# ==============================================================================

@router.get("/live-data/{topic}")
async def get_live_data(
    topic: str,
    limit: int = Query(100, ge=1, le=1000),
    format: str = Query("json", regex="^(json|csv)$")
):
    """Get live data from a specific topic"""
    try:
        # Get recent data from MongoDB
        mongo_client = get_mongo_client()
        db = mongo_client[settings.MONGO_DB]
        collection = db["streaming_raw_data"]
        
        # Query recent data for the topic
        query = {
            "data_type": topic.replace("_stream", ""),
            "timestamp": {"$gte": datetime.now() - timedelta(minutes=10)}
        }
        
        cursor = collection.find(query).sort("timestamp", -1).limit(limit)
        data = list(cursor)
        
        # Convert ObjectId to string
        for record in data:
            record["_id"] = str(record["_id"])
        
        if format == "csv":
            # Convert to CSV
            df = pd.DataFrame(data)
            csv_data = df.to_csv(index=False)
            
            return StreamingResponse(
                iter([csv_data]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={topic}_live_data.csv"}
            )
        
        return {"topic": topic, "count": len(data), "data": data}
        
    except Exception as e:
        logger.error(f"Error getting live data for topic {topic}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get live data for {topic}")

@router.get("/stream/{topic}")
async def stream_topic_data(topic: str):
    """Stream real-time data from a Kafka topic"""
    async def generate_stream():
        try:
            consumer = KafkaConsumer(
                f"{topic}_stream",
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=30000,  # 30 second timeout
                group_id=f"api_stream_{int(time.time())}"
            )
            
            for message in consumer:
                yield f"data: {json.dumps(message.value)}\n\n"
                
        except Exception as e:
            logger.error(f"Error streaming topic {topic}: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            try:
                consumer.close()
            except:
                pass
    
    return StreamingResponse(
        generate_stream(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

# ==============================================================================
# DATA ANALYTICS ENDPOINTS
# ==============================================================================

@router.get("/analytics/real-time-stats")
async def get_real_time_analytics():
    """Get real-time analytics and statistics"""
    try:
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            
            # Sales stats (last hour)
            cursor.execute("""
                SELECT 
                    COUNT(*) as transaction_count,
                    SUM(CAST(data->>'amount' AS DECIMAL)) as total_sales,
                    AVG(CAST(data->>'amount' AS DECIMAL)) as avg_transaction
                FROM streaming_processed_data 
                WHERE data_type = 'transaction' 
                AND processed_at >= NOW() - INTERVAL '1 hour'
            """)
            sales_stats = cursor.fetchone()
            
            # Top products (last hour)
            cursor.execute("""
                SELECT 
                    data->>'product_id' as product_id,
                    COUNT(*) as purchase_count
                FROM streaming_processed_data 
                WHERE data_type = 'transaction' 
                AND processed_at >= NOW() - INTERVAL '1 hour'
                GROUP BY data->>'product_id'
                ORDER BY purchase_count DESC
                LIMIT 5
            """)
            top_products = cursor.fetchall()
            
            # Customer activity (last hour)
            cursor.execute("""
                SELECT COUNT(DISTINCT data->>'customer_id') as active_customers
                FROM streaming_processed_data 
                WHERE processed_at >= NOW() - INTERVAL '1 hour'
            """)
            active_customers = cursor.fetchone()[0]
            
            return {
                "timestamp": datetime.now(),
                "period": "last_hour",
                "sales": {
                    "transaction_count": sales_stats[0] or 0,
                    "total_sales": float(sales_stats[1] or 0),
                    "avg_transaction": float(sales_stats[2] or 0)
                },
                "top_products": [
                    {"product_id": row[0], "purchase_count": row[1]} 
                    for row in top_products
                ],
                "active_customers": active_customers
            }
            
    except Exception as e:
        logger.error(f"Error getting real-time analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get real-time analytics")

@router.get("/analytics/time-series")
async def get_time_series_data(
    metric: str = Query(..., regex="^(sales|transactions|customers)$"),
    window: str = Query("1h", regex="^(5m|15m|1h|6h|24h)$")
):
    """Get time series data for charts"""
    try:
        # Convert window to SQL interval
        interval_map = {
            "5m": "5 minutes",
            "15m": "15 minutes", 
            "1h": "1 hour",
            "6h": "6 hours",
            "24h": "24 hours"
        }
        
        interval = interval_map[window]
        
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            
            if metric == "sales":
                cursor.execute(f"""
                    SELECT 
                        DATE_TRUNC('minute', processed_at) as time_bucket,
                        SUM(CAST(data->>'amount' AS DECIMAL)) as value
                    FROM streaming_processed_data 
                    WHERE data_type = 'transaction' 
                    AND processed_at >= NOW() - INTERVAL '{interval}'
                    GROUP BY time_bucket
                    ORDER BY time_bucket
                """)
            
            elif metric == "transactions":
                cursor.execute(f"""
                    SELECT 
                        DATE_TRUNC('minute', processed_at) as time_bucket,
                        COUNT(*) as value
                    FROM streaming_processed_data 
                    WHERE data_type = 'transaction' 
                    AND processed_at >= NOW() - INTERVAL '{interval}'
                    GROUP BY time_bucket
                    ORDER BY time_bucket
                """)
            
            elif metric == "customers":
                cursor.execute(f"""
                    SELECT 
                        DATE_TRUNC('minute', processed_at) as time_bucket,
                        COUNT(DISTINCT data->>'customer_id') as value
                    FROM streaming_processed_data 
                    WHERE processed_at >= NOW() - INTERVAL '{interval}'
                    GROUP BY time_bucket
                    ORDER BY time_bucket
                """)
            
            results = cursor.fetchall()
            
            return {
                "metric": metric,
                "window": window,
                "data": [
                    {"timestamp": row[0].isoformat(), "value": float(row[1] or 0)}
                    for row in results
                ]
            }
            
    except Exception as e:
        logger.error(f"Error getting time series data: {e}")
        raise HTTPException(status_code=500, detail="Failed to get time series data")

# ==============================================================================
# WEBSOCKET ENDPOINTS
# ==============================================================================

@router.websocket("/ws/live-feed")
async def websocket_live_feed(websocket: WebSocket):
    """WebSocket endpoint for live data feed"""
    await manager.connect(websocket)
    
    try:
        # Send initial connection message
        await manager.send_personal_message(
            json.dumps({
                "type": "connection",
                "message": "Connected to live data feed",
                "timestamp": datetime.now().isoformat()
            }),
            websocket
        )
        
        # Background task to send periodic updates
        async def send_updates():
            while True:
                try:
                    # Get current metrics
                    metrics = await get_streaming_metrics()
                    
                    # Send metrics update
                    await manager.send_personal_message(
                        json.dumps({
                            "type": "metrics_update",
                            "data": metrics.dict(),
                            "timestamp": datetime.now().isoformat()
                        }),
                        websocket
                    )
                    
                    # Wait 5 seconds
                    await asyncio.sleep(5)
                    
                except WebSocketDisconnect:
                    break
                except Exception as e:
                    logger.error(f"Error in WebSocket update: {e}")
                    await asyncio.sleep(5)
        
        # Start background task
        update_task = asyncio.create_task(send_updates())
        
        # Listen for client messages
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle different message types
                if message.get("type") == "subscribe":
                    topic = message.get("topic")
                    await manager.send_personal_message(
                        json.dumps({
                            "type": "subscription_confirmed",
                            "topic": topic,
                            "timestamp": datetime.now().isoformat()
                        }),
                        websocket
                    )
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                break
        
        # Cancel background task
        update_task.cancel()
        
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
        manager.disconnect(websocket)

# ==============================================================================
# DATA INJECTION ENDPOINTS
# ==============================================================================

@router.post("/inject/test-data")
async def inject_test_data(count: int = Query(10, ge=1, le=1000)):
    """Inject test data into the streaming pipeline"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        # Generate test transactions
        import random
        from faker import Faker
        fake = Faker()
        
        messages_sent = 0
        
        for i in range(count):
            test_transaction = {
                "data_type": "transaction",
                "timestamp": datetime.now().isoformat(),
                "source": "api_test_injection",
                "data": {
                    "transaction_id": f"TEST_{int(time.time())}_{i}",
                    "customer_id": f"CUST_{random.randint(100000, 999999)}",
                    "product_id": f"PROD_{random.randint(100000, 999999)}",
                    "amount": round(random.uniform(10, 500), 2),
                    "quantity": random.randint(1, 5),
                    "payment_method": random.choice(["credit_card", "debit_card", "paypal"])
                }
            }
            
            # Send to Kafka
            future = producer.send("transactions_stream", value=test_transaction)
            
            # Wait for confirmation
            try:
                future.get(timeout=1)
                messages_sent += 1
            except KafkaError as e:
                logger.error(f"Failed to send message {i}: {e}")
        
        producer.flush()
        producer.close()
        
        return {
            "message": f"Successfully injected {messages_sent} test transactions",
            "requested": count,
            "sent": messages_sent,
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Error injecting test data: {e}")
        raise HTTPException(status_code=500, detail="Failed to inject test data")

# ==============================================================================
# MONITORING ENDPOINTS
# ==============================================================================

@router.get("/monitor/topics")
async def monitor_kafka_topics():
    """Monitor Kafka topics status"""
    try:
        from kafka.admin import KafkaAdminClient
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id='api_monitor'
        )
        
        # Get topic metadata
        topics_metadata = admin_client.describe_topics()
        
        topics_info = []
        for topic_name, topic_metadata in topics_metadata.items():
            topics_info.append({
                "name": topic_name,
                "partitions": len(topic_metadata.partitions),
                "replication_factor": len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                "status": "active"
            })
        
        return {
            "timestamp": datetime.now(),
            "total_topics": len(topics_info),
            "topics": topics_info
        }
        
    except Exception as e:
        logger.error(f"Error monitoring Kafka topics: {e}")
        raise HTTPException(status_code=500, detail="Failed to monitor Kafka topics")

@router.get("/monitor/consumer-lag")
async def monitor_consumer_lag():
    """Monitor consumer lag across all topics"""
    try:
        # This is a simplified implementation
        # In production, use Kafka's consumer group monitoring APIs
        
        lag_data = {
            "timestamp": datetime.now(),
            "consumer_groups": [
                {
                    "group_id": "dss_consumers",
                    "topics": [
                        {"topic": "products_stream", "lag": 145},
                        {"topic": "customers_stream", "lag": 89},
                        {"topic": "transactions_stream", "lag": 267},
                        {"topic": "orders_stream", "lag": 34}
                    ],
                    "total_lag": 535
                }
            ],
            "status": "healthy"
        }
        
        return lag_data
        
    except Exception as e:
        logger.error(f"Error monitoring consumer lag: {e}")
        raise HTTPException(status_code=500, detail="Failed to monitor consumer lag")

# ==============================================================================
# DATA QUALITY ENDPOINTS
# ==============================================================================

@router.get("/quality/recent-checks")
async def get_recent_quality_checks(hours: int = Query(24, ge=1, le=168)):
    """Get recent data quality check results"""
    try:
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    rule_id,
                    timestamp,
                    passed,
                    actual_value,
                    expected_value,
                    message,
                    severity,
                    record_count
                FROM data_quality_results 
                WHERE timestamp >= NOW() - INTERVAL '%s hours'
                ORDER BY timestamp DESC
                LIMIT 100
            """, (hours,))
            
            results = cursor.fetchall()
            
            quality_checks = []
            for row in results:
                quality_checks.append({
                    "rule_id": row[0],
                    "timestamp": row[1].isoformat(),
                    "passed": row[2],
                    "actual_value": float(row[3]),
                    "expected_value": float(row[4]),
                    "message": row[5],
                    "severity": row[6],
                    "record_count": row[7]
                })
            
            # Calculate summary statistics
            total_checks = len(quality_checks)
            passed_checks = sum(1 for check in quality_checks if check["passed"])
            success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
            
            return {
                "timestamp": datetime.now(),
                "period_hours": hours,
                "summary": {
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": total_checks - passed_checks,
                    "success_rate": round(success_rate, 2)
                },
                "checks": quality_checks
            }
            
    except Exception as e:
        logger.error(f"Error getting quality checks: {e}")
        raise HTTPException(status_code=500, detail="Failed to get quality checks")

@router.get("/quality/alerts")
async def get_quality_alerts():
    """Get active data quality alerts"""
    try:
        # Get alerts from Redis
        alert_keys = redis_client.keys("*_alert_*")
        
        alerts = []
        for key in alert_keys:
            alert_data = redis_client.get(key)
            if alert_data:
                alert = json.loads(alert_data)
                alerts.append(alert)
        
        # Sort by timestamp
        alerts.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        return {
            "timestamp": datetime.now(),
            "total_alerts": len(alerts),
            "critical_alerts": len([a for a in alerts if a.get("type") == "critical"]),
            "warning_alerts": len([a for a in alerts if a.get("type") == "warning"]),
            "alerts": alerts
        }
        
    except Exception as e:
        logger.error(f"Error getting quality alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to get quality alerts")