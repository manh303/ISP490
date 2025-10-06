# Continue from main.py - ML and WebSocket endpoints

# ====================================
# ML PREDICTION ENDPOINTS
# ====================================
@app.get(f"{settings.API_V1_PREFIX}/ml/models")
async def get_available_models():
    """Get list of available ML models"""
    models_info = {}
    
    for model_name, model in ml_manager.models.items():
        model_info = {
            "name": model_name,
            "type": type(model).__name__,
            "has_scaler": model_name in ml_manager.scalers,
            "metadata": ml_manager.model_metadata.get(model_name, {})
        }
        
        # Get model-specific info
        if hasattr(model, 'feature_importances_'):
            model_info["supports_feature_importance"] = True
        if hasattr(model, 'predict_proba'):
            model_info["supports_probabilities"] = True
            
        models_info[model_name] = model_info
    
    return {
        "available_models": models_info,
        "total_models": len(ml_manager.models),
        "last_updated": datetime.now().isoformat()
    }

@app.post(f"{settings.API_V1_PREFIX}/ml/predict")
async def predict_with_model(request: PredictionRequest):
    """Make prediction using ML model"""
    REQUEST_COUNT.labels(method="POST", endpoint="/ml/predict").inc()
    
    try:
        with REQUEST_DURATION.time():
            result = await ml_manager.predict(
                request.model_name,
                request.features,
                request.return_probabilities
            )
        
        # Store prediction in MongoDB for tracking
        mongodb = await get_mongodb()
        prediction_record = {
            **result,
            "input_features": request.features,
            "stored_at": datetime.now()
        }
        await mongodb.predictions.insert_one(prediction_record)
        
        return result
        
    except Exception as e:
        logger.error(f"ML prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"{settings.API_V1_PREFIX}/ml/batch-predict")
async def batch_predict(
    model_name: str,
    features_list: List[Dict[str, Any]],
    return_probabilities: bool = False
):
    """Batch prediction for multiple inputs"""
    REQUEST_COUNT.labels(method="POST", endpoint="/ml/batch-predict").inc()
    
    if len(features_list) > 1000:
        raise HTTPException(status_code=400, detail="Maximum 1000 predictions per batch")
    
    try:
        predictions = []
        
        for i, features in enumerate(features_list):
            try:
                result = await ml_manager.predict(model_name, features, return_probabilities)
                result["batch_index"] = i
                predictions.append(result)
            except Exception as e:
                predictions.append({
                    "batch_index": i,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
        
        return {
            "model": model_name,
            "total_predictions": len(predictions),
            "successful_predictions": len([p for p in predictions if "error" not in p]),
            "predictions": predictions,
            "batch_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_PREFIX}/ml/feature-importance/{{model_name}}")
async def get_feature_importance(model_name: str):
    """Get feature importance for ML model"""
    if model_name not in ml_manager.models:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    
    model = ml_manager.models[model_name]
    
    if not hasattr(model, 'feature_importances_'):
        raise HTTPException(status_code=400, detail=f"Model {model_name} does not support feature importance")
    
    # Get feature names from metadata
    metadata = ml_manager.model_metadata.get(model_name, {})
    feature_names = metadata.get("feature_names", [f"feature_{i}" for i in range(len(model.feature_importances_))])
    
    importance_data = list(zip(feature_names, model.feature_importances_))
    importance_data.sort(key=lambda x: x[1], reverse=True)
    
    return {
        "model": model_name,
        "feature_importance": [
            {"feature": name, "importance": float(importance)}
            for name, importance in importance_data
        ],
        "total_features": len(importance_data)
    }

# ====================================
# REAL-TIME WEBSOCKET ENDPOINTS
# ====================================
@app.websocket(f"{settings.API_V1_PREFIX}/ws/{{topic}}")
async def websocket_endpoint(websocket: WebSocket, topic: str):
    """WebSocket endpoint for real-time data streaming"""
    await websocket_manager.connect(websocket, topic)
    ACTIVE_CONNECTIONS.inc()
    
    try:
        # Send welcome message
        await websocket_manager.send_personal_message(
            json.dumps({
                "type": "connection",
                "message": f"Connected to topic: {topic}",
                "timestamp": datetime.now().isoformat()
            }),
            websocket
        )
        
        # Start background task to stream Kafka data
        async def stream_kafka_data():
            consumer = await kafka_manager.get_consumer(topic, f"ws_{datetime.now().timestamp()}")
            
            try:
                while True:
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            ws_message = {
                                "type": "data",
                                "topic": topic,
                                "data": message.value,
                                "timestamp": message.timestamp,
                                "offset": message.offset
                            }
                            
                            await websocket_manager.send_personal_message(
                                json.dumps(ws_message, default=str),
                                websocket
                            )
                            
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"WebSocket streaming error: {e}")
            finally:
                consumer.close()
        
        # Start streaming task
        streaming_task = asyncio.create_task(stream_kafka_data())
        
        # Keep connection alive and handle client messages
        while True:
            try:
                data = await websocket.receive_text()
                client_message = json.loads(data)
                
                # Handle different message types
                if client_message.get("type") == "ping":
                    await websocket_manager.send_personal_message(
                        json.dumps({"type": "pong", "timestamp": datetime.now().isoformat()}),
                        websocket
                    )
                elif client_message.get("type") == "subscribe":
                    new_topic = client_message.get("topic")
                    if new_topic and new_topic in settings.KAFKA_TOPICS.values():
                        await websocket_manager.send_personal_message(
                            json.dumps({
                                "type": "subscription_changed",
                                "topic": new_topic,
                                "timestamp": datetime.now().isoformat()
                            }),
                            websocket
                        )
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket message handling error: {e}")
                break
        
        # Cancel streaming task
        streaming_task.cancel()
        
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected from topic: {topic}")
    finally:
        websocket_manager.disconnect(websocket, topic)
        ACTIVE_CONNECTIONS.dec()

@app.websocket(f"{settings.API_V1_PREFIX}/ws/alerts")
async def alerts_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time alerts"""
    await websocket_manager.connect(websocket, "alerts")
    ACTIVE_CONNECTIONS.inc()
    
    try:
        # Send connection confirmation
        await websocket_manager.send_personal_message(
            json.dumps({
                "type": "alert_connection",
                "message": "Connected to real-time alerts",
                "timestamp": datetime.now().isoformat()
            }),
            websocket
        )
        
        # Background task to monitor for alerts
        async def monitor_alerts():
            redis_client = await get_redis()
            
            while True:
                try:
                    # Check for new alerts in Redis
                    alert_data = await redis_client.lpop("alerts_queue")
                    
                    if alert_data:
                        alert = json.loads(alert_data)
                        await websocket_manager.send_personal_message(
                            json.dumps({
                                "type": "alert",
                                "data": alert,
                                "timestamp": datetime.now().isoformat()
                            }),
                            websocket
                        )
                    
                    await asyncio.sleep(1)  # Check every second
                    
                except Exception as e:
                    logger.error(f"Alert monitoring error: {e}")
                    await asyncio.sleep(5)
        
        # Start monitoring task
        monitoring_task = asyncio.create_task(monitor_alerts())
        
        # Keep connection alive
        while True:
            try:
                data = await websocket.receive_text()
                client_message = json.loads(data)
                
                if client_message.get("type") == "ping":
                    await websocket_manager.send_personal_message(
                        json.dumps({"type": "pong", "timestamp": datetime.now().isoformat()}),
                        websocket
                    )
                    
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Alert WebSocket error: {e}")
                break
        
        # Cancel monitoring task
        monitoring_task.cancel()
        
    except WebSocketDisconnect:
        logger.info("Alert WebSocket disconnected")
    finally:
        websocket_manager.disconnect(websocket, "alerts")
        ACTIVE_CONNECTIONS.dec()

# ====================================
# DASHBOARD DATA ENDPOINTS
# ====================================
@app.get(f"{settings.API_V1_PREFIX}/dashboard/summary")
async def get_dashboard_summary(
    db: Database = Depends(get_postgres_session),
    mongodb = Depends(get_mongodb)
):
    """Get dashboard summary data"""
    REQUEST_COUNT.labels(method="GET", endpoint="/dashboard/summary").inc()
    
    try:
        # Get summary from PostgreSQL (processed data)
        summary_sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(CASE WHEN created_at >= NOW() - INTERVAL '1 hour' THEN 1 ELSE 0 END) as hourly_records,
            AVG(order_value) as avg_order_value,
            MAX(created_at) as last_update
        FROM analytics_summary
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        
        summary_result = await db.fetch_one(summary_sql)
        
        # Get real-time streaming stats from MongoDB
        streaming_stats = await mongodb.streaming_metrics.find_one(
            {"metric_type": "hourly_summary"},
            sort=[("timestamp", -1)]
        )
        
        # Get model performance metrics
        model_stats = {}
        for model_name in ml_manager.models.keys():
            recent_predictions = await mongodb.predictions.count_documents({
                "model": model_name,
                "stored_at": {"$gte": datetime.now() - timedelta(hours=1)}
            })
            model_stats[model_name] = recent_predictions
        
        return {
            "processed_data": dict(summary_result) if summary_result else {},
            "streaming_stats": streaming_stats or {},
            "model_predictions": model_stats,
            "active_websockets": len(websocket_manager.active_connections),
            "kafka_topics": list(settings.KAFKA_TOPICS.values()),
            "system_health": "healthy",
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Dashboard summary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_PREFIX}/dashboard/real-time-metrics")
async def get_real_time_metrics(mongodb = Depends(get_mongodb)):
    """Get real-time metrics for dashboard"""
    try:
        # Get latest metrics from each topic
        metrics = {}
        
        for topic_name, topic in settings.KAFKA_TOPICS.items():
            latest_metric = await mongodb[f"{topic_name}_metrics"].find_one(
                sort=[("timestamp", -1)]
            )
            
            if latest_metric:
                metrics[topic_name] = {
                    "count": latest_metric.get("count", 0),
                    "avg_value": latest_metric.get("avg_value", 0),
                    "timestamp": latest_metric.get("timestamp")
                }
        
        return {
            "metrics": metrics,
            "timestamp": datetime.now().isoformat(),
            "update_frequency": "30s"
        }
        
    except Exception as e:
        logger.error(f"Real-time metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ====================================
# DATA EXPORT ENDPOINTS
# ====================================
@app.get(f"{settings.API_V1_PREFIX}/export/csv")
async def export_data_csv(
    table: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 10000,
    db: Database = Depends(get_postgres_session)
):
    """Export data as CSV"""
    REQUEST_COUNT.labels(method="GET", endpoint="/export/csv").inc()
    
    # Validate table name (security)
    allowed_tables = ["analytics_summary", "customer_summary", "product_summary"]
    if table not in allowed_tables:
        raise HTTPException(status_code=400, detail=f"Table not allowed: {table}")
    
    try:
        # Build query
        query = f"SELECT * FROM {table}"
        params = {}
        
        conditions = []
        if start_date:
            conditions.append("created_at >= :start_date")
            params["start_date"] = start_date
        if end_date:
            conditions.append("created_at <= :end_date")
            params["end_date"] = end_date
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += f" ORDER BY created_at DESC LIMIT {limit}"
        
        # Execute query
        result = await db.fetch_all(query, params)
        
        if not result:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Convert to DataFrame and CSV
        df = pd.DataFrame([dict(row) for row in result])
        
        # Generate CSV response
        def generate_csv():
            yield df.to_csv(index=False)
        
        return StreamingResponse(
            generate_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        )
        
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ====================================
# SYSTEM ADMINISTRATION
# ====================================
@app.post(f"{settings.API_V1_PREFIX}/admin/reload-models")
async def reload_ml_models():
    """Reload ML models from disk"""
    try:
        await ml_manager.load_models()
        return {
            "status": "success",
            "message": "ML models reloaded",
            "models_loaded": list(ml_manager.models.keys()),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Model reload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"{settings.API_V1_PREFIX}/admin/clear-cache")
async def clear_redis_cache():
    """Clear Redis cache"""
    try:
        redis_client = await get_redis()
        await redis_client.flushdb()
        
        return {
            "status": "success",
            "message": "Redis cache cleared",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Cache clear error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_PREFIX}/admin/system-stats")
async def get_system_stats():
    """Get system statistics"""
    import psutil
    
    try:
        stats = {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory": {
                "total": psutil.virtual_memory().total,
                "available": psutil.virtual_memory().available,
                "percent": psutil.virtual_memory().percent
            },
            "disk": {
                "total": psutil.disk_usage('/').total,
                "free": psutil.disk_usage('/').free,
                "percent": psutil.disk_usage('/').percent
            },
            "active_connections": len(websocket_manager.active_connections),
            "kafka_consumers": len(kafka_manager.consumers),
            "loaded_models": len(ml_manager.models),
            "timestamp": datetime.now().isoformat()
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"System stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))