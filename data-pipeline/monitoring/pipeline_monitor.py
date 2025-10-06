#!/usr/bin/env python3
"""
Advanced Pipeline Monitoring and Alerting System
Real-time monitoring of data pipeline health, performance, and data quality
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum
import threading
import asyncio
from pathlib import Path

# Database connections
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Monitoring and metrics
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, push_to_gateway
import psutil
import requests

# Alerting
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Data processing
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning" 
    ERROR = "error"
    CRITICAL = "critical"

class ComponentStatus(str, Enum):
    """Component health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class Alert:
    """Alert data structure"""
    alert_id: str
    component: str
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime
    metadata: Dict[str, Any]
    resolved: bool = False
    resolved_at: Optional[datetime] = None

@dataclass
class HealthCheck:
    """Health check configuration"""
    name: str
    check_function: Callable
    interval_seconds: int
    timeout_seconds: int = 30
    critical: bool = False
    enabled: bool = True

class PipelineMonitor:
    """
    Comprehensive pipeline monitoring system with real-time health checks,
    metrics collection, and intelligent alerting
    """
    
    def __init__(self, config_path: str = "/app/config/monitoring.json"):
        self.config = self._load_config(config_path)
        self.running = False
        self.alerts = {}
        self.health_status = {}
        self.metrics_registry = CollectorRegistry()
        
        # Database connections
        self.postgres_engine = None
        self.mongo_client = None
        self.redis_client = None
        
        # Initialize connections
        self._init_connections()
        
        # Initialize metrics
        self._init_metrics()
        
        # Initialize health checks
        self.health_checks = self._init_health_checks()
        
        # Alert channels
        self.alert_channels = self._init_alert_channels()
        
        logger.info("🔍 Pipeline Monitor initialized")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load monitoring configuration"""
        
        default_config = {
            "postgres_url": os.getenv("DATABASE_URL", "postgresql://admin:admin123@postgres:5432/dss_analytics"),
            "mongodb_url": os.getenv("MONGODB_URL", "mongodb://root:admin123@mongodb:27017/"),
            "redis_url": os.getenv("REDIS_URL", "redis://redis:6379/0"),
            "kafka_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            
            "monitoring": {
                "check_interval": 60,  # seconds
                "metrics_push_interval": 30,
                "alert_cooldown": 300,  # 5 minutes
                "health_check_timeout": 30
            },
            
            "alerting": {
                "email": {
                    "enabled": True,
                    "smtp_server": os.getenv("SMTP_SERVER", "localhost"),
                    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
                    "username": os.getenv("SMTP_USERNAME", ""),
                    "password": os.getenv("SMTP_PASSWORD", ""),
                    "from_email": os.getenv("FROM_EMAIL", "alerts@company.com"),
                    "to_emails": os.getenv("ALERT_EMAILS", "admin@company.com").split(",")
                },
                "slack": {
                    "enabled": bool(os.getenv("SLACK_TOKEN")),
                    "token": os.getenv("SLACK_TOKEN", ""),
                    "channel": os.getenv("SLACK_CHANNEL", "#alerts"),
                    "username": "Pipeline Monitor"
                },
                "webhook": {
                    "enabled": bool(os.getenv("WEBHOOK_URL")),
                    "url": os.getenv("WEBHOOK_URL", ""),
                    "timeout": 10
                }
            },
            
            "thresholds": {
                "cpu_usage": 80.0,
                "memory_usage": 85.0,
                "disk_usage": 90.0,
                "kafka_lag": 1000,
                "database_connections": 80,
                "api_response_time": 2.0,
                "data_quality_score": 0.95,
                "pipeline_failure_rate": 0.05
            }
        }
        
        try:
            if Path(config_path).exists():
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                    # Merge with defaults
                    default_config.update(file_config)
        except Exception as e:
            logger.warning(f"⚠️ Failed to load config from {config_path}: {e}")
        
        return default_config

    def _init_connections(self):
        """Initialize database connections"""
        
        try:
            # PostgreSQL
            self.postgres_engine = create_engine(self.config["postgres_url"])
            logger.info("✅ PostgreSQL connection initialized")
            
            # MongoDB
            self.mongo_client = MongoClient(self.config["mongodb_url"])
            logger.info("✅ MongoDB connection initialized")
            
            # Redis
            self.redis_client = redis.from_url(self.config["redis_url"])
            logger.info("✅ Redis connection initialized")
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")

    def _init_metrics(self):
        """Initialize Prometheus metrics"""
        
        # System metrics
        self.cpu_usage_gauge = Gauge(
            'pipeline_cpu_usage_percent', 
            'CPU usage percentage',
            registry=self.metrics_registry
        )
        
        self.memory_usage_gauge = Gauge(
            'pipeline_memory_usage_percent', 
            'Memory usage percentage',
            registry=self.metrics_registry
        )
        
        self.disk_usage_gauge = Gauge(
            'pipeline_disk_usage_percent', 
            'Disk usage percentage',
            registry=self.metrics_registry
        )
        
        # Pipeline metrics
        self.pipeline_runs_counter = Counter(
            'pipeline_runs_total',
            'Total pipeline runs',
            ['status', 'dag_id'],
            registry=self.metrics_registry
        )
        
        self.pipeline_duration_histogram = Histogram(
            'pipeline_duration_seconds',
            'Pipeline execution duration',
            ['dag_id'],
            registry=self.metrics_registry
        )
        
        # Data quality metrics
        self.data_quality_score_gauge = Gauge(
            'data_quality_score',
            'Data quality score',
            ['table', 'check_type'],
            registry=self.metrics_registry
        )
        
        # Kafka metrics
        self.kafka_lag_gauge = Gauge(
            'kafka_consumer_lag',
            'Kafka consumer lag',
            ['topic', 'consumer_group'],
            registry=self.metrics_registry
        )
        
        # Database metrics
        self.db_connections_gauge = Gauge(
            'database_active_connections',
            'Active database connections',
            ['database'],
            registry=self.metrics_registry
        )
        
        self.api_response_time_histogram = Histogram(
            'api_response_time_seconds',
            'API response time',
            ['endpoint', 'method'],
            registry=self.metrics_registry
        )
        
        logger.info("✅ Metrics initialized")

    def _init_health_checks(self) -> List[HealthCheck]:
        """Initialize health check configurations"""
        
        return [
            HealthCheck(
                name="postgres_health",
                check_function=self._check_postgres_health,
                interval_seconds=60,
                critical=True
            ),
            HealthCheck(
                name="mongodb_health", 
                check_function=self._check_mongodb_health,
                interval_seconds=60,
                critical=True
            ),
            HealthCheck(
                name="redis_health",
                check_function=self._check_redis_health,
                interval_seconds=60,
                critical=True
            ),
            HealthCheck(
                name="kafka_health",
                check_function=self._check_kafka_health,
                interval_seconds=60,
                critical=True
            ),
            HealthCheck(
                name="api_health",
                check_function=self._check_api_health,
                interval_seconds=120,
                critical=False
            ),
            HealthCheck(
                name="system_resources",
                check_function=self._check_system_resources,
                interval_seconds=30,
                critical=False
            ),
            HealthCheck(
                name="data_freshness",
                check_function=self._check_data_freshness,
                interval_seconds=300,  # 5 minutes
                critical=False
            ),
            HealthCheck(
                name="pipeline_health",
                check_function=self._check_pipeline_health,
                interval_seconds=180,  # 3 minutes
                critical=False
            )
        ]

    def _init_alert_channels(self) -> Dict[str, Any]:
        """Initialize alert channels"""
        
        channels = {}
        
        # Email alerting
        if self.config["alerting"]["email"]["enabled"]:
            channels["email"] = {
                "type": "email",
                "config": self.config["alerting"]["email"]
            }
        
        # Slack alerting
        if self.config["alerting"]["slack"]["enabled"]:
            channels["slack"] = {
                "type": "slack",
                "client": WebClient(token=self.config["alerting"]["slack"]["token"]),
                "config": self.config["alerting"]["slack"]
            }
        
        # Webhook alerting
        if self.config["alerting"]["webhook"]["enabled"]:
            channels["webhook"] = {
                "type": "webhook",
                "config": self.config["alerting"]["webhook"]
            }
        
        logger.info(f"✅ Alert channels initialized: {list(channels.keys())}")
        return channels

    def start_monitoring(self):
        """Start the monitoring system"""
        
        logger.info("🚀 Starting pipeline monitoring...")
        self.running = True
        
        # Start health check threads
        for health_check in self.health_checks:
            if health_check.enabled:
                thread = threading.Thread(
                    target=self._run_health_check_loop,
                    args=(health_check,),
                    daemon=True
                )
                thread.start()
                logger.info(f"📊 Started health check: {health_check.name}")
        
        # Start metrics collection thread
        metrics_thread = threading.Thread(
            target=self._metrics_collection_loop,
            daemon=True
        )
        metrics_thread.start()
        logger.info("📈 Started metrics collection")
        
        # Start alert processing thread
        alerts_thread = threading.Thread(
            target=self._alert_processing_loop,
            daemon=True
        )
        alerts_thread.start()
        logger.info("🚨 Started alert processing")

    def stop_monitoring(self):
        """Stop the monitoring system"""
        
        logger.info("🛑 Stopping pipeline monitoring...")
        self.running = False

    def _run_health_check_loop(self, health_check: HealthCheck):
        """Run a single health check in a loop"""
        
        while self.running:
            try:
                start_time = time.time()
                result = health_check.check_function()
                duration = time.time() - start_time
                
                # Update health status
                self.health_status[health_check.name] = {
                    'status': result['status'],
                    'message': result.get('message', ''),
                    'last_check': datetime.now(),
                    'check_duration': duration,
                    'critical': health_check.critical
                }
                
                # Generate alerts if unhealthy
                if result['status'] != ComponentStatus.HEALTHY:
                    self._generate_alert(
                        component=health_check.name,
                        severity=AlertSeverity.CRITICAL if health_check.critical else AlertSeverity.WARNING,
                        title=f"Health check failed: {health_check.name}",
                        message=result.get('message', 'Health check failed'),
                        metadata=result.get('metadata', {})
                    )
                
            except Exception as e:
                logger.error(f"❌ Health check {health_check.name} failed: {e}")
                self.health_status[health_check.name] = {
                    'status': ComponentStatus.UNKNOWN,
                    'message': f'Check failed: {e}',
                    'last_check': datetime.now(),
                    'critical': health_check.critical
                }
            
            time.sleep(health_check.interval_seconds)

    def _check_postgres_health(self) -> Dict[str, Any]:
        """Check PostgreSQL database health"""
        
        try:
            with self.postgres_engine.connect() as conn:
                # Test connection
                result = conn.execute(text("SELECT 1")).fetchone()
                
                # Check active connections
                conn_result = conn.execute(text("""
                    SELECT count(*) as active_connections
                    FROM pg_stat_activity 
                    WHERE state = 'active'
                """)).fetchone()
                
                active_connections = conn_result.active_connections
                max_connections = self.config["thresholds"]["database_connections"]
                
                if active_connections > max_connections:
                    return {
                        'status': ComponentStatus.DEGRADED,
                        'message': f'High connection count: {active_connections}',
                        'metadata': {'active_connections': active_connections}
                    }
                
                # Update metrics
                self.db_connections_gauge.labels(database='postgres').set(active_connections)
                
                return {
                    'status': ComponentStatus.HEALTHY,
                    'message': 'Database responding normally',
                    'metadata': {'active_connections': active_connections}
                }
                
        except Exception as e:
            return {
                'status': ComponentStatus.UNHEALTHY,
                'message': f'Database connection failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_mongodb_health(self) -> Dict[str, Any]:
        """Check MongoDB health"""
        
        try:
            # Test connection
            self.mongo_client.admin.command('ping')
            
            # Get server status
            status = self.mongo_client.admin.command('serverStatus')
            
            connections = status.get('connections', {})
            current_connections = connections.get('current', 0)
            max_connections = self.config["thresholds"]["database_connections"]
            
            if current_connections > max_connections:
                return {
                    'status': ComponentStatus.DEGRADED,
                    'message': f'High connection count: {current_connections}',
                    'metadata': {'connections': connections}
                }
            
            # Update metrics
            self.db_connections_gauge.labels(database='mongodb').set(current_connections)
            
            return {
                'status': ComponentStatus.HEALTHY,
                'message': 'MongoDB responding normally',
                'metadata': {'connections': connections}
            }
            
        except Exception as e:
            return {
                'status': ComponentStatus.UNHEALTHY,
                'message': f'MongoDB connection failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_redis_health(self) -> Dict[str, Any]:
        """Check Redis health"""
        
        try:
            # Test connection
            self.redis_client.ping()
            
            # Get info
            info = self.redis_client.info()
            
            memory_usage = info.get('used_memory', 0)
            max_memory = info.get('maxmemory', 0)
            
            if max_memory > 0:
                memory_percent = (memory_usage / max_memory) * 100
                if memory_percent > 90:
                    return {
                        'status': ComponentStatus.DEGRADED,
                        'message': f'High memory usage: {memory_percent:.1f}%',
                        'metadata': {'memory_usage_percent': memory_percent}
                    }
            
            return {
                'status': ComponentStatus.HEALTHY,
                'message': 'Redis responding normally',
                'metadata': {'memory_usage': memory_usage}
            }
            
        except Exception as e:
            return {
                'status': ComponentStatus.UNHEALTHY,
                'message': f'Redis connection failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_kafka_health(self) -> Dict[str, Any]:
        """Check Kafka health"""
        
        try:
            # Create consumer to test connection
            consumer = KafkaConsumer(
                bootstrap_servers=self.config["kafka_servers"],
                consumer_timeout_ms=5000
            )
            
            # Get cluster metadata
            metadata = consumer.list_consumer_groups()
            
            # Check for high lag (simplified check)
            lag_threshold = self.config["thresholds"]["kafka_lag"]
            
            consumer.close()
            
            return {
                'status': ComponentStatus.HEALTHY,
                'message': 'Kafka cluster responding normally',
                'metadata': {'consumer_groups': len(metadata)}
            }
            
        except Exception as e:
            return {
                'status': ComponentStatus.UNHEALTHY,
                'message': f'Kafka connection failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_api_health(self) -> Dict[str, Any]:
        """Check API health"""
        
        try:
            # Check main API endpoint
            api_url = os.getenv("API_BASE_URL", "http://backend:8000")
            
            start_time = time.time()
            response = requests.get(f"{api_url}/health", timeout=10)
            response_time = time.time() - start_time
            
            # Update metrics
            self.api_response_time_histogram.labels(
                endpoint="/health",
                method="GET"
            ).observe(response_time)
            
            if response.status_code == 200:
                threshold = self.config["thresholds"]["api_response_time"]
                
                if response_time > threshold:
                    return {
                        'status': ComponentStatus.DEGRADED,
                        'message': f'Slow API response: {response_time:.2f}s',
                        'metadata': {'response_time': response_time}
                    }
                
                return {
                    'status': ComponentStatus.HEALTHY,
                    'message': f'API responding normally ({response_time:.2f}s)',
                    'metadata': {'response_time': response_time}
                }
            else:
                return {
                    'status': ComponentStatus.UNHEALTHY,
                    'message': f'API returned status {response.status_code}',
                    'metadata': {'status_code': response.status_code}
                }
                
        except Exception as e:
            return {
                'status': ComponentStatus.UNHEALTHY,
                'message': f'API health check failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage"""
        
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.cpu_usage_gauge.set(cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            self.memory_usage_gauge.set(memory_percent)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.disk_usage_gauge.set(disk_percent)
            
            # Check thresholds
            issues = []
            
            if cpu_percent > self.config["thresholds"]["cpu_usage"]:
                issues.append(f"High CPU usage: {cpu_percent:.1f}%")
            
            if memory_percent > self.config["thresholds"]["memory_usage"]:
                issues.append(f"High memory usage: {memory_percent:.1f}%")
            
            if disk_percent > self.config["thresholds"]["disk_usage"]:
                issues.append(f"High disk usage: {disk_percent:.1f}%")
            
            if issues:
                return {
                    'status': ComponentStatus.DEGRADED,
                    'message': '; '.join(issues),
                    'metadata': {
                        'cpu_percent': cpu_percent,
                        'memory_percent': memory_percent,
                        'disk_percent': disk_percent
                    }
                }
            
            return {
                'status': ComponentStatus.HEALTHY,
                'message': 'System resources normal',
                'metadata': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory_percent,
                    'disk_percent': disk_percent
                }
            }
            
        except Exception as e:
            return {
                'status': ComponentStatus.UNKNOWN,
                'message': f'Resource check failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness across pipeline"""
        
        try:
            current_time = datetime.now()
            freshness_issues = []
            
            # Check PostgreSQL data freshness
            with self.postgres_engine.connect() as conn:
                tables_to_check = [
                    'products_processed',
                    'customers_processed', 
                    'orders_processed'
                ]
                
                for table in tables_to_check:
                    try:
                        result = conn.execute(text(f"""
                            SELECT MAX(processed_at) as latest_record
                            FROM {table}
                        """)).fetchone()
                        
                        if result.latest_record:
                            time_diff = current_time - result.latest_record
                            
                            # Data should be fresher than 2 hours
                            if time_diff > timedelta(hours=2):
                                freshness_issues.append(
                                    f"{table}: {time_diff.total_seconds()/3600:.1f}h old"
                                )
                    except Exception as table_error:
                        freshness_issues.append(f"{table}: check failed")
            
            # Check MongoDB streaming data
            try:
                db = self.mongo_client.dss_streaming
                
                streaming_collections = ['products_raw', 'customers_raw', 'orders_raw']
                
                for collection_name in streaming_collections:
                    collection = db[collection_name]
                    
                    latest_doc = collection.find_one(
                        sort=[('timestamp', -1)]
                    )
                    
                    if latest_doc and 'timestamp' in latest_doc:
                        latest_time = latest_doc['timestamp']
                        if isinstance(latest_time, str):
                            latest_time = datetime.fromisoformat(latest_time.replace('Z', '+00:00'))
                        
                        time_diff = current_time - latest_time.replace(tzinfo=None)
                        
                        # Streaming data should be fresher than 30 minutes
                        if time_diff > timedelta(minutes=30):
                            freshness_issues.append(
                                f"{collection_name}: {time_diff.total_seconds()/60:.1f}m old"
                            )
            
            except Exception as mongo_error:
                freshness_issues.append(f"MongoDB check failed: {mongo_error}")
            
            if freshness_issues:
                return {
                    'status': ComponentStatus.DEGRADED,
                    'message': f"Stale data detected: {'; '.join(freshness_issues)}",
                    'metadata': {'issues': freshness_issues}
                }
            
            return {
                'status': ComponentStatus.HEALTHY,
                'message': 'All data streams are fresh',
                'metadata': {}
            }
            
        except Exception as e:
            return {
                'status': ComponentStatus.UNKNOWN,
                'message': f'Data freshness check failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _check_pipeline_health(self) -> Dict[str, Any]:
        """Check overall pipeline health"""
        
        try:
            # Get recent pipeline runs from database
            with self.postgres_engine.connect() as conn:
                # Check for recent DAG failures
                recent_failures = conn.execute(text("""
                    SELECT COUNT(*) as failure_count
                    FROM dag_run_history 
                    WHERE created_at >= NOW() - INTERVAL '1 hour'
                    AND status = 'failed'
                """)).fetchone()
                
                total_runs = conn.execute(text("""
                    SELECT COUNT(*) as total_count
                    FROM dag_run_history 
                    WHERE created_at >= NOW() - INTERVAL '1 hour'
                """)).fetchone()
                
                failure_count = recent_failures.failure_count or 0
                total_count = total_runs.total_count or 0
                
                if total_count > 0:
                    failure_rate = failure_count / total_count
                    threshold = self.config["thresholds"]["pipeline_failure_rate"]
                    
                    if failure_rate > threshold:
                        return {
                            'status': ComponentStatus.DEGRADED,
                            'message': f'High pipeline failure rate: {failure_rate:.1%}',
                            'metadata': {
                                'failure_rate': failure_rate,
                                'failed_runs': failure_count,
                                'total_runs': total_count
                            }
                        }
                
                return {
                    'status': ComponentStatus.HEALTHY,
                    'message': 'Pipeline runs healthy',
                    'metadata': {
                        'failure_rate': failure_count / max(total_count, 1),
                        'failed_runs': failure_count,
                        'total_runs': total_count
                    }
                }
                
        except Exception as e:
            return {
                'status': ComponentStatus.UNKNOWN,
                'message': f'Pipeline health check failed: {e}',
                'metadata': {'error': str(e)}
            }

    def _metrics_collection_loop(self):
        """Collect and push metrics periodically"""
        
        push_gateway = os.getenv("PROMETHEUS_PUSHGATEWAY", "")
        push_interval = self.config["monitoring"]["metrics_push_interval"]
        
        while self.running:
            try:
                if push_gateway:
                    # Push metrics to Prometheus Gateway
                    push_to_gateway(
                        gateway=push_gateway,
                        job='pipeline_monitor',
                        registry=self.metrics_registry
                    )
                    logger.debug("📊 Metrics pushed to Prometheus")
                
                # Store metrics in database
                self._store_metrics_snapshot()
                
            except Exception as e:
                logger.error(f"❌ Metrics collection failed: {e}")
            
            time.sleep(push_interval)

    def _store_metrics_snapshot(self):
        """Store current metrics snapshot in database"""
        
        try:
            metrics_snapshot = {
                'timestamp': datetime.now(),
                'system_metrics': {
                    'cpu_usage': psutil.cpu_percent(),
                    'memory_usage': psutil.virtual_memory().percent,
                    'disk_usage': (psutil.disk_usage('/').used / psutil.disk_usage('/').total) * 100
                },
                'health_status': self.health_status,
                'alert_count': len([a for a in self.alerts.values() if not a.resolved])
            }
            
            # Store in MongoDB for historical analysis
            if self.mongo_client:
                db = self.mongo_client.monitoring
                db.metrics_snapshots.insert_one(metrics_snapshot)
            
            # Store summary in Redis for quick access
            if self.redis_client:
                self.redis_client.setex(
                    "monitoring:latest_snapshot",
                    timedelta(hours=1),
                    json.dumps(metrics_snapshot, default=str)
                )
            
        except Exception as e:
            logger.error(f"❌ Failed to store metrics snapshot: {e}")

    def _alert_processing_loop(self):
        """Process and send alerts"""
        
        cooldown_period = self.config["monitoring"]["alert_cooldown"]
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Process unresolved alerts
                for alert_id, alert in self.alerts.items():
                    if not alert.resolved:
                        # Check if alert should be sent (cooldown logic)
                        time_since_alert = (current_time - alert.timestamp).total_seconds()
                        
                        if time_since_alert > cooldown_period:
                            self._send_alert(alert)
                
                # Clean up old resolved alerts
                self._cleanup_old_alerts()
                
            except Exception as e:
                logger.error(f"❌ Alert processing failed: {e}")
            
            time.sleep(60)  # Check every minute

    def _generate_alert(self, component: str, severity: AlertSeverity, 
                       title: str, message: str, metadata: Dict[str, Any]):
        """Generate a new alert"""
        
        alert_id = f"{component}_{severity.value}_{int(time.time())}"
        
        # Check if similar alert already exists
        existing_alert = None
        for alert in self.alerts.values():
            if (alert.component == component and 
                alert.severity == severity and 
                not alert.resolved):
                existing_alert = alert
                break
        
        if existing_alert:
            # Update existing alert timestamp
            existing_alert.timestamp = datetime.now()
            existing_alert.metadata.update(metadata)
            logger.debug(f"📝 Updated existing alert: {alert_id}")
        else:
            # Create new alert
            alert = Alert(
                alert_id=alert_id,
                component=component,
                severity=severity,
                title=title,
                message=message,
                timestamp=datetime.now(),
                metadata=metadata
            )
            
            self.alerts[alert_id] = alert
            logger.warning(f"🚨 Alert generated: {title}")

    def _send_alert(self, alert: Alert):
        """Send alert through configured channels"""
        
        for channel_name, channel in self.alert_channels.items():
            try:
                if channel["type"] == "email":
                    self._send_email_alert(alert, channel["config"])
                elif channel["type"] == "slack":
                    self._send_slack_alert(alert, channel["client"], channel["config"])
                elif channel["type"] == "webhook":
                    self._send_webhook_alert(alert, channel["config"])
                    
            except Exception as e:
                logger.error(f"❌ Failed to send alert via {channel_name}: {e}")

    def _send_email_alert(self, alert: Alert, config: Dict[str, Any]):
        """Send alert via email"""
        
        try:
            msg = MimeMultipart()
            msg['From'] = config["from_email"]
            msg['To'] = ", ".join(config["to_emails"])
            msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.title}"
            
            body = f"""
            Alert Details:
            
            Component: {alert.component}
            Severity: {alert.severity.value.upper()}
            Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
            
            Message: {alert.message}
            
            Metadata: {json.dumps(alert.metadata, indent=2)}
            
            ---
            Pipeline Monitoring System
            """
            
            msg.attach(MimeText(body, 'plain'))
            
            with smtplib.SMTP(config["smtp_server"], config["smtp_port"]) as server:
                if config["username"]:
                    server.starttls()
                    server.login(config["username"], config["password"])
                server.send_message(msg)
            
            logger.info(f"📧 Email alert sent: {alert.alert_id}")
            
        except Exception as e:
            logger.error(f"❌ Email alert failed: {e}")

    def _send_slack_alert(self, alert: Alert, client: WebClient, config: Dict[str, Any]):
        """Send alert via Slack"""
        
        try:
            severity_colors = {
                AlertSeverity.INFO: "#36a64f",
                AlertSeverity.WARNING: "#ff9500", 
                AlertSeverity.ERROR: "#ff0000",
                AlertSeverity.CRITICAL: "#8B0000"
            }
            
            severity_emojis = {
                AlertSeverity.INFO: "ℹ️",
                AlertSeverity.WARNING: "⚠️",
                AlertSeverity.ERROR: "❌", 
                AlertSeverity.CRITICAL: "🚨"
            }
            
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{severity_emojis[alert.severity]} *{alert.title}*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Component:*\n{alert.component}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Severity:*\n{alert.severity.value.upper()}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Time:*\n{alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Message:*\n{alert.message}"
                    }
                }
            ]
            
            response = client.chat_postMessage(
                channel=config["channel"],
                username=config["username"],
                blocks=blocks,
                attachments=[{
                    "color": severity_colors[alert.severity],
                    "text": ""
                }]
            )
            
            logger.info(f"📱 Slack alert sent: {alert.alert_id}")
            
        except SlackApiError as e:
            logger.error(f"❌ Slack alert failed: {e.response['error']}")

    def _send_webhook_alert(self, alert: Alert, config: Dict[str, Any]):
        """Send alert via webhook"""
        
        try:
            payload = {
                "alert_id": alert.alert_id,
                "component": alert.component,
                "severity": alert.severity.value,
                "title": alert.title,
                "message": alert.message,
                "timestamp": alert.timestamp.isoformat(),
                "metadata": alert.metadata
            }
            
            response = requests.post(
                config["url"],
                json=payload,
                timeout=config["timeout"]
            )
            
            response.raise_for_status()
            logger.info(f"🔗 Webhook alert sent: {alert.alert_id}")
            
        except Exception as e:
            logger.error(f"❌ Webhook alert failed: {e}")

    def _cleanup_old_alerts(self):
        """Clean up old resolved alerts"""
        
        cutoff_time = datetime.now() - timedelta(days=7)
        
        alerts_to_remove = []
        for alert_id, alert in self.alerts.items():
            if alert.resolved and alert.resolved_at and alert.resolved_at < cutoff_time:
                alerts_to_remove.append(alert_id)
        
        for alert_id in alerts_to_remove:
            del self.alerts[alert_id]
        
        if alerts_to_remove:
            logger.info(f"🧹 Cleaned up {len(alerts_to_remove)} old alerts")

    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        
        overall_status = ComponentStatus.HEALTHY
        critical_issues = []
        warnings = []
        
        for component, status_info in self.health_status.items():
            if status_info['critical'] and status_info['status'] != ComponentStatus.HEALTHY:
                overall_status = ComponentStatus.UNHEALTHY
                critical_issues.append(f"{component}: {status_info['message']}")
            elif status_info['status'] == ComponentStatus.DEGRADED:
                if overall_status == ComponentStatus.HEALTHY:
                    overall_status = ComponentStatus.DEGRADED
                warnings.append(f"{component}: {status_info['message']}")
        
        return {
            'overall_status': overall_status.value,
            'components': self.health_status,
            'critical_issues': critical_issues,
            'warnings': warnings,
            'active_alerts': len([a for a in self.alerts.values() if not a.resolved]),
            'last_updated': datetime.now().isoformat()
        }

    def resolve_alert(self, alert_id: str) -> bool:
        """Manually resolve an alert"""
        
        if alert_id in self.alerts:
            alert = self.alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.now()
            logger.info(f"✅ Alert resolved: {alert_id}")
            return True
        
        return False

# Main execution
if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.start_monitoring()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(60)
            status = monitor.get_system_status()
            logger.info(f"System Status: {status['overall_status']}")
            
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down monitoring...")
        monitor.stop_monitoring()