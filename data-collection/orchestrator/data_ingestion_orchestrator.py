#!/usr/bin/env python3
"""
Data Ingestion Pipeline Orchestrator
Coordinates and manages all data collection sources for the DSS e-commerce system
"""

import asyncio
import json
import logging
import signal
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
import schedule
from concurrent.futures import ThreadPoolExecutor

# Import our data collection modules
sys.path.append(str(Path(__file__).parent.parent))

from scrapers.ecommerce_crawler import EcommerceWebCrawler
from apis.api_collector import MultiSourceAPICollector
from generators.realtime_data_generator import RealtimeDataGenerator

from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataIngestionOrchestrator:
    """
    Master orchestrator for all data collection activities
    Manages scheduling, coordination, and monitoring of data sources
    """

    def __init__(self, config_path: str = "config/data_sources.json"):
        self.load_config(config_path)
        self.setup_connections()
        self.initialize_collectors()

        # Orchestrator state
        self.running = False
        self.collection_tasks = {}
        self.performance_metrics = {}

        # Schedule configuration
        self.schedules = {
            'real_time': 'continuous',
            'web_crawling': 'hourly',
            'api_collection': 'every_30_minutes',
            'health_checks': 'every_5_minutes',
            'performance_monitoring': 'every_minute'
        }

    def load_config(self, config_path: str):
        """Load orchestrator configuration"""
        try:
            config_file = Path(__file__).parent.parent / config_path
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                logger.info("✅ Orchestrator configuration loaded")
        except Exception as e:
            logger.error(f"❌ Failed to load config: {e}")
            raise

    def setup_connections(self):
        """Setup connections for monitoring and coordination"""
        try:
            # Kafka for inter-service communication
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
            )

            # MongoDB for metadata storage
            self.mongo_client = MongoClient('mongodb://localhost:27017/')
            self.mongo_db = self.mongo_client.dss_orchestrator

            # Redis for real-time coordination and caching
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

            logger.info("✅ Orchestrator connections established")

        except Exception as e:
            logger.error(f"❌ Failed to setup orchestrator connections: {e}")
            raise

    def initialize_collectors(self):
        """Initialize all data collectors"""
        try:
            # Web crawler
            self.web_crawler = EcommerceWebCrawler()

            # API collector
            self.api_collector = MultiSourceAPICollector()

            # Real-time data generator
            self.realtime_generator = RealtimeDataGenerator()

            logger.info("✅ All data collectors initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize collectors: {e}")
            raise

    async def run_web_crawling_session(self):
        """Execute web crawling session"""
        try:
            logger.info("🕷️ Starting web crawling session...")

            start_time = datetime.now()
            result = await self.web_crawler.run_crawling_session()
            end_time = datetime.now()

            duration = (end_time - start_time).total_seconds()

            # Record performance metrics
            metrics = {
                'session_type': 'web_crawling',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'success': result.get('success', False),
                'records_collected': result.get('total_products', 0),
                'sources': result.get('sources', [])
            }

            # Store metrics
            self.mongo_db.collection_metrics.insert_one(metrics)
            self.redis_client.setex(
                'last_web_crawling_session',
                3600,  # 1 hour TTL
                json.dumps(metrics)
            )

            # Send notification to Kafka
            self.kafka_producer.send('collection_events', {
                'event_type': 'web_crawling_completed',
                'metrics': metrics,
                'timestamp': datetime.now().isoformat()
            })

            logger.info(f"✅ Web crawling session completed: {metrics}")
            return metrics

        except Exception as e:
            logger.error(f"❌ Web crawling session failed: {e}")
            return {'success': False, 'error': str(e)}

    async def run_api_collection_session(self):
        """Execute API collection session"""
        try:
            logger.info("🔗 Starting API collection session...")

            start_time = datetime.now()
            result = await self.api_collector.run_api_collection_session()
            end_time = datetime.now()

            duration = (end_time - start_time).total_seconds()

            # Record performance metrics
            metrics = {
                'session_type': 'api_collection',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'success': result.get('success', False),
                'total_records': result.get('total_records', 0),
                'collections': result.get('collections', {})
            }

            # Store metrics
            self.mongo_db.collection_metrics.insert_one(metrics)
            self.redis_client.setex(
                'last_api_collection_session',
                1800,  # 30 minutes TTL
                json.dumps(metrics)
            )

            # Send notification to Kafka
            self.kafka_producer.send('collection_events', {
                'event_type': 'api_collection_completed',
                'metrics': metrics,
                'timestamp': datetime.now().isoformat()
            })

            logger.info(f"✅ API collection session completed: {metrics}")
            return metrics

        except Exception as e:
            logger.error(f"❌ API collection session failed: {e}")
            return {'success': False, 'error': str(e)}

    async def start_realtime_generation(self):
        """Start real-time data generation"""
        try:
            logger.info("⚡ Starting real-time data generation...")

            # Start the real-time generator in background
            self.collection_tasks['realtime'] = asyncio.create_task(
                self.realtime_generator.start_generation()
            )

            # Monitor the task
            return True

        except Exception as e:
            logger.error(f"❌ Failed to start real-time generation: {e}")
            return False

    async def monitor_system_health(self):
        """Monitor overall system health and performance"""
        try:
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'services': {},
                'overall_status': 'healthy'
            }

            # Check Kafka connectivity
            try:
                self.kafka_producer.send('health_check', {'test': 'ping'})
                health_status['services']['kafka'] = 'healthy'
            except Exception as e:
                health_status['services']['kafka'] = f'unhealthy: {str(e)}'
                health_status['overall_status'] = 'degraded'

            # Check MongoDB connectivity
            try:
                self.mongo_client.admin.command('ping')
                health_status['services']['mongodb'] = 'healthy'
            except Exception as e:
                health_status['services']['mongodb'] = f'unhealthy: {str(e)}'
                health_status['overall_status'] = 'degraded'

            # Check Redis connectivity
            try:
                self.redis_client.ping()
                health_status['services']['redis'] = 'healthy'
            except Exception as e:
                health_status['services']['redis'] = f'unhealthy: {str(e)}'
                health_status['overall_status'] = 'degraded'

            # Check data flow rates
            try:
                realtime_stats = self.redis_client.get('realtime_generator_stats')
                if realtime_stats:
                    stats = json.loads(realtime_stats)
                    health_status['data_flow_rates'] = stats.get('current_rates', {})
                else:
                    health_status['data_flow_rates'] = 'no_data'
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch data flow rates: {e}")

            # Store health status
            self.redis_client.setex(
                'system_health_status',
                300,  # 5 minutes TTL
                json.dumps(health_status)
            )

            # Log health status
            if health_status['overall_status'] == 'healthy':
                logger.info(f"💚 System health check: {health_status['overall_status']}")
            else:
                logger.warning(f"⚠️ System health check: {health_status}")

            return health_status

        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return {'overall_status': 'unhealthy', 'error': str(e)}

    async def collect_performance_metrics(self):
        """Collect and analyze performance metrics"""
        try:
            # Get recent collection metrics from MongoDB
            recent_metrics = list(
                self.mongo_db.collection_metrics.find(
                    {'start_time': {'$gte': (datetime.now() - timedelta(hours=1)).isoformat()}},
                    sort=[('start_time', -1)]
                )
            )

            if not recent_metrics:
                logger.info("📊 No recent metrics available")
                return

            # Calculate performance statistics
            web_crawling_sessions = [m for m in recent_metrics if m['session_type'] == 'web_crawling']
            api_collection_sessions = [m for m in recent_metrics if m['session_type'] == 'api_collection']

            performance_summary = {
                'timestamp': datetime.now().isoformat(),
                'period': 'last_hour',
                'web_crawling': {
                    'sessions_count': len(web_crawling_sessions),
                    'avg_duration': sum(s.get('duration_seconds', 0) for s in web_crawling_sessions) / len(web_crawling_sessions) if web_crawling_sessions else 0,
                    'total_records': sum(s.get('records_collected', 0) for s in web_crawling_sessions),
                    'success_rate': sum(1 for s in web_crawling_sessions if s.get('success', False)) / len(web_crawling_sessions) if web_crawling_sessions else 0
                },
                'api_collection': {
                    'sessions_count': len(api_collection_sessions),
                    'avg_duration': sum(s.get('duration_seconds', 0) for s in api_collection_sessions) / len(api_collection_sessions) if api_collection_sessions else 0,
                    'total_records': sum(s.get('total_records', 0) for s in api_collection_sessions),
                    'success_rate': sum(1 for s in api_collection_sessions if s.get('success', False)) / len(api_collection_sessions) if api_collection_sessions else 0
                }
            }

            # Store performance summary
            self.mongo_db.performance_summaries.insert_one(performance_summary)
            self.redis_client.setex(
                'performance_summary',
                300,  # 5 minutes TTL
                json.dumps(performance_summary, default=str)
            )

            logger.info(f"📈 Performance metrics updated: {performance_summary}")

        except Exception as e:
            logger.error(f"❌ Error collecting performance metrics: {e}")

    def setup_schedules(self):
        """Setup scheduled tasks"""
        try:
            # Web crawling every hour
            schedule.every().hour.do(
                lambda: asyncio.create_task(self.run_web_crawling_session())
            )

            # API collection every 30 minutes
            schedule.every(30).minutes.do(
                lambda: asyncio.create_task(self.run_api_collection_session())
            )

            # Health checks every 5 minutes
            schedule.every(5).minutes.do(
                lambda: asyncio.create_task(self.monitor_system_health())
            )

            # Performance monitoring every minute
            schedule.every(1).minutes.do(
                lambda: asyncio.create_task(self.collect_performance_metrics())
            )

            logger.info("⏰ Scheduled tasks configured")

        except Exception as e:
            logger.error(f"❌ Failed to setup schedules: {e}")

    async def run_scheduled_tasks(self):
        """Run scheduled tasks in background"""
        while self.running:
            try:
                schedule.run_pending()
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"❌ Error in scheduled tasks: {e}")
                await asyncio.sleep(60)

    async def start_orchestration(self):
        """Start the complete data ingestion orchestration"""
        logger.info("🚀 Starting Data Ingestion Orchestration...")

        self.running = True

        try:
            # Setup schedules
            self.setup_schedules()

            # Start real-time data generation
            await self.start_realtime_generation()

            # Start initial collections
            await self.run_web_crawling_session()
            await self.run_api_collection_session()

            # Start health monitoring
            await self.monitor_system_health()

            # Start scheduled task runner
            scheduled_task = asyncio.create_task(self.run_scheduled_tasks())

            # Create orchestration summary
            orchestration_status = {
                'start_time': datetime.now().isoformat(),
                'services_started': ['web_crawler', 'api_collector', 'realtime_generator'],
                'schedules_active': list(self.schedules.keys()),
                'status': 'active'
            }

            logger.info(f"✅ Data ingestion orchestration started: {orchestration_status}")

            # Store orchestration status
            self.redis_client.setex(
                'orchestration_status',
                86400,  # 24 hours TTL
                json.dumps(orchestration_status)
            )

            # Wait for scheduled tasks to run
            await scheduled_task

        except KeyboardInterrupt:
            logger.info("🛑 Stopping data ingestion orchestration...")
            await self.stop_orchestration()

        except Exception as e:
            logger.error(f"❌ Orchestration error: {e}")
            await self.stop_orchestration()

    async def stop_orchestration(self):
        """Stop all orchestration activities"""
        logger.info("🛑 Stopping data ingestion orchestration...")

        self.running = False

        try:
            # Stop real-time generation
            if 'realtime' in self.collection_tasks:
                self.collection_tasks['realtime'].cancel()

            # Stop other collection tasks
            for task_name, task in self.collection_tasks.items():
                if not task.done():
                    task.cancel()

            # Clear scheduled tasks
            schedule.clear()

            # Update orchestration status
            self.redis_client.setex(
                'orchestration_status',
                3600,  # 1 hour TTL
                json.dumps({
                    'status': 'stopped',
                    'stop_time': datetime.now().isoformat()
                })
            )

            logger.info("✅ Data ingestion orchestration stopped")

        except Exception as e:
            logger.error(f"❌ Error stopping orchestration: {e}")

    def close_connections(self):
        """Close all connections"""
        try:
            # Close collector connections
            if hasattr(self, 'web_crawler'):
                self.web_crawler.close_connections()
            if hasattr(self, 'api_collector'):
                self.api_collector.close_connections()
            if hasattr(self, 'realtime_generator'):
                self.realtime_generator.close_connections()

            # Close orchestrator connections
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()

            logger.info("🔌 All orchestrator connections closed")

        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
        self.running = False

# CLI interface
async def main():
    """Main execution function with CLI interface"""
    orchestrator = DataIngestionOrchestrator()

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, orchestrator.signal_handler)
    signal.signal(signal.SIGTERM, orchestrator.signal_handler)

    try:
        # Start orchestration
        await orchestrator.start_orchestration()

    finally:
        # Cleanup
        orchestrator.close_connections()

if __name__ == "__main__":
    print("""
    🎯 E-commerce DSS Data Ingestion Orchestrator
    =============================================

    This orchestrator manages:
    • 🕷️  Web crawling from e-commerce sites
    • 🔗 API data collection from social media & financial sources
    • ⚡ Real-time data generation for streaming analytics
    • 📊 Performance monitoring and health checks

    Press Ctrl+C to stop gracefully
    """)

    asyncio.run(main())