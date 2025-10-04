#!/usr/bin/env python3
"""
Free Public APIs Data Collector
Collects data from free, accessible APIs for e-commerce analytics testing
"""

import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from pymongo import MongoClient
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProductAPIData:
    """Product data from APIs"""
    source: str
    product_id: str
    title: str
    price: float
    category: str
    description: str
    image: Optional[str]
    rating: Optional[Dict[str, Any]]
    timestamp: datetime

@dataclass
class UserAPIData:
    """User data from APIs"""
    source: str
    user_id: str
    name: str
    email: str
    address: Optional[Dict[str, Any]]
    phone: Optional[str]
    website: Optional[str]
    timestamp: datetime

@dataclass
class CryptoAPIData:
    """Cryptocurrency data"""
    source: str
    symbol: str
    name: str
    current_price: float
    market_cap: float
    price_change_24h: float
    volume_24h: float
    timestamp: datetime

@dataclass
class NewsAPIData:
    """News/Discussion data"""
    source: str
    item_id: str
    title: str
    content: Optional[str]
    author: Optional[str]
    score: Optional[int]
    comments_count: Optional[int]
    timestamp: datetime

class FreeAPICollector:
    """Collector for free, accessible APIs"""

    def __init__(self, config_path: str = "config/data_sources.json"):
        self.load_config(config_path)
        self.setup_connections()
        self.session = None

    def load_config(self, config_path: str):
        """Load API configuration"""
        try:
            config_file = Path(__file__).parent.parent / config_path
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                logger.info("✅ Free API configuration loaded")
        except Exception as e:
            logger.error(f"❌ Failed to load config: {e}")
            raise

    def setup_connections(self):
        """Setup database connections"""
        try:
            # Kafka producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )

            # MongoDB
            self.mongo_client = MongoClient('mongodb://localhost:27017/')
            self.mongo_db = self.mongo_client.dss_streaming

            # Redis for caching
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

            logger.info("✅ Free API collector connections established")
        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            raise

    async def get_session(self):
        """Get HTTP session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def collect_fakestore_data(self) -> List[ProductAPIData]:
        """Collect data from Fake Store API (completely free)"""
        products_data = []

        try:
            session = await self.get_session()

            # Get all products
            url = "https://fakestoreapi.com/products"

            async with session.get(url) as response:
                if response.status == 200:
                    products = await response.json()

                    for product in products:
                        try:
                            product_data = ProductAPIData(
                                source='fakestore_api',
                                product_id=str(product.get('id')),
                                title=product.get('title', 'Unknown'),
                                price=float(product.get('price', 0)),
                                category=product.get('category', 'general'),
                                description=product.get('description', ''),
                                image=product.get('image'),
                                rating=product.get('rating'),
                                timestamp=datetime.now()
                            )
                            products_data.append(product_data)

                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing product: {e}")
                            continue

                    logger.info(f"📦 Collected {len(products_data)} products from FakeStore API")

        except Exception as e:
            logger.error(f"❌ Error collecting FakeStore data: {e}")

        return products_data

    async def collect_jsonplaceholder_data(self) -> List[UserAPIData]:
        """Collect user data from JSONPlaceholder (completely free)"""
        users_data = []

        try:
            session = await self.get_session()

            # Get users
            url = "https://jsonplaceholder.typicode.com/users"

            async with session.get(url) as response:
                if response.status == 200:
                    users = await response.json()

                    for user in users:
                        try:
                            user_data = UserAPIData(
                                source='jsonplaceholder_api',
                                user_id=str(user.get('id')),
                                name=user.get('name', 'Unknown'),
                                email=user.get('email', ''),
                                address=user.get('address'),
                                phone=user.get('phone'),
                                website=user.get('website'),
                                timestamp=datetime.now()
                            )
                            users_data.append(user_data)

                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing user: {e}")
                            continue

                    logger.info(f"👥 Collected {len(users_data)} users from JSONPlaceholder API")

        except Exception as e:
            logger.error(f"❌ Error collecting JSONPlaceholder data: {e}")

        return users_data

    async def collect_coingecko_data(self) -> List[CryptoAPIData]:
        """Collect crypto data from CoinGecko (free tier)"""
        crypto_data = []

        try:
            session = await self.get_session()

            # Get top cryptocurrencies
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 20,
                'page': 1,
                'sparkline': 'false'
            }

            async with session.get(url, params=params) as response:
                if response.status == 200:
                    coins = await response.json()

                    for coin in coins:
                        try:
                            crypto_entry = CryptoAPIData(
                                source='coingecko_api',
                                symbol=coin.get('symbol', '').upper(),
                                name=coin.get('name', 'Unknown'),
                                current_price=float(coin.get('current_price', 0)),
                                market_cap=float(coin.get('market_cap', 0)),
                                price_change_24h=float(coin.get('price_change_percentage_24h', 0)),
                                volume_24h=float(coin.get('total_volume', 0)),
                                timestamp=datetime.now()
                            )
                            crypto_data.append(crypto_entry)

                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing crypto data: {e}")
                            continue

                    logger.info(f"₿ Collected {len(crypto_data)} crypto prices from CoinGecko API")

                elif response.status == 429:
                    logger.warning("⚠️ CoinGecko rate limit reached")

        except Exception as e:
            logger.error(f"❌ Error collecting CoinGecko data: {e}")

        return crypto_data

    async def collect_hacker_news_data(self) -> List[NewsAPIData]:
        """Collect tech news from Hacker News (completely free)"""
        news_data = []

        try:
            session = await self.get_session()

            # Get top stories
            top_stories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"

            async with session.get(top_stories_url) as response:
                if response.status == 200:
                    story_ids = await response.json()

                    # Get first 20 stories
                    for story_id in story_ids[:20]:
                        try:
                            item_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"

                            async with session.get(item_url) as item_response:
                                if item_response.status == 200:
                                    item = await item_response.json()

                                    if item and item.get('type') == 'story':
                                        news_entry = NewsAPIData(
                                            source='hackernews_api',
                                            item_id=str(item.get('id')),
                                            title=item.get('title', 'No title'),
                                            content=item.get('text'),
                                            author=item.get('by'),
                                            score=item.get('score', 0),
                                            comments_count=len(item.get('kids', [])),
                                            timestamp=datetime.fromtimestamp(item.get('time', 0)) if item.get('time') else datetime.now()
                                        )
                                        news_data.append(news_entry)

                            # Rate limiting
                            await asyncio.sleep(0.1)

                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing HN story {story_id}: {e}")
                            continue

                    logger.info(f"📰 Collected {len(news_data)} stories from Hacker News API")

        except Exception as e:
            logger.error(f"❌ Error collecting Hacker News data: {e}")

        return news_data

    async def collect_exchange_rates(self) -> Dict[str, Any]:
        """Collect currency exchange rates (free)"""
        try:
            session = await self.get_session()

            url = "https://api.exchangerate-api.com/v4/latest/USD"

            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()

                    exchange_data = {
                        'source': 'exchangerate_api',
                        'base': data.get('base'),
                        'date': data.get('date'),
                        'rates': data.get('rates'),
                        'timestamp': datetime.now().isoformat()
                    }

                    logger.info(f"💱 Collected exchange rates for {len(data.get('rates', {}))} currencies")
                    return exchange_data

        except Exception as e:
            logger.error(f"❌ Error collecting exchange rates: {e}")

        return {}

    def save_to_storage(self, data_list: List[Any], data_type: str):
        """Save collected data to storage systems"""
        try:
            for data_item in data_list:
                data_dict = asdict(data_item)

                # Topic mapping
                topic_mapping = {
                    'products': 'products_raw',
                    'users': 'customers_raw',
                    'crypto': 'market_data',
                    'news': 'news_trends'
                }

                topic = topic_mapping.get(data_type, 'raw_data')

                # Send to Kafka
                self.kafka_producer.send(
                    topic,
                    key=f"{data_item.source}_{getattr(data_item, 'product_id', getattr(data_item, 'user_id', getattr(data_item, 'symbol', getattr(data_item, 'item_id', 'unknown'))))}",
                    value=data_dict
                )

                # Save to MongoDB
                collection_mapping = {
                    'products': 'products_raw',
                    'users': 'customers_raw',
                    'crypto': 'crypto_data_raw',
                    'news': 'news_data_raw'
                }

                collection_name = collection_mapping.get(data_type, 'raw_data')
                getattr(self.mongo_db, collection_name).insert_one(data_dict)

            # Flush Kafka messages
            self.kafka_producer.flush()

            logger.info(f"💾 Saved {len(data_list)} {data_type} records to storage")

        except Exception as e:
            logger.error(f"❌ Error saving {data_type} data: {e}")

    def save_exchange_data(self, exchange_data: Dict[str, Any]):
        """Save exchange rate data"""
        try:
            if exchange_data:
                # Send to Kafka
                self.kafka_producer.send(
                    'market_data',
                    key='exchange_rates',
                    value=exchange_data
                )

                # Save to MongoDB
                self.mongo_db.exchange_rates_raw.insert_one(exchange_data)
                self.kafka_producer.flush()

                logger.info("💾 Saved exchange rate data to storage")

        except Exception as e:
            logger.error(f"❌ Error saving exchange data: {e}")

    async def run_free_api_collection(self):
        """Run complete free API collection session"""
        logger.info("🚀 Starting free API data collection...")

        try:
            session_results = {
                'start_time': datetime.now().isoformat(),
                'collections': {},
                'total_records': 0,
                'success': True
            }

            # Collect from different free APIs
            tasks = []

            # FakeStore API - Products
            logger.info("📦 Collecting products from FakeStore API...")
            products = await self.collect_fakestore_data()
            if products:
                self.save_to_storage(products, 'products')
                session_results['collections']['fakestore_products'] = len(products)

            await asyncio.sleep(1)  # Rate limiting

            # JSONPlaceholder API - Users
            logger.info("👥 Collecting users from JSONPlaceholder API...")
            users = await self.collect_jsonplaceholder_data()
            if users:
                self.save_to_storage(users, 'users')
                session_results['collections']['jsonplaceholder_users'] = len(users)

            await asyncio.sleep(1)  # Rate limiting

            # CoinGecko API - Crypto data
            logger.info("₿ Collecting crypto data from CoinGecko API...")
            crypto_data = await self.collect_coingecko_data()
            if crypto_data:
                self.save_to_storage(crypto_data, 'crypto')
                session_results['collections']['coingecko_crypto'] = len(crypto_data)

            await asyncio.sleep(2)  # Rate limiting for CoinGecko

            # Hacker News API - Tech news
            logger.info("📰 Collecting tech news from Hacker News API...")
            news_data = await self.collect_hacker_news_data()
            if news_data:
                self.save_to_storage(news_data, 'news')
                session_results['collections']['hackernews_stories'] = len(news_data)

            await asyncio.sleep(1)  # Rate limiting

            # Exchange Rate API
            logger.info("💱 Collecting exchange rates...")
            exchange_data = await self.collect_exchange_rates()
            if exchange_data:
                self.save_exchange_data(exchange_data)
                session_results['collections']['exchange_rates'] = 1

            # Calculate totals
            session_results['total_records'] = sum(session_results['collections'].values())
            session_results['end_time'] = datetime.now().isoformat()

            logger.info(f"✅ Free API collection completed: {session_results}")
            return session_results

        except Exception as e:
            logger.error(f"❌ Free API collection failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

        finally:
            if self.session and not self.session.closed:
                await self.session.close()

    def close_connections(self):
        """Close all connections"""
        try:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
            logger.info("🔌 Free API collector connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

# Main execution
async def main():
    """Main execution function for testing"""
    collector = FreeAPICollector()

    try:
        # Test all free APIs
        result = await collector.run_free_api_collection()
        print(f"📊 Free API Collection Results: {json.dumps(result, indent=2)}")

    finally:
        collector.close_connections()

if __name__ == "__main__":
    print("""
    🎯 Free API Data Collector
    =========================

    Testing completely free APIs:
    • 📦 FakeStore API - E-commerce products
    • 👥 JSONPlaceholder - User profiles
    • ₿ CoinGecko - Cryptocurrency data
    • 📰 Hacker News - Tech news/discussions
    • 💱 Exchange Rate API - Currency rates

    All APIs are free and work without authentication!
    """)

    asyncio.run(main())