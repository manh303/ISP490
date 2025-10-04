#!/usr/bin/env python3
"""
Multi-Source API Data Collector
Collects data from various public APIs for e-commerce analytics
"""

import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import os
from pathlib import Path

import requests
import tweepy
import yfinance as yf
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
class SocialMentionData:
    """Social media mention data structure"""
    source: str
    platform: str
    post_id: str
    author: str
    content: str
    timestamp: datetime
    engagement_metrics: Dict[str, int]
    sentiment_score: Optional[float]
    brand_mentions: List[str]
    hashtags: List[str]
    location: Optional[str] = None

@dataclass
class MarketData:
    """Financial market data structure"""
    source: str
    symbol: str
    price: float
    change: float
    change_percent: float
    volume: int
    market_cap: Optional[float]
    timestamp: datetime
    currency: str = 'USD'

@dataclass
class CryptoData:
    """Cryptocurrency data structure"""
    source: str
    symbol: str
    name: str
    price_usd: float
    price_change_24h: float
    volume_24h: float
    market_cap: float
    timestamp: datetime

class MultiSourceAPICollector:
    """Comprehensive API data collector"""

    def __init__(self, config_path: str = "config/data_sources.json"):
        self.load_config(config_path)
        self.setup_connections()
        self.setup_api_clients()

        # Rate limiting settings
        self.rate_limits = {
            'twitter': {'requests_per_window': 100, 'window_minutes': 15},
            'yahoo_finance': {'requests_per_window': 500, 'window_minutes': 60},
            'coingecko': {'requests_per_window': 100, 'window_minutes': 1},
            'google_trends': {'requests_per_window': 100, 'window_minutes': 60}
        }

    def load_config(self, config_path: str):
        """Load API configuration"""
        try:
            config_file = Path(__file__).parent.parent / config_path
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                logger.info("✅ API configuration loaded")
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

            # Redis for caching and rate limiting
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

            logger.info("✅ Database connections established")
        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            raise

    def setup_api_clients(self):
        """Setup API clients with authentication"""
        try:
            # Twitter API (you need to set these environment variables)
            twitter_bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
            if twitter_bearer_token:
                self.twitter_client = tweepy.Client(bearer_token=twitter_bearer_token)
                logger.info("✅ Twitter API client initialized")
            else:
                self.twitter_client = None
                logger.warning("⚠️ Twitter API credentials not found")

            # Other API clients will be initialized as needed
            logger.info("✅ API clients setup completed")

        except Exception as e:
            logger.error(f"❌ Error setting up API clients: {e}")

    async def collect_social_media_data(self) -> List[SocialMentionData]:
        """Collect social media mentions and trends"""
        social_data = []

        try:
            # Twitter data collection
            if self.twitter_client:
                twitter_data = await self.collect_twitter_data()
                social_data.extend(twitter_data)

            # Add more social media sources as needed
            logger.info(f"📱 Collected {len(social_data)} social media mentions")

        except Exception as e:
            logger.error(f"❌ Error collecting social media data: {e}")

        return social_data

    async def collect_twitter_data(self) -> List[SocialMentionData]:
        """Collect Twitter mentions for e-commerce brands"""
        tweets_data = []

        try:
            # E-commerce related keywords
            keywords = [
                'shopee', 'lazada', 'tiki', 'sendo', 'amazon',
                'ecommerce', 'online shopping', 'shopping online',
                'mua sắm online', 'thương mại điện tử'
            ]

            for keyword in keywords:
                try:
                    # Rate limiting check
                    if not self.check_rate_limit('twitter'):
                        logger.warning("⚠️ Twitter rate limit reached, skipping...")
                        break

                    # Search recent tweets
                    tweets = tweepy.Paginator(
                        self.twitter_client.search_recent_tweets,
                        query=f"{keyword} -is:retweet lang:vi OR lang:en",
                        tweet_fields=['created_at', 'author_id', 'public_metrics', 'geo'],
                        max_results=10
                    ).flatten(limit=50)

                    for tweet in tweets:
                        try:
                            # Extract engagement metrics
                            metrics = tweet.public_metrics or {}

                            # Simple sentiment analysis (you can integrate with more sophisticated tools)
                            sentiment = self.simple_sentiment_analysis(tweet.text)

                            # Extract mentions and hashtags
                            mentions = self.extract_brand_mentions(tweet.text)
                            hashtags = self.extract_hashtags(tweet.text)

                            tweet_data = SocialMentionData(
                                source='twitter_api',
                                platform='twitter',
                                post_id=str(tweet.id),
                                author=str(tweet.author_id),
                                content=tweet.text,
                                timestamp=tweet.created_at,
                                engagement_metrics={
                                    'likes': metrics.get('like_count', 0),
                                    'retweets': metrics.get('retweet_count', 0),
                                    'replies': metrics.get('reply_count', 0),
                                    'quotes': metrics.get('quote_count', 0)
                                },
                                sentiment_score=sentiment,
                                brand_mentions=mentions,
                                hashtags=hashtags,
                                location=None
                            )

                            tweets_data.append(tweet_data)

                        except Exception as e:
                            logger.warning(f"⚠️ Error processing tweet: {e}")
                            continue

                    # Update rate limit tracker
                    self.update_rate_limit('twitter')

                    # Delay between keyword searches
                    await asyncio.sleep(2)

                except Exception as e:
                    logger.warning(f"⚠️ Error searching for keyword '{keyword}': {e}")
                    continue

            logger.info(f"🐦 Collected {len(tweets_data)} tweets")

        except Exception as e:
            logger.error(f"❌ Error in Twitter data collection: {e}")

        return tweets_data

    def simple_sentiment_analysis(self, text: str) -> float:
        """Simple sentiment analysis using keyword matching"""
        positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'best', 'awesome',
                         'tốt', 'tuyệt', 'xuất sắc', 'thích', 'hay', 'đẹp']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'horrible',
                         'tệ', 'khủng khiếp', 'ghét', 'kém', 'tồi', 'dở']

        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        if positive_count + negative_count == 0:
            return 0.0  # Neutral

        return (positive_count - negative_count) / (positive_count + negative_count)

    def extract_brand_mentions(self, text: str) -> List[str]:
        """Extract e-commerce brand mentions"""
        brands = ['shopee', 'lazada', 'tiki', 'sendo', 'amazon', 'alibaba', 'fptshop']
        text_lower = text.lower()
        return [brand for brand in brands if brand in text_lower]

    def extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from text"""
        import re
        hashtags = re.findall(r'#\w+', text)
        return [tag.lower() for tag in hashtags]

    async def collect_financial_data(self) -> List[MarketData]:
        """Collect financial market data for e-commerce companies"""
        market_data = []

        try:
            # E-commerce stock symbols
            symbols = ['AMZN', 'BABA', 'MELI', 'SE', 'SHOP', 'EBAY', 'ETSY']

            for symbol in symbols:
                try:
                    # Get stock data using yfinance
                    stock = yf.Ticker(symbol)
                    info = stock.info
                    hist = stock.history(period='1d', interval='1m')

                    if not hist.empty:
                        latest = hist.iloc[-1]
                        previous = hist.iloc[-2] if len(hist) > 1 else latest

                        change = latest['Close'] - previous['Close']
                        change_percent = (change / previous['Close']) * 100 if previous['Close'] != 0 else 0

                        market_entry = MarketData(
                            source='yahoo_finance',
                            symbol=symbol,
                            price=float(latest['Close']),
                            change=float(change),
                            change_percent=float(change_percent),
                            volume=int(latest['Volume']),
                            market_cap=info.get('marketCap'),
                            timestamp=datetime.now(),
                            currency='USD'
                        )

                        market_data.append(market_entry)

                    # Rate limiting
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.warning(f"⚠️ Error collecting data for {symbol}: {e}")
                    continue

            logger.info(f"📈 Collected {len(market_data)} market data points")

        except Exception as e:
            logger.error(f"❌ Error collecting financial data: {e}")

        return market_data

    async def collect_crypto_data(self) -> List[CryptoData]:
        """Collect cryptocurrency data relevant to e-commerce"""
        crypto_data = []

        try:
            # Cryptocurrencies relevant to e-commerce payments
            crypto_symbols = ['bitcoin', 'ethereum', 'binancecoin', 'cardano', 'dogecoin']

            async with aiohttp.ClientSession() as session:
                for symbol in crypto_symbols:
                    try:
                        url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true"

                        async with session.get(url) as response:
                            if response.status == 200:
                                data = await response.json()

                                if symbol in data:
                                    crypto_info = data[symbol]

                                    crypto_entry = CryptoData(
                                        source='coingecko',
                                        symbol=symbol,
                                        name=symbol.replace('binancecoin', 'bnb').title(),
                                        price_usd=crypto_info.get('usd', 0),
                                        price_change_24h=crypto_info.get('usd_24h_change', 0),
                                        volume_24h=crypto_info.get('usd_24h_vol', 0),
                                        market_cap=crypto_info.get('usd_market_cap', 0),
                                        timestamp=datetime.now()
                                    )

                                    crypto_data.append(crypto_entry)

                        # Rate limiting for CoinGecko API
                        await asyncio.sleep(1)

                    except Exception as e:
                        logger.warning(f"⚠️ Error collecting data for {symbol}: {e}")
                        continue

            logger.info(f"₿ Collected {len(crypto_data)} crypto data points")

        except Exception as e:
            logger.error(f"❌ Error collecting crypto data: {e}")

        return crypto_data

    def check_rate_limit(self, api_name: str) -> bool:
        """Check if API rate limit allows new requests"""
        try:
            rate_config = self.rate_limits.get(api_name, {})
            window_minutes = rate_config.get('window_minutes', 60)
            max_requests = rate_config.get('requests_per_window', 100)

            # Check current usage from Redis
            key = f"rate_limit:{api_name}"
            current_count = self.redis_client.get(key)

            if current_count is None:
                return True

            return int(current_count) < max_requests

        except Exception as e:
            logger.warning(f"⚠️ Error checking rate limit for {api_name}: {e}")
            return True

    def update_rate_limit(self, api_name: str):
        """Update rate limit counter"""
        try:
            rate_config = self.rate_limits.get(api_name, {})
            window_minutes = rate_config.get('window_minutes', 60)

            key = f"rate_limit:{api_name}"

            # Increment counter with expiration
            pipe = self.redis_client.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_minutes * 60)
            pipe.execute()

        except Exception as e:
            logger.warning(f"⚠️ Error updating rate limit for {api_name}: {e}")

    def save_to_storage(self, data_list: List[Any], data_type: str):
        """Save collected data to storage systems"""
        try:
            for data_item in data_list:
                data_dict = asdict(data_item)

                # Determine Kafka topic based on data type
                topic_mapping = {
                    'social': 'social_mentions',
                    'market': 'market_data',
                    'crypto': 'crypto_data'
                }

                topic = topic_mapping.get(data_type, 'raw_data')

                # Send to Kafka
                self.kafka_producer.send(
                    topic,
                    key=f"{data_item.source}_{getattr(data_item, 'post_id', getattr(data_item, 'symbol', 'unknown'))}",
                    value=data_dict
                )

                # Save to MongoDB
                collection_mapping = {
                    'social': 'social_mentions_raw',
                    'market': 'market_data_raw',
                    'crypto': 'crypto_data_raw'
                }

                collection_name = collection_mapping.get(data_type, 'raw_data')
                getattr(self.mongo_db, collection_name).insert_one(data_dict)

            # Flush Kafka messages
            self.kafka_producer.flush()

            logger.info(f"💾 Saved {len(data_list)} {data_type} records to storage")

        except Exception as e:
            logger.error(f"❌ Error saving {data_type} data: {e}")

    async def run_api_collection_session(self):
        """Run complete API data collection session"""
        logger.info("🚀 Starting API data collection session...")

        try:
            session_results = {
                'start_time': datetime.now().isoformat(),
                'collections': {},
                'total_records': 0,
                'success': True
            }

            # Collect social media data
            logger.info("📱 Collecting social media data...")
            social_data = await self.collect_social_media_data()
            if social_data:
                self.save_to_storage(social_data, 'social')
                session_results['collections']['social_media'] = len(social_data)

            # Collect financial market data
            logger.info("📈 Collecting financial market data...")
            market_data = await self.collect_financial_data()
            if market_data:
                self.save_to_storage(market_data, 'market')
                session_results['collections']['market_data'] = len(market_data)

            # Collect cryptocurrency data
            logger.info("₿ Collecting cryptocurrency data...")
            crypto_data = await self.collect_crypto_data()
            if crypto_data:
                self.save_to_storage(crypto_data, 'crypto')
                session_results['collections']['crypto_data'] = len(crypto_data)

            # Calculate totals
            session_results['total_records'] = sum(session_results['collections'].values())
            session_results['end_time'] = datetime.now().isoformat()

            logger.info(f"✅ API collection session completed: {session_results}")
            return session_results

        except Exception as e:
            logger.error(f"❌ API collection session failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def close_connections(self):
        """Close all connections"""
        try:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
            logger.info("🔌 API collector connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

# Main execution
async def main():
    """Main execution function"""
    collector = MultiSourceAPICollector()

    try:
        # Run API collection session
        result = await collector.run_api_collection_session()
        print(f"📊 API Collection Results: {json.dumps(result, indent=2)}")

    finally:
        collector.close_connections()

if __name__ == "__main__":
    # Set up environment variables for APIs (example)
    # os.environ['TWITTER_BEARER_TOKEN'] = 'your_twitter_bearer_token'

    asyncio.run(main())