#!/usr/bin/env python3
"""
Simple E-commerce Data Collector
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaProducer

class SimpleEcommerceCollector:
    def __init__(self):
        self.setup_connections()

    def setup_connections(self):
        """Setup database connections"""
        try:
            # MongoDB
            self.mongo_client = MongoClient('mongodb://mongodb:27017/')
            self.mongo_db = self.mongo_client.dss_streaming
            print("OK MongoDB connected")

            # Kafka
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['kafka:29092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
            )
            print("OK Kafka connected")

        except Exception as e:
            print(f"ERROR Connection error: {e}")
            raise

    async def collect_fakestore(self):
        """Thu thập từ FakeStore API"""
        results = {'products': [], 'users': [], 'carts': []}

        async with aiohttp.ClientSession() as session:
            # Products
            async with session.get("https://fakestoreapi.com/products") as response:
                if response.status == 200:
                    products = await response.json()
                    for product in products:
                        product_data = {
                            'source': 'fakestore',
                            'product_id': str(product.get('id')),
                            'title': product.get('title'),
                            'price': product.get('price'),
                            'category': product.get('category'),
                            'description': product.get('description'),
                            'image': product.get('image'),
                            'rating': product.get('rating', {}).get('rate'),
                            'timestamp': datetime.now().isoformat()
                        }
                        results['products'].append(product_data)

            # Users
            async with session.get("https://fakestoreapi.com/users") as response:
                if response.status == 200:
                    users = await response.json()
                    for user in users:
                        user_data = {
                            'source': 'fakestore',
                            'user_id': str(user.get('id')),
                            'name': f"{user.get('name', {}).get('firstname', '')} {user.get('name', {}).get('lastname', '')}",
                            'email': user.get('email'),
                            'phone': user.get('phone'),
                            'timestamp': datetime.now().isoformat()
                        }
                        results['users'].append(user_data)

            # Carts
            async with session.get("https://fakestoreapi.com/carts") as response:
                if response.status == 200:
                    carts = await response.json()
                    for cart in carts:
                        cart_data = {
                            'source': 'fakestore',
                            'cart_id': str(cart.get('id')),
                            'user_id': str(cart.get('userId')),
                            'products': cart.get('products'),
                            'date': cart.get('date'),
                            'timestamp': datetime.now().isoformat()
                        }
                        results['carts'].append(cart_data)

        return results

    async def collect_dummyjson(self):
        """Thu thập từ DummyJSON API"""
        results = {'products': [], 'users': []}

        async with aiohttp.ClientSession() as session:
            # Products
            async with session.get("https://dummyjson.com/products?limit=30") as response:
                if response.status == 200:
                    data = await response.json()
                    for product in data.get('products', []):
                        product_data = {
                            'source': 'dummyjson',
                            'product_id': str(product.get('id')),
                            'title': product.get('title'),
                            'price': product.get('price'),
                            'category': product.get('category'),
                            'description': product.get('description'),
                            'brand': product.get('brand'),
                            'rating': product.get('rating'),
                            'stock': product.get('stock'),
                            'discount': product.get('discountPercentage'),
                            'timestamp': datetime.now().isoformat()
                        }
                        results['products'].append(product_data)

            # Users
            async with session.get("https://dummyjson.com/users?limit=20") as response:
                if response.status == 200:
                    data = await response.json()
                    for user in data.get('users', []):
                        user_data = {
                            'source': 'dummyjson',
                            'user_id': str(user.get('id')),
                            'name': f"{user.get('firstName', '')} {user.get('lastName', '')}",
                            'email': user.get('email'),
                            'phone': user.get('phone'),
                            'timestamp': datetime.now().isoformat()
                        }
                        results['users'].append(user_data)

        return results

    def save_data(self, all_data):
        """Lưu dữ liệu vào Kafka"""
        total_saved = 0

        for source, data_types in all_data.items():
            for data_type, items in data_types.items():
                for item in items:
                    # Send to Kafka
                    topic = f"{data_type}_raw"
                    self.kafka_producer.send(topic, item)
                    total_saved += 1

        self.kafka_producer.flush()
        return total_saved

    async def run_collection(self):
        """Chạy thu thập dữ liệu"""
        print("Starting E-commerce Data Collection...")
        print("=" * 40)

        all_data = {}

        # Thu thập từ FakeStore
        print("Collecting from FakeStore API...")
        fakestore_data = await self.collect_fakestore()
        all_data['fakestore'] = fakestore_data
        print(f"OK FakeStore: {len(fakestore_data['products'])} products, {len(fakestore_data['users'])} users, {len(fakestore_data['carts'])} carts")

        await asyncio.sleep(1)

        # Thu thập từ DummyJSON
        print("Collecting from DummyJSON API...")
        dummyjson_data = await self.collect_dummyjson()
        all_data['dummyjson'] = dummyjson_data
        print(f"OK DummyJSON: {len(dummyjson_data['products'])} products, {len(dummyjson_data['users'])} users")

        # Lưu dữ liệu
        print("Saving data to Kafka...")
        total_saved = self.save_data(all_data)

        print("=" * 40)
        print(f"SUCCESS! Saved {total_saved} records")
        print("Data available in:")
        print("- Kafka topics: products_raw, users_raw, carts_raw")

        return {
            'fakestore': {
                'products': len(fakestore_data['products']),
                'users': len(fakestore_data['users']),
                'carts': len(fakestore_data['carts'])
            },
            'dummyjson': {
                'products': len(dummyjson_data['products']),
                'users': len(dummyjson_data['users'])
            },
            'total_saved': total_saved
        }

    def close(self):
        """Đóng connections"""
        if hasattr(self, 'kafka_producer'):
            self.kafka_producer.close()
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()

async def main():
    collector = SimpleEcommerceCollector()

    try:
        result = await collector.run_collection()
        print("\nCollection Summary:")
        print(json.dumps(result, indent=2))

    finally:
        collector.close()

if __name__ == "__main__":
    asyncio.run(main())