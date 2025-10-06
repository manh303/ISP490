#!/usr/bin/env python3
"""
Simple test for e-commerce APIs
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime

async def test_api(session, name, url):
    """Test một API endpoint"""
    try:
        print(f"Testing {name}...", end=' ')
        start_time = time.time()

        async with session.get(url, timeout=10) as response:
            end_time = time.time()
            response_time = round((end_time - start_time) * 1000, 2)

            if response.status == 200:
                data = await response.json()

                if isinstance(data, list):
                    count = len(data)
                elif isinstance(data, dict) and 'products' in data:
                    count = len(data['products'])
                else:
                    count = 1

                print(f"SUCCESS - {count} records ({response_time}ms)")
                return True
            else:
                print(f"FAILED - HTTP {response.status}")
                return False

    except Exception as e:
        print(f"ERROR - {str(e)}")
        return False

async def main():
    print("E-commerce APIs Test")
    print("===================")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    apis = [
        ("FakeStore Products", "https://fakestoreapi.com/products?limit=5"),
        ("FakeStore Users", "https://fakestoreapi.com/users?limit=3"),
        ("FakeStore Carts", "https://fakestoreapi.com/carts?limit=3"),
        ("DummyJSON Products", "https://dummyjson.com/products?limit=5"),
        ("DummyJSON Users", "https://dummyjson.com/users?limit=3"),
        ("Platzi Products", "https://api.escuelajs.co/api/v1/products?offset=0&limit=5"),
    ]

    success_count = 0

    async with aiohttp.ClientSession() as session:
        for name, url in apis:
            success = await test_api(session, name, url)
            if success:
                success_count += 1
            await asyncio.sleep(0.5)

    print()
    print("SUMMARY:")
    print(f"Total APIs: {len(apis)}")
    print(f"Working: {success_count}")
    print(f"Success Rate: {round((success_count/len(apis))*100, 1)}%")

    if success_count >= 4:
        print()
        print("READY FOR DATA COLLECTION!")
        print("Next: python apis/ecommerce_api_collector.py")
    else:
        print()
        print("Some APIs not working. Please check network connection.")

if __name__ == "__main__":
    asyncio.run(main())