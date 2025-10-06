#!/usr/bin/env python3
"""
Test script for Free APIs - Kiểm tra xem APIs nào hoạt động
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime

async def test_api(session, name, url, description):
    """Test individual API endpoint"""
    try:
        start_time = time.time()

        async with session.get(url, timeout=10) as response:
            end_time = time.time()
            response_time = round((end_time - start_time) * 1000, 2)

            if response.status == 200:
                data = await response.json()
                data_size = len(str(data))

                result = {
                    'name': name,
                    'status': '✅ SUCCESS',
                    'response_time_ms': response_time,
                    'data_size': data_size,
                    'description': description,
                    'sample_data': str(data)[:200] + '...' if len(str(data)) > 200 else str(data)
                }
            else:
                result = {
                    'name': name,
                    'status': f'❌ FAILED - HTTP {response.status}',
                    'response_time_ms': response_time,
                    'description': description,
                    'error': f'HTTP {response.status}'
                }

    except asyncio.TimeoutError:
        result = {
            'name': name,
            'status': '⏰ TIMEOUT',
            'description': description,
            'error': 'Request timeout after 10 seconds'
        }
    except Exception as e:
        result = {
            'name': name,
            'status': '❌ ERROR',
            'description': description,
            'error': str(e)
        }

    return result

async def test_all_free_apis():
    """Test all free APIs from config"""

    apis_to_test = [
        {
            'name': 'FakeStore API - Products',
            'url': 'https://fakestoreapi.com/products?limit=5',
            'description': 'Fake e-commerce product data'
        },
        {
            'name': 'FakeStore API - Categories',
            'url': 'https://fakestoreapi.com/products/categories',
            'description': 'Product categories'
        },
        {
            'name': 'JSONPlaceholder - Users',
            'url': 'https://jsonplaceholder.typicode.com/users?_limit=5',
            'description': 'Mock user profiles'
        },
        {
            'name': 'JSONPlaceholder - Posts',
            'url': 'https://jsonplaceholder.typicode.com/posts?_limit=5',
            'description': 'Mock social media posts'
        },
        {
            'name': 'CoinGecko - Simple Price',
            'url': 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd',
            'description': 'Cryptocurrency prices'
        },
        {
            'name': 'CoinGecko - Markets',
            'url': 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=5&page=1',
            'description': 'Crypto market data'
        },
        {
            'name': 'Exchange Rate API',
            'url': 'https://api.exchangerate-api.com/v4/latest/USD',
            'description': 'Currency exchange rates'
        },
        {
            'name': 'Hacker News - Top Stories',
            'url': 'https://hacker-news.firebaseio.com/v0/topstories.json',
            'description': 'Top Hacker News story IDs'
        },
        {
            'name': 'Hacker News - Sample Item',
            'url': 'https://hacker-news.firebaseio.com/v0/item/1.json',
            'description': 'Sample Hacker News item'
        },
        {
            'name': 'ReqRes - Users',
            'url': 'https://reqres.in/api/users?page=1',
            'description': 'Sample user data'
        },
        {
            'name': 'ReqRes - Products',
            'url': 'https://reqres.in/api/unknown',
            'description': 'Sample product data'
        }
    ]

    print(f"""
🎯 Testing Free APIs for E-commerce DSS Data Collection
=====================================================
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Testing {len(apis_to_test)} API endpoints...
""")

    results = []

    async with aiohttp.ClientSession() as session:
        # Test APIs sequentially to avoid rate limiting
        for api in apis_to_test:
            print(f"Testing {api['name']}...", end=' ')
            result = await test_api(session, api['name'], api['url'], api['description'])
            results.append(result)
            print(result['status'])

            # Small delay between requests
            await asyncio.sleep(0.5)

    # Print detailed results
    print(f"\n📊 DETAILED RESULTS:")
    print("=" * 80)

    successful = 0
    failed = 0

    for result in results:
        print(f"\n🔗 {result['name']}")
        print(f"   Status: {result['status']}")
        print(f"   Description: {result['description']}")

        if 'response_time_ms' in result:
            print(f"   Response Time: {result['response_time_ms']} ms")

        if 'data_size' in result:
            print(f"   Data Size: {result['data_size']} characters")

        if 'sample_data' in result:
            print(f"   Sample Data: {result['sample_data']}")

        if 'error' in result:
            print(f"   Error: {result['error']}")

        if '✅' in result['status']:
            successful += 1
        else:
            failed += 1

    # Summary
    print(f"\n📈 SUMMARY:")
    print("=" * 40)
    print(f"Total APIs tested: {len(results)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"Success Rate: {round((successful/len(results))*100, 1)}%")

    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    print("=" * 40)

    working_apis = [r for r in results if '✅' in r['status']]

    if working_apis:
        print("✅ Working APIs you can use immediately:")
        for api in working_apis:
            print(f"   • {api['name']}")

    print(f"\n🚀 NEXT STEPS:")
    print("=" * 40)
    print("1. Use working APIs in your data collection pipeline")
    print("2. Run the free_api_collector.py to start collecting data")
    print("3. Check data quality and volume in your databases")
    print("4. Optionally sign up for APIs that require free registration")

    return results

if __name__ == "__main__":
    asyncio.run(test_all_free_apis())