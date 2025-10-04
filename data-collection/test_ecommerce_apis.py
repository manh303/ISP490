#!/usr/bin/env python3
"""
Test E-commerce APIs - Kiểm tra 3 APIs chuyên về e-commerce
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime

async def test_ecommerce_api(session, api_name, url, data_type):
    """Test một API endpoint cụ thể"""
    try:
        start_time = time.time()

        async with session.get(url, timeout=15) as response:
            end_time = time.time()
            response_time = round((end_time - start_time) * 1000, 2)

            if response.status == 200:
                data = await response.json()

                # Đếm records
                if isinstance(data, list):
                    record_count = len(data)
                    sample_record = data[0] if data else None
                elif isinstance(data, dict) and 'products' in data:
                    record_count = len(data['products'])
                    sample_record = data['products'][0] if data['products'] else None
                elif isinstance(data, dict) and 'users' in data:
                    record_count = len(data['users'])
                    sample_record = data['users'][0] if data['users'] else None
                elif isinstance(data, dict) and 'carts' in data:
                    record_count = len(data['carts'])
                    sample_record = data['carts'][0] if data['carts'] else None
                else:
                    record_count = 1
                    sample_record = data

                result = {
                    'api': api_name,
                    'data_type': data_type,
                    'status': '✅ SUCCESS',
                    'response_time_ms': response_time,
                    'record_count': record_count,
                    'sample_record': sample_record
                }
            else:
                result = {
                    'api': api_name,
                    'data_type': data_type,
                    'status': f'❌ FAILED - HTTP {response.status}',
                    'response_time_ms': response_time,
                    'error': f'HTTP {response.status}'
                }

    except asyncio.TimeoutError:
        result = {
            'api': api_name,
            'data_type': data_type,
            'status': '⏰ TIMEOUT',
            'error': 'Request timeout after 15 seconds'
        }
    except Exception as e:
        result = {
            'api': api_name,
            'data_type': data_type,
            'status': '❌ ERROR',
            'error': str(e)
        }

    return result

async def test_all_ecommerce_apis():
    """Test tất cả 3 e-commerce APIs"""

    ecommerce_apis = [
        # FakeStore API
        {
            'api': 'FakeStore',
            'url': 'https://fakestoreapi.com/products?limit=5',
            'data_type': 'Products'
        },
        {
            'api': 'FakeStore',
            'url': 'https://fakestoreapi.com/products/categories',
            'data_type': 'Categories'
        },
        {
            'api': 'FakeStore',
            'url': 'https://fakestoreapi.com/users?limit=3',
            'data_type': 'Users'
        },
        {
            'api': 'FakeStore',
            'url': 'https://fakestoreapi.com/carts?limit=3',
            'data_type': 'Shopping Carts'
        },

        # DummyJSON API
        {
            'api': 'DummyJSON',
            'url': 'https://dummyjson.com/products?limit=5',
            'data_type': 'Products'
        },
        {
            'api': 'DummyJSON',
            'url': 'https://dummyjson.com/products/categories',
            'data_type': 'Categories'
        },
        {
            'api': 'DummyJSON',
            'url': 'https://dummyjson.com/users?limit=3',
            'data_type': 'Users'
        },
        {
            'api': 'DummyJSON',
            'url': 'https://dummyjson.com/carts?limit=3',
            'data_type': 'Shopping Carts'
        },

        # Platzi Fake API
        {
            'api': 'Platzi',
            'url': 'https://api.escuelajs.co/api/v1/products?offset=0&limit=5',
            'data_type': 'Products'
        },
        {
            'api': 'Platzi',
            'url': 'https://api.escuelajs.co/api/v1/categories?limit=5',
            'data_type': 'Categories'
        },
        {
            'api': 'Platzi',
            'url': 'https://api.escuelajs.co/api/v1/users?limit=3',
            'data_type': 'Users'
        }
    ]

    print(f"""
🛒 Testing E-commerce APIs for DSS Data Collection
================================================
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Testing {len(ecommerce_apis)} e-commerce endpoints...
""")

    results = []

    async with aiohttp.ClientSession() as session:
        # Test APIs
        for api in ecommerce_apis:
            print(f"Testing {api['api']} - {api['data_type']}...", end=' ')
            result = await test_ecommerce_api(session, api['api'], api['url'], api['data_type'])
            results.append(result)
            print(result['status'])

            # Rate limiting
            await asyncio.sleep(0.5)

    # Chi tiết kết quả
    print(f"\n🛒 E-COMMERCE APIs DETAILED RESULTS:")
    print("=" * 80)

    api_stats = {}
    total_records = 0

    for result in results:
        api_name = result['api']
        if api_name not in api_stats:
            api_stats[api_name] = {'success': 0, 'failed': 0, 'records': 0}

        print(f"\n🔗 {result['api']} - {result['data_type']}")
        print(f"   Status: {result['status']}")

        if 'response_time_ms' in result:
            print(f"   Response Time: {result['response_time_ms']} ms")

        if 'record_count' in result:
            print(f"   Records: {result['record_count']}")
            api_stats[api_name]['records'] += result['record_count']
            total_records += result['record_count']

        if 'sample_record' in result and result['sample_record']:
            sample = result['sample_record']
            if isinstance(sample, dict):
                # Hiển thị các trường quan trọng
                important_fields = ['id', 'title', 'name', 'price', 'category', 'email']
                sample_str = ', '.join([f"{k}: {v}" for k, v in sample.items()
                                      if k in important_fields and v is not None][:3])
                print(f"   Sample: {sample_str}")

        if 'error' in result:
            print(f"   Error: {result['error']}")

        if '✅' in result['status']:
            api_stats[api_name]['success'] += 1
        else:
            api_stats[api_name]['failed'] += 1

    # Tổng kết từng API
    print(f"\n📊 API PERFORMANCE SUMMARY:")
    print("=" * 50)

    for api_name, stats in api_stats.items():
        total_tests = stats['success'] + stats['failed']
        success_rate = (stats['success'] / total_tests * 100) if total_tests > 0 else 0

        print(f"\n🔸 {api_name} API:")
        print(f"   Success Rate: {success_rate:.1f}% ({stats['success']}/{total_tests})")
        print(f"   Total Records: {stats['records']}")

        if success_rate >= 75:
            print(f"   Status: ✅ RECOMMENDED FOR USE")
        else:
            print(f"   Status: ⚠️ NEEDS ATTENTION")

    # Tổng kết chung
    successful_apis = sum(1 for stats in api_stats.values() if stats['success'] > stats['failed'])

    print(f"\n🎯 OVERALL SUMMARY:")
    print("=" * 40)
    print(f"APIs tested: {len(api_stats)}")
    print(f"Working APIs: {successful_apis}")
    print(f"Total sample records: {total_records}")
    print(f"Data types available: Products, Users, Categories, Shopping Carts")

    # Recommendations
    print(f"\n💡 E-COMMERCE DATA RECOMMENDATIONS:")
    print("=" * 50)

    working_apis = [name for name, stats in api_stats.items() if stats['success'] > stats['failed']]

    if working_apis:
        print("✅ Recommended APIs for e-commerce data collection:")
        for api in working_apis:
            records = api_stats[api]['records']
            print(f"   • {api} API - {records} sample records available")

    print(f"\n🚀 NEXT STEPS FOR E-COMMERCE DSS:")
    print("=" * 50)
    print("1. Run: python apis/ecommerce_api_collector.py")
    print("2. Check data in MongoDB collections:")
    print("   - products_raw (product catalog)")
    print("   - customers_raw (user profiles)")
    print("   - carts_raw (shopping behavior)")
    print("3. Start real-time data generation for transactions")
    print("4. Begin analytics and ML model development")

    return results

if __name__ == "__main__":
    asyncio.run(test_all_ecommerce_apis())