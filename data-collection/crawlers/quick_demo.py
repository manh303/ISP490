#!/usr/bin/env python3
"""Quick demo to show mass crawling is working"""

import requests
import json
import time
from datetime import datetime

def quick_multi_category_demo():
    """Quick demo with multiple categories and pages"""
    print("QUICK MULTI-CATEGORY DEMO")
    print("=" * 40)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    # Test 3 categories with 3 pages each
    categories = ['dien thoai', 'laptop', 'tai nghe']
    all_products = []

    for i, category in enumerate(categories, 1):
        print(f"\n[{i}/3] Category: {category}")

        for page in range(1, 4):  # 3 pages
            print(f"  Page {page}/3...")

            try:
                url = "https://tiki.vn/api/v2/products"
                params = {
                    'limit': 20,
                    'q': category,
                    'page': page
                }

                response = session.get(url, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    products = data.get('data', [])

                    if products:
                        print(f"    Found {len(products)} products")

                        for product in products:
                            simplified = {
                                "category": category,
                                "name": product.get('name', ''),
                                "price": product.get('price', 0),
                                "brand": product.get('brand_name', ''),
                                "page": page
                            }
                            all_products.append(simplified)
                    else:
                        print(f"    No products found")
                        break

                time.sleep(1)  # Quick delay

            except Exception as e:
                print(f"    Error: {e}")
                break

    # Results
    print(f"\n" + "=" * 40)
    print(f"DEMO RESULTS:")
    print(f"Total products collected: {len(all_products)}")
    print(f"Categories: {len(categories)}")

    # Breakdown by category
    breakdown = {}
    for product in all_products:
        cat = product['category']
        breakdown[cat] = breakdown.get(cat, 0) + 1

    print(f"\nBreakdown:")
    for cat, count in breakdown.items():
        print(f"  {cat}: {count} products")

    # Sample products
    print(f"\nSample products:")
    for i, product in enumerate(all_products[:5], 1):
        name = product['name'][:40]
        price = f"{product['price']:,}" if product['price'] else 'N/A'
        print(f"  {i}. {name}... | {price} VND")

    # Save quick demo results
    filename = f"quick_demo_{int(time.time())}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)

    print(f"\nResults saved to: {filename}")
    print(f"\nThis demonstrates multi-category pagination crawling!")
    print(f"The full mass crawler is running in background for ALL categories...")

    return all_products

if __name__ == "__main__":
    quick_multi_category_demo()