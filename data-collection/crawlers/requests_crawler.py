#!/usr/bin/env python3
"""Try using requests instead of Selenium to avoid bot detection"""

import requests
import json
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
import os

class RequestsCrawler:
    def __init__(self):
        self.session = requests.Session()

        # Headers to mimic real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def test_simple_get(self, url):
        """Test simple GET request"""
        print(f"Testing GET: {url}")

        try:
            response = self.session.get(url, timeout=10)
            print(f"Status: {response.status_code}")
            print(f"Content length: {len(response.content)}")

            if response.status_code == 200:
                # Check if we got blocked
                if "_____tmd_____" in response.url or "punish" in response.url:
                    print("X Got blocked by anti-bot")
                    return None

                soup = BeautifulSoup(response.content, 'html.parser')
                title = soup.find('title')
                print(f"Page title: {title.text if title else 'No title'}")

                # Look for products
                product_elements = soup.find_all('a', href=lambda x: x and '/products/' in x)
                print(f"Found {len(product_elements)} product links")

                return soup

        except Exception as e:
            print(f"Error: {e}")

        return None

    def test_lazada_mobile_api(self):
        """Try Lazada mobile API endpoint"""
        print("Testing Lazada mobile API...")

        api_url = "https://www.lazada.vn/api/catalog/products/"
        params = {
            'q': 'iphone',
            'page': 1,
            'pageSize': 20
        }

        try:
            response = self.session.get(api_url, params=params, timeout=10)
            print(f"API Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'products' in data:
                        products = data['products']
                        print(f"Found {len(products)} products via API")

                        for i, product in enumerate(products[:3]):
                            name = product.get('name', 'N/A')
                            price = product.get('price', 'N/A')
                            print(f"  {i+1}. {name} | {price}")

                        return products
                except:
                    print("Response is not JSON")

        except Exception as e:
            print(f"API Error: {e}")

        return []

    def try_alternative_sites(self):
        """Try other Vietnamese e-commerce sites that might be easier"""
        print("Testing alternative sites...")

        sites = [
            {
                'name': 'Tiki',
                'url': 'https://tiki.vn/api/v2/products?limit=10&include=advertisement&aggregations=2&q=iphone',
                'type': 'api'
            },
            {
                'name': 'Sendo',
                'url': 'https://www.sendo.vn/tim-kiem/iphone',
                'type': 'html'
            }
        ]

        results = []

        for site in sites:
            try:
                print(f"\nTesting {site['name']}...")
                response = self.session.get(site['url'], timeout=10)
                print(f"Status: {response.status_code}")

                if response.status_code == 200:
                    if site['type'] == 'api':
                        try:
                            data = response.json()
                            if 'data' in data:
                                products = data['data']
                                print(f"  Found {len(products)} products")
                                results.extend(products[:3])
                        except:
                            print("  Not valid JSON")
                    else:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        product_links = soup.find_all('a', href=lambda x: x and '/san-pham/' in x)
                        print(f"  Found {len(product_links)} product links")

            except Exception as e:
                print(f"  Error: {e}")

        return results

    def test_seeds_with_requests(self):
        """Test seed URLs with requests"""
        print("Testing seed URLs with requests...")

        # Load seeds
        seeds_file = os.path.join(os.path.dirname(__file__), 'seeds.txt')
        seed_urls = []

        try:
            with open(seeds_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('http'):
                        seed_urls.append(line)
        except Exception as e:
            print(f"Error loading seeds: {e}")
            return []

        print(f"Loaded {len(seed_urls)} seed URLs")

        working_urls = []

        for i, url in enumerate(seed_urls[:5]):  # Test first 5
            print(f"\n{i+1}. Testing: {url}")

            try:
                response = self.session.get(url, timeout=10)
                print(f"   Status: {response.status_code}")

                if response.status_code == 200 and "_____tmd_____" not in response.url:
                    print("   Status: OK")
                    working_urls.append(url)
                else:
                    print("   Status: Blocked or error")

            except Exception as e:
                print(f"   Error: {e}")

            time.sleep(2)  # Be polite

        print(f"\nWorking URLs: {len(working_urls)}/{len(seed_urls[:5])}")
        return working_urls

def main():
    """Run all tests"""
    crawler = RequestsCrawler()

    print("=== REQUESTS-BASED CRAWLER TEST ===\n")

    # Test 1: Simple Lazada page
    print("1. Testing simple Lazada page...")
    soup = crawler.test_simple_get("https://www.lazada.vn/")

    time.sleep(3)

    # Test 2: Search page
    print("\n2. Testing search page...")
    search_soup = crawler.test_simple_get("https://www.lazada.vn/catalog/?q=iphone")

    time.sleep(3)

    # Test 3: Mobile API
    print("\n3. Testing mobile API...")
    api_products = crawler.test_lazada_mobile_api()

    time.sleep(3)

    # Test 4: Alternative sites
    print("\n4. Testing alternative sites...")
    alt_products = crawler.try_alternative_sites()

    time.sleep(3)

    # Test 5: Seed URLs
    print("\n5. Testing seed URLs...")
    working_seeds = crawler.test_seeds_with_requests()

    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"Lazada home page: {'OK' if soup else 'Blocked'}")
    print(f"Lazada search: {'OK' if search_soup else 'Blocked'}")
    print(f"Mobile API products: {len(api_products)}")
    print(f"Alternative site products: {len(alt_products)}")
    print(f"Working seed URLs: {len(working_seeds)}")

    total_success = len(api_products) + len(alt_products) + len(working_seeds)

    if total_success > 0:
        print("\nSome endpoints are working! Can proceed with alternative approach.")
    else:
        print("\nAll endpoints blocked. Need different strategy.")

if __name__ == "__main__":
    main()