#!/usr/bin/env python3
"""Working crawler using requests + BeautifulSoup that actually gets products"""

import requests
import json
import time
import random
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse

class WorkingRequestsCrawler:
    def __init__(self):
        self.session = requests.Session()

        # Real browser headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def extract_products_from_tiki(self, query="iphone", limit=10):
        """Extract products from Tiki API (working alternative)"""
        print(f"Getting products from Tiki API: {query}")

        url = f"https://tiki.vn/api/v2/products"
        params = {
            'limit': limit,
            'include': 'advertisement',
            'aggregations': 2,
            'q': query
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                products = data.get('data', [])

                extracted = []
                for product in products:
                    extracted_product = {
                        "source": "tiki_api",
                        "product_id": str(product.get('id', '')),
                        "product_name": product.get('name', ''),
                        "price_current": product.get('price', 0),
                        "price_original": product.get('list_price', 0),
                        "discount_percent": product.get('discount_rate', 0),
                        "rating_avg": product.get('rating_average', 0),
                        "review_count": product.get('review_count', 0),
                        "brand": product.get('brand_name', ''),
                        "url": f"https://tiki.vn/{product.get('url_path', '')}",
                        "image_urls": [product.get('thumbnail_url', '')],
                        "crawl_date": datetime.now().isoformat()
                    }
                    extracted.append(extracted_product)

                print(f"Successfully extracted {len(extracted)} products from Tiki")
                return extracted

        except Exception as e:
            print(f"Tiki API error: {e}")

        return []

    def extract_products_from_lazada_category(self, category_url, max_products=10):
        """Try to extract products from Lazada category using requests"""
        print(f"Trying Lazada category: {category_url}")

        try:
            response = self.session.get(category_url, timeout=15)
            if response.status_code == 200 and "_____tmd_____" not in response.url:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Look for JSON data in script tags (Lazada often has product data in JS)
                scripts = soup.find_all('script')
                products = []

                for script in scripts:
                    if script.string and 'window.pageData' in script.string:
                        try:
                            # Try to extract JSON data
                            script_content = script.string
                            start = script_content.find('window.pageData =') + 18
                            end = script_content.find('};', start) + 1
                            json_str = script_content[start:end]

                            data = json.loads(json_str)
                            if 'mods' in data:
                                # Look for product data in mods
                                for mod in data['mods'].values():
                                    if isinstance(mod, dict) and 'data' in mod:
                                        mod_data = mod['data']
                                        if 'listItems' in mod_data:
                                            items = mod_data['listItems']
                                            for item in items[:max_products]:
                                                product = {
                                                    "source": "lazada_category",
                                                    "product_id": item.get('itemId', ''),
                                                    "product_name": item.get('name', ''),
                                                    "price_current": item.get('price', ''),
                                                    "price_original": item.get('originalPrice', ''),
                                                    "rating_avg": item.get('ratingScore', 0),
                                                    "image_urls": [item.get('image', '')],
                                                    "url": f"https:{item.get('itemUrl', '')}" if item.get('itemUrl') else '',
                                                    "crawl_date": datetime.now().isoformat()
                                                }
                                                products.append(product)

                        except Exception as e:
                            continue

                if products:
                    print(f"Extracted {len(products)} products from Lazada JS data")
                    return products

                # Fallback: try to find product elements in HTML
                product_selectors = [
                    '[data-qa-locator="product-item"]',
                    '.c16H9d',
                    '[data-spm*="product"]',
                    '.gridItem'
                ]

                for selector in product_selectors:
                    elements = soup.select(selector)
                    if elements:
                        print(f"Found {len(elements)} elements with {selector}")
                        # Try to extract basic info
                        for elem in elements[:max_products]:
                            try:
                                # Get product name
                                name_elem = elem.find(['h3', 'h2', 'h1']) or elem.find(attrs={'title': True})
                                name = name_elem.get('title') or name_elem.text.strip() if name_elem else ''

                                # Get price
                                price_elem = elem.find(class_=re.compile(r'price', re.I))
                                price = price_elem.text.strip() if price_elem else ''

                                # Get URL
                                link_elem = elem.find('a', href=True)
                                url = urljoin(category_url, link_elem['href']) if link_elem else ''

                                if name and len(name) > 5:
                                    product = {
                                        "source": "lazada_html",
                                        "product_name": name,
                                        "price_current": price,
                                        "url": url,
                                        "crawl_date": datetime.now().isoformat()
                                    }
                                    products.append(product)

                            except Exception as e:
                                continue

                        if products:
                            print(f"Extracted {len(products)} products from Lazada HTML")
                            return products

                print("No products found in Lazada category")

        except Exception as e:
            print(f"Lazada category error: {e}")

        return []

    def crawl_seeds_urls(self):
        """Crawl all seed URLs and extract products"""
        print("=== CRAWLING ALL SEED URLs ===")

        # Load seeds
        seeds_file = os.path.join(os.path.dirname(__file__), 'seeds.txt')
        seed_categories = {}

        try:
            with open(seeds_file, 'r', encoding='utf-8') as f:
                current_category = None
                for line in f:
                    line = line.strip()
                    if line.startswith('#'):
                        current_category = line[1:].strip()
                    elif line.startswith('http') and current_category:
                        seed_categories[current_category] = line
        except Exception as e:
            print(f"Error loading seeds: {e}")
            return []

        print(f"Loaded {len(seed_categories)} seed categories")

        all_products = []

        # Try each category
        for category, url in seed_categories.items():
            print(f"\n--- Crawling: {category} ---")
            print(f"URL: {url}")

            # Try Lazada category
            lazada_products = self.extract_products_from_lazada_category(url, max_products=5)
            all_products.extend(lazada_products)

            # Wait between requests
            time.sleep(random.uniform(2, 4))

            # Stop if we get too many to avoid being blocked
            if len(all_products) >= 20:
                print(f"Reached 20 products, stopping to avoid blocking")
                break

        return all_products

    def mixed_crawl_demo(self):
        """Demo crawl mixing Tiki API and Lazada attempts"""
        print("=== MIXED CRAWL DEMO ===")

        all_products = []

        # 1. Get some products from Tiki (reliable)
        print("\n1. Getting products from Tiki...")
        tiki_products = self.extract_products_from_tiki("smartphone", limit=10)
        all_products.extend(tiki_products)

        time.sleep(2)

        # 2. Try Lazada seeds
        print("\n2. Trying Lazada seed URLs...")
        lazada_products = self.crawl_seeds_urls()
        all_products.extend(lazada_products)

        # 3. Summary and save
        print(f"\n=== CRAWL RESULTS ===")
        print(f"Tiki products: {len(tiki_products)}")
        print(f"Lazada products: {len(lazada_products)}")
        print(f"Total products: {len(all_products)}")

        if all_products:
            # Save results
            filename = f"mixed_crawl_results_{int(time.time())}.json"
            filepath = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(all_products, f, ensure_ascii=False, indent=2)

            print(f"Results saved to: {filename}")

            # Show samples
            print(f"\nSample products:")
            for i, product in enumerate(all_products[:5]):
                name = product.get('product_name', 'N/A')[:50]
                price = product.get('price_current', 'N/A')
                source = product.get('source', 'N/A')
                print(f"  {i+1}. [{source}] {name}... | {price}")

            return all_products

        else:
            print("No products extracted")
            return []

def main():
    """Run the working crawler"""
    crawler = WorkingRequestsCrawler()

    try:
        products = crawler.mixed_crawl_demo()

        if products:
            print(f"\n SUCCESS! Extracted {len(products)} products")
            print("The crawler is working! You can now:")
            print("1. Scale up the number of products")
            print("2. Add more categories")
            print("3. Add pagination support")
        else:
            print("\n No products extracted - need further adjustment")

    except Exception as e:
        print(f"Crawl error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()