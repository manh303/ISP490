#!/usr/bin/env python3
"""
Optimized Lazada Crawler with Enhanced Anti-Bot Evasion
======================================================
Specialized crawler for bypassing Lazada's anti-bot detection
"""

import time
import random
import json
import requests
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re

class LazadaAntiBotCrawler:
    def __init__(self, mode="fast"):
        self.base_url = "https://www.lazada.vn"
        self.mode = mode  # fast, demo, mass

        # Anti-bot session with rotation
        self.sessions = []
        self.current_session_idx = 0
        self.create_session_pool()

        # Enhanced delays for anti-bot
        self.delay_ranges = {
            "fast": (3, 7),
            "demo": (5, 10),
            "mass": (8, 15)
        }

        # Statistics
        self.stats = {
            'requests_made': 0,
            'products_found': 0,
            'anti_bot_blocks': 0,
            'session_rotations': 0
        }

    def create_session_pool(self, pool_size=3):
        """Create multiple sessions with different fingerprints"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]

        for i in range(pool_size):
            session = requests.Session()

            # Randomize headers per session
            headers = {
                'User-Agent': user_agents[i % len(user_agents)],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Cache-Control': 'max-age=0',
            }

            # Add random additional headers
            if random.choice([True, False]):
                headers['Sec-CH-UA'] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
                headers['Sec-CH-UA-Mobile'] = '?0'
                headers['Sec-CH-UA-Platform'] = f'"{random.choice(["Windows", "macOS", "Linux"])}"'

            session.headers.update(headers)
            self.sessions.append(session)

        print(f"Created {pool_size} sessions for anti-bot evasion")

    def get_session(self):
        """Get current session with rotation"""
        session = self.sessions[self.current_session_idx]
        self.current_session_idx = (self.current_session_idx + 1) % len(self.sessions)
        if self.current_session_idx == 0:
            self.stats['session_rotations'] += 1
        return session

    def anti_bot_delay(self):
        """Smart delay based on mode and request history"""
        base_delay = random.uniform(*self.delay_ranges[self.mode])

        # Increase delay if we've been blocked recently
        if self.stats['anti_bot_blocks'] > 0:
            base_delay *= (1 + self.stats['anti_bot_blocks'] * 0.5)

        # Add random jitter
        jitter = random.uniform(-1, 2)
        delay = max(1, base_delay + jitter)

        print(f"Anti-bot delay: {delay:.1f}s")
        time.sleep(delay)

    def make_request(self, url, max_retries=3):
        """Make request with anti-bot handling"""
        for attempt in range(max_retries):
            session = self.get_session()

            try:
                print(f"Request attempt {attempt + 1}: {url}")

                # Add random referer
                if random.choice([True, False]):
                    session.headers['Referer'] = random.choice([
                        'https://www.google.com/',
                        'https://www.lazada.vn/',
                        'https://www.facebook.com/'
                    ])

                response = session.get(url, timeout=15)
                self.stats['requests_made'] += 1

                # Check for anti-bot indicators
                if self.is_blocked(response):
                    self.stats['anti_bot_blocks'] += 1
                    print(f" Anti-bot detection on attempt {attempt + 1}")

                    if attempt < max_retries - 1:
                        # Wait longer before retry
                        block_delay = random.uniform(10, 20)
                        print(f"Waiting {block_delay:.1f}s before retry...")
                        time.sleep(block_delay)
                        continue
                    else:
                        raise Exception("Blocked by anti-bot after all retries")

                if response.status_code == 200:
                    print(f" Success: {response.status_code}")
                    return response
                else:
                    print(f" HTTP {response.status_code}")

            except Exception as e:
                print(f" Request error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                    continue

        raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

    def is_blocked(self, response):
        """Detect anti-bot blocking"""
        if response.status_code in [403, 429, 503]:
            return True

        content = response.text.lower()
        block_indicators = [
            'access denied',
            'blocked',
            'captcha',
            'security check',
            'robot',
            'automation',
            'suspicious activity',
            'temporarily unavailable',
            'please verify you are human'
        ]

        return any(indicator in content for indicator in block_indicators)

    def extract_products_from_html(self, html_content, url):
        """Extract products from HTML using multiple strategies"""
        soup = BeautifulSoup(html_content, 'html.parser')
        products = []

        # Strategy 1: Look for product links
        product_selectors = [
            'a[href*="/products/"]',
            'a[href*="-i"]',
            '[data-qa-locator="product-item"] a',
            '.c16H9d a',
            '.Bm3ON a'
        ]

        found_urls = set()

        for selector in product_selectors:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    href = elem.get('href')
                    if href:
                        # Clean and validate URL
                        if href.startswith('/'):
                            href = urljoin(self.base_url, href)
                        elif not href.startswith('http'):
                            continue

                        # Check if it's a valid product URL
                        if ('/products/' in href or '-i' in href) and href not in found_urls:
                            found_urls.add(href.split('?')[0])  # Remove query params

                if found_urls:
                    print(f"Found {len(found_urls)} URLs with selector: {selector}")
                    break

            except Exception as e:
                print(f"Error with selector {selector}: {e}")
                continue

        # Convert to products format
        for url in found_urls:
            try:
                # Extract basic info from URL
                url_parts = url.split('/')
                product_name = url_parts[-1] if url_parts else "Unknown Product"

                # Try to extract product ID
                product_id_match = re.search(r'-i(\d+)', url)
                product_id = product_id_match.group(1) if product_id_match else "unknown"

                product = {
                    'source': 'lazada_antibot',
                    'product_id': product_id,
                    'url': url,
                    'title': product_name.replace('-', ' ').replace('.html', '').title(),
                    'extracted_at': datetime.now().isoformat(),
                    'extraction_method': 'url_parsing'
                }
                products.append(product)

            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                continue

        self.stats['products_found'] += len(products)
        return products

    def crawl_category(self, category="mobiles", max_pages=5):
        """Crawl a category with enhanced anti-bot evasion"""
        print(f"\n==> Starting anti-bot Lazada crawl: {category}")
        print(f"Mode: {self.mode}, Max pages: {max_pages}")

        all_products = []
        successful_pages = 0

        # Build base category URL
        category_urls = {
            "mobiles": "https://www.lazada.vn/dien-thoai-di-dong/",
            "laptops": "https://www.lazada.vn/may-tinh-xach-tay/",
            "smartphones": "https://www.lazada.vn/dien-thoai-di-dong/",
            "tablets": "https://www.lazada.vn/may-tinh-bang/"
        }

        base_url = category_urls.get(category, f"https://www.lazada.vn/catalog/?q={category}")

        # Visit homepage first to establish session
        try:
            print("Establishing session with homepage visit...")
            homepage_response = self.make_request("https://www.lazada.vn/")
            self.anti_bot_delay()
        except Exception as e:
            print(f"Homepage visit failed: {e}")

        for page in range(1, max_pages + 1):
            try:
                # Build page URL
                if '?' in base_url:
                    page_url = f"{base_url}&page={page}"
                else:
                    page_url = f"{base_url}?page={page}"

                print(f"\n Page {page}/{max_pages}: {page_url}")

                # Make request with anti-bot handling
                response = self.make_request(page_url)

                # Extract products
                page_products = self.extract_products_from_html(response.text, page_url)

                if page_products:
                    all_products.extend(page_products)
                    successful_pages += 1
                    print(f" Page {page}: Found {len(page_products)} products")
                else:
                    print(f" Page {page}: No products found")

                # Anti-bot delay between pages
                if page < max_pages:
                    self.anti_bot_delay()

            except Exception as e:
                print(f" Error on page {page}: {e}")

                # Don't stop entirely, try next page
                if page < max_pages:
                    print("Attempting next page after error delay...")
                    time.sleep(random.uniform(15, 25))
                continue

        # Print summary
        print(f"\n Crawl Summary:")
        print(f"  Category: {category}")
        print(f"  Successful pages: {successful_pages}/{max_pages}")
        print(f"  Total products: {len(all_products)}")
        print(f"  Anti-bot blocks: {self.stats['anti_bot_blocks']}")
        print(f"  Session rotations: {self.stats['session_rotations']}")
        print(f"  Total requests: {self.stats['requests_made']}")

        return all_products

    def save_data(self, products, filename=None):
        """Save products to file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lazada_antibot_{timestamp}.jsonl"

        try:
            import os
            data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
            os.makedirs(data_dir, exist_ok=True)

            filepath = os.path.join(data_dir, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                for product in products:
                    json.dump(product, f, ensure_ascii=False)
                    f.write('\n')

            print(f" Saved {len(products)} products to {filepath}")
            return filepath

        except Exception as e:
            print(f" Save error: {e}")
            return None

def main():
    """Test the anti-bot crawler with fallback strategy"""
    print("==> Advanced Lazada Anti-Bot Crawler")

    # Start with simple requests approach for Airflow compatibility
    crawler = None
    try:
        print("\nUsing REQUESTS method (no browser dependencies)")
        crawler = LazadaAntiBotCrawler(mode="fast", use_browser=False)

        # Test with mobile category - minimal pages
        products = crawler.crawl_category("mobiles", max_pages=2)

        if products and len(products) > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lazada_antibot_{timestamp}.jsonl"
            saved_file = crawler.save_data(products, filename)
            print(f"\nSUCCESS: {len(products)} products")
            print(f"Saved to: {saved_file}")
            return 0
        else:
            print("\nNo products found - anti-bot blocking detected")
            return 1

    except Exception as e:
        print(f"\nERROR: {e}")
        return 1

if __name__ == "__main__":
    main()