#!/usr/bin/env python3
"""
Lazada Session-Based Crawler
============================
Ultra-conservative crawler using persistent sessions and human-like patterns
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
import os
import sys

class LazadaSessionCrawler:
    def __init__(self, mode="ultra_slow"):
        self.base_url = "https://www.lazada.vn"
        self.mode = mode

        # Create persistent session
        self.session = self.create_persistent_session()

        # Ultra-conservative timing
        self.delay_ranges = {
            "ultra_slow": (120, 300),  # 2-5 minutes between requests
            "very_slow": (90, 180),    # 1.5-3 minutes
            "slow": (60, 120)          # 1-2 minutes
        }

        # Statistics
        self.stats = {
            'requests_made': 0,
            'products_found': 0,
            'anti_bot_blocks': 0,
            'successful_requests': 0,
            'session_resets': 0
        }

    def create_persistent_session(self):
        """Create a persistent session with realistic browser fingerprint"""
        session = requests.Session()

        # Choose a consistent user agent for this session
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

        # Realistic headers
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }

        session.headers.update(headers)
        return session

    def reset_session(self):
        """Reset session if blocked"""
        print("Resetting session due to blocking...")
        self.session.close()
        self.session = self.create_persistent_session()
        self.stats['session_resets'] += 1

        # Long delay after reset
        reset_delay = random.uniform(180, 300)  # 3-5 minutes
        print(f"Session reset delay: {reset_delay:.1f}s")
        time.sleep(reset_delay)

    def human_delay(self):
        """Implement ultra-human-like delays"""
        base_delay = random.uniform(*self.delay_ranges[self.mode])

        # Add extra delay if we've been blocked
        if self.stats['anti_bot_blocks'] > 0:
            base_delay *= (1.8 + self.stats['anti_bot_blocks'] * 0.5)

        # Add random "thinking time"
        thinking_time = random.uniform(5, 30)
        total_delay = base_delay + thinking_time

        print(f"Human-like delay: {total_delay:.1f}s (base: {base_delay:.1f}s + thinking: {thinking_time:.1f}s)")
        time.sleep(total_delay)

    def make_human_request(self, url, max_retries=2):
        """Make request with human-like behavior"""
        for attempt in range(max_retries):
            try:
                print(f"Human request attempt {attempt + 1}: {url}")

                # Sometimes add a referer from previous "browsing"
                if random.random() > 0.5 and attempt == 0:
                    self.session.headers['Referer'] = self.base_url + '/'

                # Make request
                response = self.session.get(
                    url,
                    timeout=60,  # Long timeout for slow connections
                    allow_redirects=True
                )

                self.stats['requests_made'] += 1

                # Check for blocking
                if self.is_blocked(response):
                    self.stats['anti_bot_blocks'] += 1
                    print(f"   Blocked on attempt {attempt + 1}")

                    if attempt < max_retries - 1:
                        # Reset session and try again
                        self.reset_session()
                        continue
                    else:
                        raise Exception("Persistent blocking detected")

                if response.status_code == 200:
                    self.stats['successful_requests'] += 1
                    print(f"  Success: {response.status_code} ({len(response.text)} chars)")
                    return response
                else:
                    print(f"  HTTP {response.status_code}")

            except Exception as e:
                print(f"  Request error: {e}")
                if attempt < max_retries - 1:
                    error_delay = random.uniform(60, 120)
                    print(f"  Waiting {error_delay:.1f}s before retry...")
                    time.sleep(error_delay)
                    continue

        raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

    def is_blocked(self, response):
        """Detect anti-bot blocking with comprehensive checks"""
        # Status code check
        if response.status_code in [403, 429, 503, 520, 521, 522, 523, 524]:
            return True

        # Content length check - blocked pages are usually very short
        if len(response.text) < 1000:
            return True

        # Content analysis
        content = response.text.lower()
        block_indicators = [
            'access denied', 'blocked', 'captcha', 'security check',
            'robot', 'automation', 'suspicious activity',
            'temporarily unavailable', 'please verify you are human',
            'cloudflare', 'checking your browser', 'ddos protection',
            'ray id', 'error 1020', 'attention required',
            'browser checking', 'challenge', 'verification',
            'are you human', 'prove you are human'
        ]

        return any(indicator in content for indicator in block_indicators)

    def extract_products_carefully(self, html_content, url):
        """Carefully extract products with multiple fallback strategies"""
        soup = BeautifulSoup(html_content, 'html.parser')
        products = []

        # Multiple selector strategies
        selectors = [
            'a[href*="/products/"]',
            'a[href*="-i"]',
            '[data-qa-locator*="product"] a',
            '.c16H9d a',
            '.Bm3ON a',
            '.product-item a',
            '.item a',
            'a[title]'  # Any link with a title
        ]

        found_urls = set()

        for selector in selectors:
            try:
                elements = soup.select(selector)
                print(f"Trying selector '{selector}': found {len(elements)} elements")

                for elem in elements:
                    href = elem.get('href')
                    if not href:
                        continue

                    # Clean and validate URL
                    if href.startswith('/'):
                        href = urljoin(self.base_url, href)
                    elif href.startswith('//'):
                        href = 'https:' + href
                    elif not href.startswith('http'):
                        continue

                    # Check if it's a product URL
                    if any(pattern in href for pattern in ['/products/', '-i']):
                        clean_url = href.split('?')[0].split('#')[0]
                        found_urls.add(clean_url)

                if found_urls:
                    print(f"Success with selector '{selector}': {len(found_urls)} URLs")
                    break

            except Exception as e:
                print(f"Error with selector '{selector}': {e}")
                continue

        # Convert URLs to products
        for url in found_urls:
            try:
                # Extract product info
                url_parts = url.split('/')
                product_name = url_parts[-1] if url_parts else "Unknown"

                # Extract product ID
                product_id_match = re.search(r'-i(\d+)', url)
                product_id = product_id_match.group(1) if product_id_match else str(abs(hash(url)))

                product = {
                    'source': 'lazada_session',
                    'product_id': product_id,
                    'url': url,
                    'title': product_name.replace('-', ' ').replace('.html', '').title(),
                    'extracted_at': datetime.now().isoformat(),
                    'page_url': url,
                    'extraction_method': 'session_crawling'
                }
                products.append(product)

            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                continue

        self.stats['products_found'] += len(products)
        return products

    def crawl_category_human(self, category="mobiles", max_pages=2):
        """Crawl with ultra-human behavior"""
        print(f"\n  Starting HUMAN-LIKE Lazada crawl: {category}")
        print(f"Mode: {self.mode}, Max pages: {max_pages}")
        print("Warning: This will be VERY slow (2-5 minutes per request)")

        all_products = []
        successful_pages = 0

        # Category URLs
        category_urls = {
            "mobiles": "https://www.lazada.vn/dien-thoai-di-dong/",
            "smartphones": "https://www.lazada.vn/dien-thoai-di-dong/",
            "laptops": "https://www.lazada.vn/may-tinh-xach-tay/",
            "tablets": "https://www.lazada.vn/may-tinh-bang/",
            "headphones": "https://www.lazada.vn/tai-nghe/",
            "cameras": "https://www.lazada.vn/may-anh-may-quay/"
        }

        base_url = category_urls.get(category, f"https://www.lazada.vn/catalog/?q={category}")

        # Start with homepage like a human would
        try:
            print("\n Phase 1: Visiting homepage like a human...")
            homepage_response = self.make_human_request("https://www.lazada.vn/")
            print(" Homepage visit completed successfully")

            # Simulate reading the page
            reading_time = random.uniform(10, 30)
            print(f" Simulating reading homepage for {reading_time:.1f}s...")
            time.sleep(reading_time)

        except Exception as e:
            print(f" Homepage visit failed: {e}")
            print(" Continuing to category pages...")

        # Navigate to category
        print(f"\n Phase 2: Navigating to {category} category...")
        self.human_delay()

        # Crawl pages
        for page in range(1, max_pages + 1):
            try:
                # Build page URL
                if '?' in base_url:
                    page_url = f"{base_url}&page={page}"
                else:
                    page_url = f"{base_url}?page={page}"

                print(f"\n Page {page}/{max_pages}: {page_url}")

                # Make human-like request
                response = self.make_human_request(page_url)

                # Simulate reading the page
                reading_time = random.uniform(5, 15)
                print(f" Simulating page reading for {reading_time:.1f}s...")
                time.sleep(reading_time)

                # Extract products
                page_products = self.extract_products_carefully(response.text, page_url)

                if page_products:
                    all_products.extend(page_products)
                    successful_pages += 1
                    print(f"    Page {page}: Found {len(page_products)} products")
                else:
                    print(f"     Page {page}: No products found")

                # Human delay between pages
                if page < max_pages:
                    print(f"  Preparing for next page...")
                    self.human_delay()

            except Exception as e:
                print(f"    Page {page} failed: {e}")

                # Don't give up entirely
                if page < max_pages:
                    print("    Long delay before trying next page...")
                    time.sleep(random.uniform(300, 600))  # 5-10 minute delay
                continue

        # Summary
        print(f"\n HUMAN CRAWL SUMMARY:")
        print(f"   Category: {category}")
        print(f"   Successful pages: {successful_pages}/{max_pages}")
        print(f"   Total products: {len(all_products)}")
        print(f"   Anti-bot blocks: {self.stats['anti_bot_blocks']}")
        print(f"   Session resets: {self.stats['session_resets']}")
        print(f"   Successful requests: {self.stats['successful_requests']}")
        print(f"   Total requests: {self.stats['requests_made']}")

        success_rate = (self.stats['successful_requests'] / max(self.stats['requests_made'], 1)) * 100
        print(f"   Success rate: {success_rate:.1f}%")

        return all_products

    def save_data(self, products, filename=None):
        """Save products to file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lazada_session_{timestamp}.jsonl"

        try:
            # Create output directory
            output_dir = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/crawler_outputs")
            os.makedirs(output_dir, exist_ok=True)

            filepath = os.path.join(output_dir, filename)

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
    """Test human-like crawler"""
    print("==> LAZADA HUMAN-LIKE SESSION CRAWLER")
    print("=" * 50)
    print("WARNING: This crawler is EXTREMELY slow but highly evasive")
    print("Expected time: 10-20 minutes for 2 pages")

    try:
        # Create human crawler
        crawler = LazadaSessionCrawler(mode="ultra_slow")

        # Test with minimal pages
        products = crawler.crawl_category_human("mobiles", max_pages=1)

        if products and len(products) > 0:
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lazada_session_{timestamp}.jsonl"
            saved_file = crawler.save_data(products, filename)

            print(f"\n SUCCESS: Found {len(products)} products!")
            print(f" Saved to: {saved_file}")
            return 0
        else:
            print(f"\n FAILED: No products found")
            print("💡 The site may have very strong anti-bot protection")
            return 1

    except Exception as e:
        print(f"\n ERROR: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)