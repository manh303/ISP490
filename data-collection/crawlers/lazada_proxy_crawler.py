#!/usr/bin/env python3
"""
Lazada Proxy Rotation Crawler
=============================
Advanced anti-bot evasion using proxy rotation and stealth techniques
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

class LazadaProxyCrawler:
    def __init__(self, mode="stealth"):
        self.base_url = "https://www.lazada.vn"
        self.mode = mode

        # Free proxy endpoints (for testing)
        self.proxy_sources = [
            "https://www.proxy-list.download/api/v1/get?type=http",
            "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all",
        ]

        # Collected proxies
        self.proxies = []
        self.current_proxy_idx = 0

        # MASSIVELY increased timing for stealth - Lazada is very aggressive
        self.delay_ranges = {
            "stealth": (60, 120),   # 1-2 minutes between requests
            "slow": (45, 90),       # 45s-1.5m delays
            "normal": (30, 60)      # 30s-1m minimum delays
        }

        # Multiple user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]

        # Statistics
        self.stats = {
            'requests_made': 0,
            'products_found': 0,
            'anti_bot_blocks': 0,
            'proxy_failures': 0,
            'successful_requests': 0
        }

        # Initialize proxies
        self.setup_proxies()

    def setup_proxies(self):
        """Setup proxy rotation - fallback to direct connection"""
        print("Setting up proxy rotation...")

        # For demo/testing, use direct connection with stealth timing
        self.proxies = [None]  # None means direct connection
        print("Using direct connection with enhanced stealth timing")

        # In production, you would collect working proxies here:
        # self.collect_proxies()

    def collect_proxies(self):
        """Collect working proxies from free sources"""
        # This would implement actual proxy collection
        # For now, using direct connection only
        pass

    def get_proxy(self):
        """Get next proxy in rotation"""
        if not self.proxies:
            return None

        proxy = self.proxies[self.current_proxy_idx]
        self.current_proxy_idx = (self.current_proxy_idx + 1) % len(self.proxies)
        return proxy

    def create_session(self):
        """Create session with random fingerprint"""
        session = requests.Session()

        # Random user agent
        user_agent = random.choice(self.user_agents)

        # Comprehensive headers
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': f'"{random.choice(["Windows", "macOS", "Linux"])}"'
        }

        session.headers.update(headers)
        return session

    def stealth_delay(self):
        """Implement stealth delays"""
        base_delay = random.uniform(*self.delay_ranges[self.mode])

        # Add additional delay if we've been blocked
        if self.stats['anti_bot_blocks'] > 0:
            base_delay *= (1.5 + self.stats['anti_bot_blocks'] * 0.3)

        # Random jitter
        jitter = random.uniform(-2, 5)
        delay = max(3, base_delay + jitter)

        print(f"Stealth delay: {delay:.1f}s")
        time.sleep(delay)

    def make_stealth_request(self, url, max_retries=2):
        """Make request with maximum stealth"""
        for attempt in range(max_retries):
            session = self.create_session()
            proxy = self.get_proxy()

            try:
                print(f"Stealth attempt {attempt + 1}: {url}")

                # Setup proxy
                proxies = None
                if proxy:
                    proxies = {
                        'http': proxy,
                        'https': proxy
                    }

                # Add random referer
                referers = [
                    'https://www.google.com.vn/',
                    'https://www.google.com/',
                    'https://www.bing.com/',
                    'https://duckduckgo.com/',
                    'https://www.lazada.vn/',
                    'https://www.facebook.com/',
                    'https://www.youtube.com/',
                    '',  # No referer sometimes
                ]
                if random.random() > 0.3:  # 70% chance to add referer
                    referer = random.choice(referers)
                    if referer:
                        session.headers['Referer'] = referer

                # Add random additional headers to look more human
                if random.random() > 0.5:
                    session.headers['Accept-CH'] = 'UA, Platform'
                if random.random() > 0.6:
                    session.headers['Pragma'] = 'no-cache'
                if random.random() > 0.7:
                    session.headers['X-Requested-With'] = 'XMLHttpRequest'

                # Make request with extended timeout
                response = session.get(
                    url,
                    timeout=45,  # Longer timeout
                    proxies=proxies,
                    allow_redirects=True,
                    stream=False  # Don't stream to avoid connection issues
                )

                self.stats['requests_made'] += 1

                # Check for blocking
                if self.is_blocked(response):
                    self.stats['anti_bot_blocks'] += 1
                    print(f"   Anti-bot detected on attempt {attempt + 1}")

                    if attempt < max_retries - 1:
                        block_delay = random.uniform(30, 60)  # Long delay after blocking
                        print(f"   Waiting {block_delay:.1f}s before retry...")
                        time.sleep(block_delay)
                        continue
                    else:
                        raise Exception("Persistent anti-bot blocking")

                if response.status_code == 200:
                    self.stats['successful_requests'] += 1
                    print(f"  Success: {response.status_code}")
                    return response
                else:
                    print(f"  HTTP {response.status_code}")

            except requests.exceptions.ProxyError:
                self.stats['proxy_failures'] += 1
                print(f"  Proxy failed: {proxy}")
                continue
            except requests.exceptions.Timeout:
                print(f"   Request timeout")
                continue
            except Exception as e:
                print(f"  Request error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(10, 20))
                    continue

        raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

    def is_blocked(self, response):
        """Enhanced blocking detection"""
        # Status code check
        if response.status_code in [403, 429, 503, 520, 521, 522, 523, 524]:
            return True

        # Content analysis
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
            'please verify you are human',
            'cloudflare',
            'checking your browser',
            'ddos protection',
            'ray id',
            'error 1020',
            'attention required',
            'browser checking',
            'challenge'
        ]

        return any(indicator in content for indicator in block_indicators)

    def extract_products_smart(self, html_content, url):
        """Smart product extraction with multiple strategies"""
        soup = BeautifulSoup(html_content, 'html.parser')
        products = []

        # Strategy 1: Search for various product link patterns
        selectors = [
            'a[href*="/products/"]',
            'a[href*="-i"]',
            'a[href*="lazada.vn/"]',
            '[data-qa-locator="product-item"] a',
            '.c16H9d a',
            '.Bm3ON a',
            '.product-item a',
            '.item a'
        ]

        found_urls = set()

        for selector in selectors:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    href = elem.get('href')
                    if href:
                        # Clean URL
                        if href.startswith('/'):
                            href = urljoin(self.base_url, href)
                        elif href.startswith('//'):
                            href = 'https:' + href
                        elif not href.startswith('http'):
                            continue

                        # Validate product URL
                        if any(pattern in href for pattern in ['/products/', '-i', 'lazada.vn']):
                            clean_url = href.split('?')[0].split('#')[0]
                            found_urls.add(clean_url)

                if found_urls:
                    print(f"Found {len(found_urls)} product URLs with selector: {selector}")
                    break
            except Exception as e:
                continue

        # Strategy 2: Look for JSON data
        if not found_urls:
            try:
                scripts = soup.find_all('script', type='application/ld+json')
                for script in scripts:
                    try:
                        data = json.loads(script.string)
                        # Extract URLs from JSON-LD structured data
                        if isinstance(data, dict) and 'url' in data:
                            found_urls.add(data['url'])
                    except:
                        continue
            except Exception:
                pass

        # Convert URLs to product objects
        for url in found_urls:
            try:
                # Extract product information
                url_parts = url.split('/')
                product_name = url_parts[-1] if url_parts else "Unknown"

                # Extract product ID
                product_id_match = re.search(r'-i(\d+)', url)
                product_id = product_id_match.group(1) if product_id_match else str(abs(hash(url)))

                product = {
                    'source': 'lazada_proxy',
                    'product_id': product_id,
                    'url': url,
                    'title': product_name.replace('-', ' ').replace('.html', '').title(),
                    'extracted_at': datetime.now().isoformat(),
                    'page_url': url,
                    'extraction_method': 'stealth_crawling'
                }
                products.append(product)

            except Exception as e:
                continue

        self.stats['products_found'] += len(products)
        return products

    def crawl_category_stealth(self, category="mobiles", max_pages=3):
        """Stealth crawl with maximum evasion"""
        print(f"\n  Starting STEALTH Lazada crawl: {category}")
        print(f"Mode: {self.mode}, Max pages: {max_pages}")

        all_products = []
        successful_pages = 0

        # Category mapping
        category_urls = {
            "mobiles": "https://www.lazada.vn/dien-thoai-di-dong/",
            "smartphones": "https://www.lazada.vn/dien-thoai-di-dong/",
            "laptops": "https://www.lazada.vn/may-tinh-xach-tay/",
            "tablets": "https://www.lazada.vn/may-tinh-bang/",
            "headphones": "https://www.lazada.vn/tai-nghe/",
            "cameras": "https://www.lazada.vn/may-anh-may-quay/"
        }

        base_url = category_urls.get(category, f"https://www.lazada.vn/catalog/?q={category}")

        # Warm up with homepage visit
        try:
            print(" Warming up with homepage...")
            homepage_response = self.make_stealth_request("https://www.lazada.vn/")
            print(" Homepage visit successful")
            self.stealth_delay()  # Long delay after homepage
        except Exception as e:
            print(f" Homepage failed: {e}")
            print(" Continuing without warmup...")

        # Crawl pages with maximum stealth
        for page in range(1, max_pages + 1):
            try:
                # Build page URL
                if '?' in base_url:
                    page_url = f"{base_url}&page={page}"
                else:
                    page_url = f"{base_url}?page={page}"

                print(f"\n Page {page}/{max_pages}: {page_url}")

                # Make stealth request
                response = self.make_stealth_request(page_url)

                # Extract products
                page_products = self.extract_products_smart(response.text, page_url)

                if page_products:
                    all_products.extend(page_products)
                    successful_pages += 1
                    print(f"    Page {page}: Found {len(page_products)} products")
                else:
                    print(f"     Page {page}: No products found")

                # Long stealth delay between pages
                if page < max_pages:
                    self.stealth_delay()

            except Exception as e:
                print(f"    Page {page} failed: {e}")

                # Don't give up, wait and try next page
                if page < max_pages:
                    print("    Waiting before next page...")
                    time.sleep(random.uniform(30, 60))
                continue

        # Summary
        print(f"\n STEALTH CRAWL SUMMARY:")
        print(f"   Category: {category}")
        print(f"   Successful pages: {successful_pages}/{max_pages}")
        print(f"   Total products: {len(all_products)}")
        print(f"   Anti-bot blocks: {self.stats['anti_bot_blocks']}")
        print(f"   Successful requests: {self.stats['successful_requests']}")
        print(f"   Total requests: {self.stats['requests_made']}")

        success_rate = (self.stats['successful_requests'] / max(self.stats['requests_made'], 1)) * 100
        print(f"   Success rate: {success_rate:.1f}%")

        return all_products

    def save_data(self, products, filename=None):
        """Save products to file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lazada_stealth_{timestamp}.jsonl"

        try:
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
    """Test stealth crawler"""
    print("==> LAZADA STEALTH PROXY CRAWLER")
    print("=" * 50)

    try:
        # Create stealth crawler
        crawler = LazadaProxyCrawler(mode="stealth")

        # Test crawl
        products = crawler.crawl_category_stealth("mobiles", max_pages=2)

        if products and len(products) > 0:
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lazada_stealth_{timestamp}.jsonl"
            saved_file = crawler.save_data(products, filename)

            print(f"\n SUCCESS: Found {len(products)} products!")
            print(f" Saved to: {saved_file}")
            return 0
        else:
            print(f"\n FAILED: No products found")
            print("💡 Try running again with longer delays or different timing")
            return 1

    except Exception as e:
        print(f"\n ERROR: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)