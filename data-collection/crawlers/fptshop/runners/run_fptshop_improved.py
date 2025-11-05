#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import random
import sys
import requests
import concurrent.futures
from typing import List, Dict, Optional
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import threading

class FPTShopSuperCrawler:
    def __init__(self):
        self.source_name = "fptshop"
        self.base_url = "https://fptshop.com.vn"

        # Rotating headers to avoid detection
        self.headers_list = [
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            },
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
            }
        ]

        self.session_lock = threading.Lock()
        self.sessions = []
        self._init_sessions()

    def _init_sessions(self):
        """Initialize multiple sessions with different headers"""
        for headers in self.headers_list:
            session = requests.Session()
            session.headers.update(headers)
            self.sessions.append(session)

    def get_session(self):
        """Get a random session"""
        with self.session_lock:
            return random.choice(self.sessions)

    def get_page_content(self, url: str, retries: int = 2) -> Optional[str]:
        """Get page content with retry"""
        for attempt in range(retries):
            try:
                session = self.get_session()
                time.sleep(random.uniform(0.5, 1.0))

                response = session.get(url, timeout=10)
                if response.status_code == 200:
                    return response.text
            except Exception as e:
                if attempt == retries - 1:
                    print(f"  Request failed: {e}")
                time.sleep(1)
        return None

    def clean_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price from text"""
        if not price_text:
            return None

        price_clean = re.sub(r'[^\d]', '', price_text)
        try:
            price = int(price_clean) if price_clean else None
            if price and 10000 <= price <= 200000000:
                return price
        except ValueError:
            pass
        return None

    def extract_product_data(self, url: str) -> Optional[Dict]:
        """Extract product data"""
        content = self.get_page_content(url)
        if not content:
            return None

        try:
            soup = BeautifulSoup(content, 'html.parser')

            product_id = url.split('/')[-1].split('?')[0]
            product_name = self._extract_name(soup, content)
            brand = self._extract_brand(url, product_name)
            price_current, price_original = self._extract_prices(soup, content)
            image_urls = self._extract_images(soup)

            discount_percent = None
            if price_current and price_original and price_original > price_current:
                discount_percent = round(((price_original - price_current) / price_original) * 100, 2)

            return {
                "source": self.source_name,
                "product_id": product_id,
                "url": url,
                "crawl_date": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "product_name": product_name,
                "brand": brand,
                "category": "Dien tu",
                "description": "N/A",
                "image_urls": image_urls,
                "price_current": price_current,
                "price_original": price_original,
                "discount_percent": discount_percent,
                "rating_avg": None,
                "rating_count": None,
                "sold_count": None,
                "favorite_count": None,
                "seller_name": "FPTShop",
                "seller_type": "Retail"
            }
        except Exception as e:
            print(f"  Parse error: {e}")
            return None

    def _extract_name(self, soup, content: str) -> str:
        """Extract product name"""
        selectors = [
            'h1[data-testid="pdp-product-name"]',
            'h1.fs-dttitle',
            'h1.product-title',
            'h1'
        ]

        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    name = element.get_text().strip()
                    if len(name) > 3 and 'fptshop' not in name.lower():
                        return name
            except:
                continue

        # Regex fallback
        patterns = [
            r'<h1[^>]*>([^<]+)</h1>',
            r'"name":\s*"([^"]+)"',
            r'<title>([^<]+?)\s*-\s*FPTShop'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if len(name) > 3:
                    return name

        return "N/A"

    def _extract_brand(self, url: str, name: str) -> str:
        """Extract brand"""
        brands = ['iPhone', 'Apple', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'Nokia', 'Realme', 'Honor', 'Tecno']

        for brand in brands:
            if brand.lower() in url.lower() or brand.lower() in name.lower():
                return brand
        return "N/A"

    def _extract_prices(self, soup, content: str) -> tuple:
        """Extract prices with improved patterns"""
        price_current = None
        price_original = None

        # Current price
        current_selectors = [
            '[data-testid="product-price"]',
            '.Price_currentPrice__PBYcv',
            '.fs-dtprice-special',
            '.price-current'
        ]

        for selector in current_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    price = self.clean_price(element.get_text())
                    if price:
                        price_current = price
                        break
                if price_current:
                    break
            except:
                continue

        # Regex patterns for price
        if not price_current:
            price_patterns = [
                r'₫\s*([0-9.,]+)',
                r'"price":\s*"?([0-9.,]+)"?',
                r'Gia:\s*([0-9.,]+)',
                r'>([0-9.,]+)\s*₫<'
            ]

            for pattern in price_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    price = self.clean_price(match)
                    if price:
                        price_current = price
                        break
                if price_current:
                    break

        # Original price
        original_selectors = [
            '[data-testid="product-original-price"]',
            '.fs-dtprice-old',
            '.line-through'
        ]

        for selector in original_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    price = self.clean_price(element.get_text())
                    if price and price > (price_current or 0):
                        price_original = price
                        break
                if price_original:
                    break
            except:
                continue

        return price_current, price_original

    def _extract_images(self, soup) -> List[str]:
        """Extract product images (filter out icons)"""
        image_urls = []

        # Look for actual product images
        selectors = [
            '.fs-dtimage img',
            '.product-gallery img',
            '.ProductImage img'
        ]

        for selector in selectors:
            try:
                images = soup.select(selector)
                for img in images:
                    src = img.get('src') or img.get('data-src')
                    if src and self._is_product_image(src):
                        full_url = urljoin(self.base_url, src) if src.startswith('/') else src
                        if full_url not in image_urls:
                            image_urls.append(full_url)
                            if len(image_urls) >= 3:
                                return image_urls
            except:
                continue

        return image_urls

    def _is_product_image(self, url: str) -> bool:
        """Filter out promotional icons"""
        if not url:
            return False

        url_lower = url.lower()

        # Exclude promotional icons
        exclude = ['icon_', 'logo_', 'mua_1_tang_1', 'vong_quay', 'sim_du_lich', 'ban_sao_logo']
        if any(exc in url_lower for exc in exclude):
            return False

        # Include actual product images
        include = ['iphone', 'samsung', 'xiaomi', 'phone', '360x', '720x']
        return any(inc in url_lower for inc in include) or len(url) > 80

    def save_data(self, products_data: List[Dict], filename: str):
        """Save data to JSON file"""
        import os
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products_data, f, ensure_ascii=False, indent=2)

        print(f"Saved to: {filepath}")

def main():
    try:
        print("=== FPTShop Super Crawler ===")

        # Test URLs
        test_urls = [
            "https://fptshop.com.vn/dien-thoai/iphone-16-pro-max",
            "https://fptshop.com.vn/dien-thoai/samsung-galaxy-s24-ultra",
            "https://fptshop.com.vn/dien-thoai/xiaomi-redmi-note-13",
            "https://fptshop.com.vn/dien-thoai/oppo-reno12-f",
            "https://fptshop.com.vn/dien-thoai/vivo-y28-5g",
            "https://fptshop.com.vn/dien-thoai/honor-x9b",
            "https://fptshop.com.vn/dien-thoai/realme-c67",
            "https://fptshop.com.vn/dien-thoai/nokia-g42",
            "https://fptshop.com.vn/dien-thoai/tecno-spark-30"
        ]

        crawler = FPTShopSuperCrawler()
        products_data = []
        successful = 0

        print(f"Testing {len(test_urls)} products...")

        for i, url in enumerate(test_urls, 1):
            print(f"[{i}/{len(test_urls)}] {url}")

            product_data = crawler.extract_product_data(url)
            if product_data:
                products_data.append(product_data)
                if product_data['price_current']:
                    successful += 1
                    print(f"  OK: {product_data['product_name']} - {product_data['price_current']:,} VND")
                else:
                    print(f"  OK: {product_data['product_name']} - No price")
            else:
                print(f"  FAIL: Could not extract data")

            time.sleep(random.uniform(1, 2))

        if products_data:
            timestamp = int(time.time())
            filename = f"fptshop_super_test_{timestamp}.json"
            crawler.save_data(products_data, filename)

            print(f"\n=== Results ===")
            print(f"Total: {len(products_data)}")
            print(f"With prices: {successful}")
            print(f"Success rate: {successful/len(products_data)*100:.1f}%")

        return products_data

    except KeyboardInterrupt:
        print("\nStopped")
        return []
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    main()