#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import random
import sys
import requests
from typing import List, Dict, Optional
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

class FPTShopRequestsCrawler:
    def __init__(self):
        self.source_name = "fptshop"
        self.base_url = "https://fptshop.com.vn"

        # Advanced headers to bypass detection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Sample product URLs for testing
        self.test_urls = [
            "https://fptshop.com.vn/dien-thoai/iphone-16-pro-max",
            "https://fptshop.com.vn/dien-thoai/samsung-galaxy-s24-ultra",
            "https://fptshop.com.vn/dien-thoai/xiaomi-redmi-note-13",
            "https://fptshop.com.vn/dien-thoai/oppo-reno12-f",
            "https://fptshop.com.vn/dien-thoai/vivo-y28-5g"
        ]

    def get_page_content(self, url: str) -> Optional[str]:
        """Get page content with multiple retry strategies"""
        try:
            # Try direct request first
            response = self.session.get(url, timeout=10)

            if response.status_code == 200:
                content = response.text

                # Check if blocked by Cloudflare
                if 'cloudflare' in content.lower() or 'captcha' in content.lower():
                    print(f"  Cloudflare detected, trying alternative...")

                    # Try with different headers
                    alt_headers = self.headers.copy()
                    alt_headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'

                    response = self.session.get(url, headers=alt_headers, timeout=10)
                    if response.status_code == 200:
                        content = response.text

                return content
            else:
                print(f"  HTTP {response.status_code}")
                return None

        except Exception as e:
            print(f"  Request error: {e}")
            return None

    def clean_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price from text"""
        if not price_text:
            return None

        # Remove all non-digit characters
        price_clean = re.sub(r'[^\d]', '', price_text)

        try:
            return int(price_clean) if price_clean else None
        except ValueError:
            return None

    def extract_product_data(self, url: str, content: str) -> Optional[Dict]:
        """Extract product data from HTML content"""
        try:
            soup = BeautifulSoup(content, 'html.parser')

            # Product ID from URL
            product_id = url.split('/')[-1].split('?')[0]

            # Product name - multiple selectors
            product_name = self._extract_name(soup, content)

            # Brand
            brand = self._extract_brand(url, product_name)

            # Prices
            price_current, price_original = self._extract_prices(soup, content)

            # Images
            image_urls = self._extract_images(soup)

            # Calculate discount
            discount_percent = None
            if price_current and price_original and price_original > price_current:
                discount_percent = round(((price_original - price_current) / price_original) * 100, 2)

            product_data = {
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

            return product_data

        except Exception as e:
            print(f"  Parse error: {e}")
            return None

    def _extract_name(self, soup, content: str) -> str:
        """Extract product name"""
        # Try BeautifulSoup first
        selectors = [
            'h1[data-testid="product-name"]',
            'h1.fs-dttitle',
            'h1.product-title',
            'h1'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element and element.get_text().strip():
                name = element.get_text().strip()
                if len(name) > 3 and 'fptshop' not in name.lower():
                    return name

        # Fallback to regex
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
        """Extract brand from URL or name"""
        brands = ['iPhone', 'Apple', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'Huawei', 'Nokia', 'Realme', 'OnePlus', 'Honor', 'Tecno']

        # Check URL
        for brand in brands:
            if brand.lower() in url.lower():
                return brand

        # Check name
        for brand in brands:
            if brand.lower() in name.lower():
                return brand

        return "N/A"

    def _extract_prices(self, soup, content: str) -> tuple:
        """Extract current and original prices"""
        price_current = None
        price_original = None

        # Try BeautifulSoup selectors
        current_selectors = [
            '[data-testid="product-price"]',
            '.Price_currentPrice__PBYcv',
            '.fs-dtprice-special',
            '.price-current'
        ]

        for selector in current_selectors:
            element = soup.select_one(selector)
            if element:
                price = self.clean_price(element.get_text())
                if price and price > 1000:
                    price_current = price
                    break

        # Try regex patterns for current price
        if not price_current:
            patterns = [
                r'₫\s*([0-9.,]+)',
                r'"price":\s*"?([0-9.,]+)"?',
                r'class="[^"]*price[^"]*"[^>]*>₫?\s*([0-9.,]+)',
                r'data-price="([0-9.,]+)"'
            ]

            for pattern in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    price = self.clean_price(match)
                    if price and price > 1000:
                        price_current = price
                        break
                if price_current:
                    break

        # Original price
        original_selectors = [
            '[data-testid="product-original-price"]',
            '.Price_originalPrice__abc',
            '.fs-dtprice-old',
            '.line-through'
        ]

        for selector in original_selectors:
            element = soup.select_one(selector)
            if element:
                price = self.clean_price(element.get_text())
                if price and price > (price_current or 0):
                    price_original = price
                    break

        return price_current, price_original

    def _extract_images(self, soup) -> List[str]:
        """Extract product images"""
        image_urls = []

        # Find images
        img_tags = soup.find_all('img', src=True)

        for img in img_tags[:5]:  # Limit to 5
            src = img.get('src')
            if src and ('fptshop' in src or src.startswith('/')):
                if src.startswith('/'):
                    src = urljoin(self.base_url, src)
                if src not in image_urls:
                    image_urls.append(src)

        return image_urls

    def crawl_products(self, urls: List[str] = None, max_products: int = 5) -> List[Dict]:
        """Crawl products using requests"""
        if not urls:
            urls = self.test_urls[:max_products]

        products_data = []
        successful = 0

        print(f"Starting requests-based crawl of {len(urls)} products...")

        for i, url in enumerate(urls, 1):
            print(f"[{i}/{len(urls)}] {url}")

            # Get page content
            content = self.get_page_content(url)

            if content:
                # Extract data
                product_data = self.extract_product_data(url, content)

                if product_data:
                    products_data.append(product_data)
                    if product_data['price_current']:
                        successful += 1
                        print(f"  OK: {product_data['product_name']} - {product_data['price_current']:,} VND")
                    else:
                        print(f"  OK: {product_data['product_name']} - No price")
                else:
                    print(f"  FAIL: Could not extract data")
            else:
                print(f"  FAIL: Could not get page content")

            # Short delay
            time.sleep(random.uniform(1, 2))

        print(f"\nResults:")
        print(f"- Total products: {len(products_data)}")
        print(f"- With prices: {successful}")
        print(f"- Success rate: {successful/len(products_data)*100:.1f}%" if products_data else "0%")

        return products_data

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
        print("=== FPTShop Requests Crawler ===")
        print("Using requests + BeautifulSoup (faster, no browser)")

        crawler = FPTShopRequestsCrawler()
        products_data = crawler.crawl_products(max_products=5)

        if products_data:
            timestamp = int(time.time())
            filename = f"fptshop_requests_test_{timestamp}.json"
            crawler.save_data(products_data, filename)

        return products_data

    except KeyboardInterrupt:
        print("\nStopped by user")
        return []
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    main()