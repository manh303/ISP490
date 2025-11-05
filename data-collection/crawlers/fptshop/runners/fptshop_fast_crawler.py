#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import random
import sys
import undetected_chromedriver as uc
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urljoin, urlparse
import re

class FPTShopFastCrawler:
    def __init__(self):
        self.source_name = "fptshop"
        self.base_url = "https://fptshop.com.vn"

        # Pre-defined product URLs for faster testing
        self.sample_product_urls = [
            "https://fptshop.com.vn/dien-thoai/iphone-16-pro-max",
            "https://fptshop.com.vn/dien-thoai/samsung-galaxy-s24-ultra",
            "https://fptshop.com.vn/dien-thoai/xiaomi-redmi-note-13",
            "https://fptshop.com.vn/dien-thoai/oppo-reno12-f",
            "https://fptshop.com.vn/dien-thoai/vivo-y28-5g",
            "https://fptshop.com.vn/dien-thoai/honor-x9b",
            "https://fptshop.com.vn/dien-thoai/realme-c67",
            "https://fptshop.com.vn/dien-thoai/nokia-g42",
            "https://fptshop.com.vn/dien-thoai/xiaomi-poco-x6-pro",
            "https://fptshop.com.vn/dien-thoai/samsung-galaxy-a55"
        ]

    def setup_fast_driver(self):
        """Setup fast undetected Chrome driver"""
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-javascript')  # Faster loading
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)

        try:
            driver = uc.Chrome(options=options, version_main=None)
            return driver
        except Exception as e:
            print(f"Failed to create undetected driver: {e}")
            # Fallback to regular Chrome
            from selenium import webdriver
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            return webdriver.Chrome(options=options)

    def clean_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price from text"""
        if not price_text or price_text == "N/A":
            return None

        # Remove all non-digit characters
        price_clean = re.sub(r'[^\d]', '', price_text)

        try:
            return int(price_clean) if price_clean else None
        except ValueError:
            return None

    def extract_product_data_fast(self, driver, product_url: str) -> Optional[Dict]:
        """Fast extraction focusing on essentials"""
        try:
            # Get page source for regex parsing (faster than DOM queries)
            page_source = driver.page_source

            # Extract product name using regex
            product_name = self._extract_name_regex(page_source)

            # Extract prices using regex
            price_current, price_original = self._extract_prices_regex(page_source)

            # Extract brand from URL or name
            brand = self._extract_brand_from_url_or_name(product_url, product_name)

            # Extract product ID
            product_id = product_url.split('/')[-1].split('?')[0]

            # Extract basic images
            image_urls = self._extract_images_regex(page_source)

            # Calculate discount
            discount_percent = None
            if price_current and price_original and price_original > price_current:
                discount_percent = round(((price_original - price_current) / price_original) * 100, 2)

            product_data = {
                "source": self.source_name,
                "product_id": product_id,
                "url": product_url,
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
            print(f"ERROR extracting from {product_url}: {e}")
            return None

    def _extract_name_regex(self, page_source: str) -> str:
        """Extract product name using regex"""
        patterns = [
            r'<h1[^>]*data-testid="product-name"[^>]*>([^<]+)</h1>',
            r'<h1[^>]*class="[^"]*fs-dttitle[^"]*"[^>]*>([^<]+)</h1>',
            r'<h1[^>]*>([^<]+)</h1>',
            r'"name":\s*"([^"]+)"',
            r'<title>([^<]+)</title>'
        ]

        for pattern in patterns:
            match = re.search(pattern, page_source, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if name and len(name) > 3 and 'fptshop' not in name.lower():
                    return name

        return "N/A"

    def _extract_prices_regex(self, page_source: str) -> tuple:
        """Extract prices using regex patterns"""
        # Current price patterns
        current_patterns = [
            r'data-testid="product-price"[^>]*>([^<]+)<',
            r'class="[^"]*Price_currentPrice[^"]*"[^>]*>([^<]+)<',
            r'class="[^"]*fs-dtprice-special[^"]*"[^>]*>([^<]+)<',
            r'"price":\s*"?([0-9,.]+)"?',
            r'₫([0-9,.]+)',
        ]

        # Original price patterns
        original_patterns = [
            r'data-testid="product-original-price"[^>]*>([^<]+)<',
            r'class="[^"]*Price_originalPrice[^"]*"[^>]*>([^<]+)<',
            r'class="[^"]*fs-dtprice-old[^"]*"[^>]*>([^<]+)<',
            r'class="[^"]*line-through[^"]*"[^>]*>([^<]+)<',
        ]

        price_current = None
        for pattern in current_patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            for match in matches:
                price = self.clean_price(match)
                if price and price > 1000:  # Valid price threshold
                    price_current = price
                    break
            if price_current:
                break

        price_original = None
        for pattern in original_patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            for match in matches:
                price = self.clean_price(match)
                if price and price > (price_current or 0):
                    price_original = price
                    break
            if price_original:
                break

        return price_current, price_original

    def _extract_brand_from_url_or_name(self, url: str, name: str) -> str:
        """Extract brand from URL or product name"""
        brands = ['iPhone', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'Huawei', 'Nokia', 'Realme', 'OnePlus', 'Honor', 'Tecno', 'Apple']

        # Check URL
        for brand in brands:
            if brand.lower() in url.lower():
                return brand

        # Check product name
        for brand in brands:
            if brand.lower() in name.lower():
                return brand

        return "N/A"

    def _extract_images_regex(self, page_source: str) -> List[str]:
        """Extract image URLs using regex"""
        patterns = [
            r'<img[^>]+src="([^"]*fptshop[^"]*\.(?:jpg|jpeg|png|webp))"',
            r'<img[^>]+data-src="([^"]*fptshop[^"]*\.(?:jpg|jpeg|png|webp))"',
            r'"image":\s*"([^"]+)"',
        ]

        image_urls = []
        for pattern in patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            for match in matches:
                if match not in image_urls and len(image_urls) < 5:
                    if match.startswith('/'):
                        match = urljoin(self.base_url, match)
                    image_urls.append(match)

        return image_urls

    def crawl_fast(self, product_urls: List[str] = None, max_products: int = 10) -> List[Dict]:
        """Fast crawl of specific products"""
        if not product_urls:
            product_urls = self.sample_product_urls[:max_products]

        products_data = []
        successful = 0

        print(f"Starting fast crawl of {len(product_urls)} products...")

        for i, url in enumerate(product_urls, 1):
            print(f"[{i}/{len(product_urls)}] {url}")

            driver = None
            try:
                driver = self.setup_fast_driver()

                # Fast page load
                driver.set_page_load_timeout(10)
                driver.get(url)
                time.sleep(2)  # Minimal wait

                product_data = self.extract_product_data_fast(driver, url)
                if product_data:
                    products_data.append(product_data)
                    if product_data['price_current']:
                        successful += 1
                        print(f"  OK: {product_data['product_name']} - {product_data['price_current']:,} VND")
                    else:
                        print(f"  OK: {product_data['product_name']} - No price")
                else:
                    print(f"  FAIL: Could not extract data")

            except Exception as e:
                print(f"  ERROR: {e}")
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass

            # Short delay
            time.sleep(random.uniform(0.5, 1.5))

        print(f"\nCrawl completed:")
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
    crawler = FPTShopFastCrawler()

    try:
        print("=== FPTShop Fast Crawler ===")
        print("Testing anti-bot bypass with speed optimization...")

        # Quick test
        products_data = crawler.crawl_fast(max_products=10)

        if products_data:
            # Save results
            timestamp = int(time.time())
            filename = f"fptshop_fast_test_{timestamp}.json"
            crawler.save_data(products_data, filename)

        return products_data

    except KeyboardInterrupt:
        print("\nStopped by user")
        return []
    except Exception as e:
        print(f"Main error: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    main()