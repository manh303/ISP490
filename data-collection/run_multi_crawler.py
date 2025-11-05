#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import random
import requests
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

class FPTShopFinalCrawler:
    def __init__(self):
        self.source_name = "fptshop"
        self.base_url = "https://fptshop.com.vn"

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_page_content(self, url: str) -> Optional[str]:
        """Get page content"""
        try:
            time.sleep(random.uniform(0.5, 1.0))
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            print(f"  Request error: {e}")
        return None

    def clean_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price"""
        if not price_text:
            return None
        price_clean = re.sub(r'[^\d]', '', price_text)
        try:
            price = int(price_clean) if price_clean else None
            return price if price and 10000 <= price <= 200000000 else None
        except ValueError:
            return None

    def extract_product_data(self, url: str) -> Optional[Dict]:
        """Extract complete product data"""
        content = self.get_page_content(url)
        if not content:
            return None

        try:
            soup = BeautifulSoup(content, 'html.parser')

            product_id = url.split('/')[-1].split('?')[0]
            product_name = self._extract_name(soup, content)
            brand = self._extract_brand(url, product_name)
            price_current, price_original = self._extract_prices(soup, content)
            image_urls = self._extract_images_comprehensive(soup, content)

            # Calculate discount
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
        # Try multiple selectors
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

        # Regex patterns
        patterns = [
            r'<h1[^>]*>([^<]+)</h1>',
            r'"productName":\s*"([^"]+)"',
            r'"name":\s*"([^"]+)"',
            r'<title>([^<]+?)\s*(?:-\s*FPTShop|\s*\|\s*FPTShop)'
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
        """Extract prices"""
        price_current = None
        price_original = None

        # Current price selectors
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

        # Regex price patterns
        if not price_current:
            price_patterns = [
                r'₫\s*([0-9.,]+)',
                r'"price":\s*"?([0-9.,]+)"?',
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

    def _extract_images_comprehensive(self, soup, content: str) -> List[str]:
        """Comprehensive image extraction"""
        image_urls = []

        # Strategy 1: CSS selectors for product images
        image_selectors = [
            '.fs-dtimage img',
            '.product-gallery img',
            '.ProductImage img',
            '.gallery img',
            '.slider img',
            '.thumb img',
            '[data-testid="product-image"] img',
            'img[alt*="product"]',
            'img[alt*="phone"]',
            'img[alt*="iphone"]',
            'img[alt*="samsung"]'
        ]

        for selector in image_selectors:
            try:
                images = soup.select(selector)
                for img in images:
                    # Try multiple src attributes
                    src_attrs = ['src', 'data-src', 'data-lazy-src', 'data-original']
                    for attr in src_attrs:
                        src = img.get(attr)
                        if src and self._is_valid_product_image(src):
                            full_url = urljoin(self.base_url, src) if src.startswith('/') else src
                            if full_url not in image_urls:
                                image_urls.append(full_url)
                            break

                    if len(image_urls) >= 5:
                        return image_urls
            except:
                continue

        # Strategy 2: Regex patterns for image URLs in HTML/JSON
        if len(image_urls) < 3:
            image_patterns = [
                r'src="([^"]*(?:iphone|samsung|xiaomi|oppo|vivo|nokia|phone)[^"]*\.(?:jpg|jpeg|png|webp))"',
                r'data-src="([^"]*fptshop[^"]*(?:360x|720x|1080x)[^"]*\.(?:jpg|jpeg|png|webp))"',
                r'"image":\s*"([^"]+\.(?:jpg|jpeg|png|webp))"',
                r'"thumbnail":\s*"([^"]+\.(?:jpg|jpeg|png|webp))"',
                r'url\(["\']([^"\']*fptshop[^"\']*\.(?:jpg|jpeg|png|webp))["\']',
                r'https://cdn[0-9]*.fptshop.com.vn/[^"\s]*\.(?:jpg|jpeg|png|webp)'
            ]

            for pattern in image_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    if self._is_valid_product_image(match):
                        full_url = urljoin(self.base_url, match) if match.startswith('/') else match
                        if full_url not in image_urls:
                            image_urls.append(full_url)
                        if len(image_urls) >= 5:
                            break
                if len(image_urls) >= 5:
                    break

        # Strategy 3: Find all images and filter
        if len(image_urls) < 2:
            all_images = soup.find_all('img')
            for img in all_images:
                src = img.get('src') or img.get('data-src')
                if src and self._is_valid_product_image(src):
                    full_url = urljoin(self.base_url, src) if src.startswith('/') else src
                    if full_url not in image_urls:
                        image_urls.append(full_url)
                        if len(image_urls) >= 3:
                            break

        return image_urls[:5]

    def _is_valid_product_image(self, url: str) -> bool:
        """Check if URL is a valid product image"""
        if not url or len(url) < 20:
            return False

        url_lower = url.lower()

        # Exclude promotional/system images
        exclude_patterns = [
            'icon_', 'logo_', 'banner_', 'badge_', 'promotion_',
            'mua_1_tang_1', 'vong_quay', 'sim_du_lich',
            'ban_sao_logo', 'icon_50', 'icon_mua',
            'favicon', 'sprite', 'placeholder'
        ]

        for pattern in exclude_patterns:
            if pattern in url_lower:
                return False

        # Must be an image file
        if not any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.webp']):
            return False

        # Include patterns for product images
        include_patterns = [
            'iphone', 'samsung', 'xiaomi', 'oppo', 'vivo', 'nokia',
            'phone', 'smartphone', 'mobile', 'product',
            'fptshop.com.vn', '360x', '720x', '1080x'
        ]

        return any(pattern in url_lower for pattern in include_patterns)

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
    print("=== FPTShop Final Multi-Crawler ===")

    # Test với specific product URLs
    test_urls = [
        "https://fptshop.com.vn/dien-thoai/iphone-16-pro-max",
        "https://fptshop.com.vn/dien-thoai/samsung-galaxy-s24-ultra",
        "https://fptshop.com.vn/dien-thoai/xiaomi-redmi-note-13",
        "https://fptshop.com.vn/dien-thoai/oppo-reno12-f",
        "https://fptshop.com.vn/dien-thoai/honor-x9b",
        "https://fptshop.com.vn/dien-thoai/realme-c67",
        "https://fptshop.com.vn/dien-thoai/tecno-spark-30"
    ]

    crawler = FPTShopFinalCrawler()
    products_data = []
    successful_prices = 0
    successful_images = 0

    print(f"Testing {len(test_urls)} products...")

    for i, url in enumerate(test_urls, 1):
        print(f"[{i}/{len(test_urls)}] {url}")

        product_data = crawler.extract_product_data(url)
        if product_data:
            products_data.append(product_data)

            # Check success metrics
            has_price = bool(product_data.get('price_current'))
            has_images = len(product_data.get('image_urls', [])) > 0

            if has_price:
                successful_prices += 1
            if has_images:
                successful_images += 1

            print(f"  OK: {product_data['product_name']}")
            print(f"      Price: {product_data['price_current']:,} VND" if has_price else "      Price: None")
            print(f"      Images: {len(product_data.get('image_urls', []))}")
        else:
            print(f"  FAIL: Could not extract data")

        time.sleep(random.uniform(1, 2))

    # Save results
    if products_data:
        timestamp = int(time.time())
        filename = f"fptshop_final_test_{timestamp}.json"
        crawler.save_data(products_data, filename)

        # Final statistics
        print(f"\n=== FINAL RESULTS ===")
        print(f"Total products: {len(products_data)}")
        print(f"With prices: {successful_prices} ({successful_prices/len(products_data)*100:.1f}%)")
        print(f"With images: {successful_images} ({successful_images/len(products_data)*100:.1f}%)")

        # Image quality analysis
        total_images = sum(len(p.get('image_urls', [])) for p in products_data)
        print(f"Total images: {total_images}")
        print(f"Avg per product: {total_images/len(products_data):.1f}")

    return products_data

if __name__ == "__main__":
    main()