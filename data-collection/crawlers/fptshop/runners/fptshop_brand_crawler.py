#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import random
import requests
import re
import concurrent.futures
import threading
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import os

class FPTShopBrandCrawler:
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

        # Brand categories that likely have more products
        self.brand_categories = {
            "apple-iphone": "/dien-thoai/apple-iphone",
            "samsung": "/dien-thoai/samsung",
            "xiaomi": "/dien-thoai/xiaomi",
            "oppo": "/dien-thoai/oppo",
            "vivo": "/dien-thoai/vivo",
            "honor": "/dien-thoai/honor",
            "realme": "/dien-thoai/realme",
            "nokia": "/dien-thoai/nokia",
            "tecno": "/dien-thoai/tecno",
            "oneplus": "/dien-thoai/oneplus"
        }

        # Specific product categories
        self.product_categories = {
            "dien-thoai-5g": "/dien-thoai/dien-thoai-5g",
            "dien-thoai-ai": "/dien-thoai/ai",
            "dien-thoai-gap": "/dien-thoai/dien-thoai-gap",
            "dien-thoai-gaming": "/dien-thoai/dien-thoai-gaming",
            "pho-thong": "/dien-thoai/pho-thong"
        }

    def get_page_content(self, url: str, retries: int = 3) -> Optional[str]:
        """Get page content with retry"""
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(0.5, 1.0))
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    return response.text
            except Exception as e:
                if attempt == retries - 1:
                    print(f"  Failed to get {url}: {e}")
                time.sleep(random.uniform(1, 2))
        return None

    def discover_all_product_urls(self) -> List[str]:
        """Discover all possible product URLs from various categories"""
        print("=== Discovering ALL FPTShop Products ===")

        all_product_urls = set()

        # Method 1: Main categories
        print("1. Crawling main categories...")
        main_categories = ["/dien-thoai", "/may-tinh-xach-tay", "/tai-nghe", "/dong-ho-thong-minh"]

        for category in main_categories:
            url = f"{self.base_url}{category}"
            urls = self._extract_products_from_page(url, category.replace("/", ""))
            print(f"   {category}: {len(urls)} products")
            all_product_urls.update(urls)

        # Method 2: Brand categories
        print("2. Crawling brand categories...")
        for brand, path in self.brand_categories.items():
            url = f"{self.base_url}{path}"
            urls = self._extract_products_from_page(url, brand)
            print(f"   {brand}: {len(urls)} products")
            all_product_urls.update(urls)

        # Method 3: Product type categories
        print("3. Crawling product type categories...")
        for product_type, path in self.product_categories.items():
            url = f"{self.base_url}{path}"
            urls = self._extract_products_from_page(url, product_type)
            print(f"   {product_type}: {len(urls)} products")
            all_product_urls.update(urls)

        # Method 4: Search-based discovery
        print("4. Search-based discovery...")
        search_terms = ["iphone", "samsung", "xiaomi", "oppo", "vivo", "phone"]
        for term in search_terms:
            search_url = f"{self.base_url}/tim?q={term}&category=dien-thoai"
            urls = self._extract_products_from_page(search_url, f"search-{term}")
            print(f"   search-{term}: {len(urls)} products")
            all_product_urls.update(urls)

        unique_urls = list(all_product_urls)
        print(f"\nTotal unique products discovered: {len(unique_urls)}")
        return unique_urls

    def _extract_products_from_page(self, url: str, category_name: str) -> List[str]:
        """Extract product URLs from a single page"""
        content = self.get_page_content(url)
        if not content:
            return []

        soup = BeautifulSoup(content, 'html.parser')
        product_urls = []

        # Multiple strategies to find product links
        strategies = [
            # Strategy 1: Direct phone product links
            lambda: [a.get('href') for a in soup.find_all('a', href=True)
                    if '/dien-thoai/' in a.get('href', '') and len(a.get('href', '').split('/')) > 3],

            # Strategy 2: Product cards
            lambda: [a.get('href') for card in soup.find_all(['div', 'article'], class_=re.compile(r'[Pp]roduct'))
                    for a in card.find_all('a', href=True) if '/dien-thoai/' in a.get('href', '')],

            # Strategy 3: Links with product names in text
            lambda: [a.get('href') for a in soup.find_all('a', href=True)
                    if any(brand in a.get_text().lower() for brand in ['iphone', 'samsung', 'xiaomi', 'oppo', 'vivo'])
                    and '/dien-thoai/' in a.get('href', '')],

            # Strategy 4: All links in product-like containers
            lambda: [a.get('href') for container in soup.find_all(['div', 'section'], class_=re.compile(r'item|card|product', re.I))
                    for a in container.find_all('a', href=True) if '/dien-thoai/' in a.get('href', '')]
        ]

        for strategy in strategies:
            try:
                links = strategy()
                for href in links:
                    if href and self._is_valid_product_url(href):
                        full_url = urljoin(self.base_url, href) if href.startswith('/') else href
                        clean_url = full_url.split('?')[0].split('#')[0]
                        if clean_url not in product_urls:
                            product_urls.append(clean_url)
            except Exception as e:
                continue

        return product_urls

    def _is_valid_product_url(self, url: str) -> bool:
        """Check if URL is a valid product URL"""
        if not url:
            return False

        # Must be a phone product
        if '/dien-thoai/' not in url:
            return False

        # Exclude non-product pages
        exclude_patterns = [
            '/page=', '/filter', '/sort', '/category', '/brand', '/price',
            '/promotion', '/news', '/search', '/compare', '/review', '/tin-tuc'
        ]

        for pattern in exclude_patterns:
            if pattern in url.lower():
                return False

        # Must have proper structure
        parts = url.strip('/').split('/')
        if len(parts) < 3:
            return False

        # Last part should be product slug
        last_part = parts[-1]
        return len(last_part) > 3 and '-' in last_part

    def clean_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price"""
        if not price_text:
            return None
        price_clean = re.sub(r'[^\d]', '', price_text)
        try:
            price = int(price_clean) if price_clean else None
            return price if price and 10000 <= price <= 500000000 else None
        except ValueError:
            return None

    def extract_product_data(self, url: str) -> Optional[Dict]:
        """Extract product data from URL"""
        content = self.get_page_content(url)
        if not content:
            return None

        try:
            soup = BeautifulSoup(content, 'html.parser')

            product_id = url.split('/')[-1].split('?')[0]
            product_name = self._extract_name(soup, content)
            brand = self._extract_brand(url, product_name)
            price_current, price_original = self._extract_prices(soup, content)
            image_urls = self._extract_images(soup, content)

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
            print(f"  Parse error for {url}: {e}")
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
            r'"productName":\s*"([^"]+)"',
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

        # Regex price patterns
        if not price_current:
            price_patterns = [
                r'₫\s*([0-9.,]+)',
                r'"price":\s*"?([0-9.,]+)"?'
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

    def _extract_images(self, soup, content: str) -> List[str]:
        """Extract product images"""
        image_urls = []

        # CSS selectors
        image_selectors = [
            '.fs-dtimage img',
            '.product-gallery img',
            '.ProductImage img'
        ]

        for selector in image_selectors:
            try:
                images = soup.select(selector)
                for img in images:
                    src = img.get('src') or img.get('data-src')
                    if src and self._is_valid_image(src):
                        full_url = urljoin(self.base_url, src) if src.startswith('/') else src
                        if full_url not in image_urls:
                            image_urls.append(full_url)
                        if len(image_urls) >= 5:
                            return image_urls
            except:
                continue

        # Regex patterns
        if len(image_urls) < 3:
            image_patterns = [
                r'https://cdn[0-9]*.fptshop.com.vn/[^"\s]*\.(?:jpg|jpeg|png|webp)'
            ]

            for pattern in image_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    if self._is_valid_image(match) and match not in image_urls:
                        image_urls.append(match)
                        if len(image_urls) >= 5:
                            break

        return image_urls[:5]

    def _is_valid_image(self, url: str) -> bool:
        """Check if URL is a valid product image"""
        if not url or len(url) < 20:
            return False

        url_lower = url.lower()

        # Exclude icons
        exclude_patterns = [
            'icon_', 'logo_', 'banner_', 'mua_1_tang_1', 'vong_quay', 'sim_du_lich'
        ]

        return (not any(pattern in url_lower for pattern in exclude_patterns) and
                any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.webp']) and
                'fptshop.com.vn' in url_lower)

    def crawl_products_parallel(self, product_urls: List[str], max_workers: int = 5) -> List[Dict]:
        """Crawl products in parallel"""
        products_data = []
        successful = 0

        print(f"\nCrawling {len(product_urls)} products with {max_workers} workers...")

        def crawl_single(args):
            i, url = args
            try:
                if i % 50 == 0:
                    print(f"Progress: {i}/{len(product_urls)}")

                product_data = self.extract_product_data(url)
                if product_data and product_data.get('price_current'):
                    return product_data, 1
                elif product_data:
                    return product_data, 0
                return None, 0
            except Exception as e:
                return None, 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(crawl_single, (i, url)) for i, url in enumerate(product_urls)]

            for future in concurrent.futures.as_completed(futures):
                try:
                    result, success = future.result()
                    if result:
                        products_data.append(result)
                        successful += success
                except Exception as e:
                    continue

        print(f"\nResults: {len(products_data)} products, {successful} with prices")
        return products_data

    def save_data(self, products_data: List[Dict], filename: str):
        """Save data to JSON"""
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products_data, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(products_data)} products to: {filepath}")

    def crawl_all_fptshop_products(self):
        """Main function to crawl ALL FPTShop products"""
        print("=== FPTShop Complete Product Crawler ===")
        start_time = time.time()

        # Step 1: Discover all product URLs
        product_urls = self.discover_all_product_urls()

        if not product_urls:
            print("No products found!")
            return []

        # Step 2: Crawl all products
        products_data = self.crawl_products_parallel(product_urls, max_workers=5)

        # Step 3: Save results
        if products_data:
            timestamp = int(time.time())
            filename = f"fptshop_complete_{timestamp}.json"
            self.save_data(products_data, filename)

            duration = time.time() - start_time
            with_prices = sum(1 for p in products_data if p.get('price_current'))
            with_images = sum(1 for p in products_data if p.get('image_urls'))

            print(f"\n=== FINAL RESULTS ===")
            print(f"Total time: {duration/60:.1f} minutes")
            print(f"Products found: {len(products_data)}")
            print(f"With prices: {with_prices} ({with_prices/len(products_data)*100:.1f}%)")
            print(f"With images: {with_images} ({with_images/len(products_data)*100:.1f}%)")
            print(f"Speed: {len(products_data)/(duration/60):.1f} products/minute")

        return products_data

def main():
    crawler = FPTShopBrandCrawler()
    products = crawler.crawl_all_fptshop_products()
    return products

if __name__ == "__main__":
    main()