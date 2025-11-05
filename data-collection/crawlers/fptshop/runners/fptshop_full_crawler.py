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
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
import os

class FPTShopFullCrawler:
    def __init__(self):
        self.source_name = "fptshop"
        self.base_url = "https://fptshop.com.vn"

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
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
            },
            {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'vi,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }
        ]

        self.session_lock = threading.Lock()
        self.sessions = []
        self._init_sessions()

        # Categories to crawl
        self.categories = {
            "dien-thoai": "/dien-thoai",
            "laptop": "/may-tinh-xach-tay",
            "tai-nghe": "/tai-nghe",
            "dong-ho": "/dong-ho-thong-minh"
        }

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

    def get_page_content(self, url: str, retries: int = 3) -> Optional[str]:
        """Get page content with retry"""
        for attempt in range(retries):
            try:
                session = self.get_session()
                time.sleep(random.uniform(0.3, 0.8))

                response = session.get(url, timeout=15)
                if response.status_code == 200:
                    content = response.text
                    # Basic check for valid content
                    if len(content) > 10000 and 'challenge' not in content.lower():
                        return content
                    elif attempt < retries - 1:
                        time.sleep(random.uniform(1, 3))
                        continue
                    return content
            except Exception as e:
                if attempt == retries - 1:
                    print(f"  Failed to get {url}: {e}")
                time.sleep(random.uniform(1, 2))
        return None

    def get_all_product_urls_from_category(self, category: str, max_pages: int = 50) -> List[str]:
        """Get ALL product URLs from category with pagination"""
        print(f"\n=== Getting ALL products from {category} ===")

        category_path = self.categories.get(category, "/dien-thoai")
        product_urls = set()  # Use set to avoid duplicates

        for page in range(1, max_pages + 1):
            print(f"Crawling page {page}...")

            # FPTShop pagination format
            if page == 1:
                page_url = f"{self.base_url}{category_path}"
            else:
                page_url = f"{self.base_url}{category_path}?page={page}"

            content = self.get_page_content(page_url)
            if not content:
                print(f"  Failed to get page {page}")
                continue

            soup = BeautifulSoup(content, 'html.parser')
            page_urls = self._extract_product_urls_from_page(soup, category_path)

            if not page_urls:
                print(f"  No products found on page {page}, stopping pagination")
                break

            print(f"  Found {len(page_urls)} products on page {page}")
            product_urls.update(page_urls)

            # Check if we've reached the end (no new products)
            if len(page_urls) < 10:  # Usually pages have 20+ products
                print(f"  Small page detected, might be end of pagination")
                # Continue for a few more pages to be sure
                if page > 3:
                    break

            time.sleep(random.uniform(1, 2))

        unique_urls = list(product_urls)
        print(f"\nTotal unique products found: {len(unique_urls)}")
        return unique_urls

    def _extract_product_urls_from_page(self, soup: BeautifulSoup, category_path: str) -> List[str]:
        """Extract product URLs from a single page"""
        product_urls = []

        # Multiple selectors for finding product links
        link_selectors = [
            f'a[href*="{category_path}"]',
            '.ProductCard a',
            '.product-item a',
            '.fs-pitem a',
            'a[href*="/dien-thoai/"]',
            'a[href*="/may-tinh-xach-tay/"]',
            'a[href*="/tai-nghe/"]',
            'a[href*="/dong-ho-thong-minh/"]'
        ]

        for selector in link_selectors:
            try:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and self._is_valid_product_url(href, category_path):
                        full_url = urljoin(self.base_url, href) if href.startswith('/') else href
                        # Clean URL (remove parameters)
                        clean_url = full_url.split('?')[0].split('#')[0]
                        if clean_url not in product_urls:
                            product_urls.append(clean_url)
            except Exception as e:
                continue

        return product_urls

    def _is_valid_product_url(self, url: str, category_path: str) -> bool:
        """Check if URL is a valid product URL"""
        if not url:
            return False

        # Must contain category path
        if category_path not in url:
            return False

        # Exclude non-product pages
        exclude_patterns = [
            '/page=', '/filter', '/sort', '/category',
            '/brand', '/price', '/promotion', '/news',
            '/search', '/compare', '/review'
        ]

        for pattern in exclude_patterns:
            if pattern in url.lower():
                return False

        # Should have proper product slug format
        parts = url.strip('/').split('/')
        return len(parts) >= 3  # At least domain/category/product

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
        brands = ['iPhone', 'Apple', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'Nokia', 'Realme', 'Honor', 'Tecno', 'Asus', 'Dell', 'HP', 'Lenovo', 'Acer', 'MSI']
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
            '[data-testid="product-image"] img'
        ]

        for selector in image_selectors:
            try:
                images = soup.select(selector)
                for img in images:
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

        # Strategy 2: Regex patterns
        if len(image_urls) < 3:
            image_patterns = [
                r'https://cdn[0-9]*.fptshop.com.vn/[^"\s]*\.(?:jpg|jpeg|png|webp)',
                r'data-src="([^"]*fptshop[^"]*(?:360x|720x|1080x)[^"]*\.(?:jpg|jpeg|png|webp))"',
                r'"image":\s*"([^"]+\.(?:jpg|jpeg|png|webp))"'
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
            'fptshop.com.vn', '360x', '720x', '1080x', '1920x'
        ]

        return any(pattern in url_lower for pattern in include_patterns)

    def crawl_products_parallel(self, product_urls: List[str], max_workers: int = 5) -> List[Dict]:
        """Crawl products in parallel"""
        products_data = []
        successful_prices = 0
        successful_images = 0

        print(f"\nCrawling {len(product_urls)} products with {max_workers} workers...")

        def crawl_single_product(args):
            i, url = args
            try:
                if i % 50 == 0:  # Progress update every 50 products
                    print(f"Progress: {i}/{len(product_urls)}")

                product_data = self.extract_product_data(url)
                if product_data:
                    has_price = bool(product_data.get('price_current'))
                    has_images = len(product_data.get('image_urls', [])) > 0
                    return product_data, has_price, has_images
                return None, False, False
            except Exception as e:
                print(f"Error processing {url}: {e}")
                return None, False, False

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = [executor.submit(crawl_single_product, (i, url))
                      for i, url in enumerate(product_urls)]

            # Collect results
            for future in concurrent.futures.as_completed(futures):
                try:
                    result, has_price, has_images = future.result()
                    if result:
                        products_data.append(result)
                        if has_price:
                            successful_prices += 1
                        if has_images:
                            successful_images += 1
                except Exception as e:
                    print(f"Future error: {e}")

        print(f"\nCrawling completed:")
        print(f"- Total products: {len(products_data)}")
        print(f"- With prices: {successful_prices} ({successful_prices/len(products_data)*100:.1f}%)" if products_data else "0%")
        print(f"- With images: {successful_images} ({successful_images/len(products_data)*100:.1f}%)" if products_data else "0%")

        return products_data

    def save_data(self, products_data: List[Dict], filename: str):
        """Save data to JSON file with progress info"""
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)

        # Add metadata
        metadata = {
            "crawl_info": {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "total_products": len(products_data),
                "with_prices": sum(1 for p in products_data if p.get('price_current')),
                "with_images": sum(1 for p in products_data if p.get('image_urls')),
                "crawler_version": "FPTShopFullCrawler_v1.0"
            },
            "products": products_data
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(products_data)} products to: {filepath}")

    def crawl_full_category(self, category: str = "dien-thoai", max_workers: int = 3):
        """Crawl ALL products from a category"""
        print(f"=== FPTShop Full Category Crawler ===")
        print(f"Target: {category}")

        start_time = time.time()

        # Step 1: Get all product URLs
        product_urls = self.get_all_product_urls_from_category(category, max_pages=100)

        if not product_urls:
            print("No product URLs found!")
            return []

        print(f"\nFound {len(product_urls)} total products to crawl")

        # Step 2: Crawl all products
        products_data = self.crawl_products_parallel(product_urls, max_workers=max_workers)

        # Step 3: Save results
        if products_data:
            timestamp = int(time.time())
            filename = f"fptshop_full_{category}_{timestamp}.json"
            self.save_data(products_data, filename)

            duration = time.time() - start_time
            print(f"\n=== FINAL RESULTS ===")
            print(f"Category: {category}")
            print(f"Total time: {duration/60:.1f} minutes")
            print(f"Total products: {len(products_data)}")
            print(f"Speed: {len(products_data)/(duration/60):.1f} products/minute")

        return products_data

def main():
    crawler = FPTShopFullCrawler()

    print("Choose crawling mode:")
    print("1. Full category (all products)")
    print("2. Quick test (50 products)")

    choice = input("Enter choice (1-2): ").strip()

    if choice == "1":
        category = input("Enter category (dien-thoai/laptop/tai-nghe/dong-ho) [dien-thoai]: ").strip() or "dien-thoai"
        products = crawler.crawl_full_category(category, max_workers=3)
    else:
        # Quick test mode
        print("Quick test mode - crawling first 50 products...")
        product_urls = crawler.get_all_product_urls_from_category("dien-thoai", max_pages=3)[:50]
        products = crawler.crawl_products_parallel(product_urls, max_workers=3)

        if products:
            timestamp = int(time.time())
            filename = f"fptshop_test_{timestamp}.json"
            crawler.save_data(products, filename)

    return products

if __name__ == "__main__":
    main()