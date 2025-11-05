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

class FPTShopImprovedCrawler:
    def __init__(self):
        self.source_name = "fptshop"
        self.base_url = "https://fptshop.com.vn"
        self.delay_range = (1, 3)  # Faster delays

        # Categories with direct product URLs
        self.categories = {
            "dien-thoai": "/dien-thoai",
            "laptop": "/may-tinh-xach-tay",
            "tai-nghe": "/tai-nghe",
            "dong-ho": "/dong-ho-thong-minh"
        }

    def setup_undetected_driver(self):
        """Setup undetected Chrome driver to bypass Cloudflare"""
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--no-first-run')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')

        # Use undetected chromedriver
        driver = uc.Chrome(options=options, version_main=None)

        # Additional stealth settings
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                  get: () => undefined
                })
            """
        })

        return driver

    def random_delay(self):
        """Random delay between requests"""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)

    def clean_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price from text"""
        if not price_text or price_text == "N/A":
            return None

        # Remove all non-digit characters except comma and dot
        price_clean = re.sub(r'[^\d,.]', '', price_text.replace('.', '').replace(',', ''))

        try:
            return int(price_clean) if price_clean else None
        except ValueError:
            return None

    def wait_for_page_load(self, driver, timeout=10):
        """Wait for page to load completely"""
        try:
            # Wait for basic page structure
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            # Additional wait for dynamic content
            time.sleep(3)
            return True
        except TimeoutException:
            return False

    def extract_product_data(self, driver, product_url: str) -> Optional[Dict]:
        """Extract product data from current page"""
        try:
            # Extract product ID from URL
            product_id = self._extract_product_id_from_url(product_url)

            # Extract product name with multiple selectors
            product_name = self._extract_product_name(driver)

            # Extract brand
            brand = self._extract_brand(driver, product_name)

            # Extract prices - IMPROVED SELECTORS
            price_current, price_original = self._extract_prices_improved(driver)

            # Calculate discount
            discount_percent = self._calculate_discount(price_current, price_original)

            # Extract images
            image_urls = self._extract_images(driver)

            product_data = {
                "source": self.source_name,
                "product_id": product_id,
                "url": product_url,
                "crawl_date": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "product_name": product_name,
                "brand": brand,
                "category": "Điện tử",
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
            print(f"Error extracting data from {product_url}: {e}")
            return None

    def _extract_product_id_from_url(self, url: str) -> str:
        """Extract product ID from URL"""
        try:
            path = urlparse(url).path
            path_parts = [p for p in path.split('/') if p]
            return path_parts[-1] if path_parts else "unknown"
        except:
            return "unknown"

    def _extract_product_name(self, driver) -> str:
        """Extract product name with improved selectors"""
        selectors = [
            'h1[data-testid="product-name"]',
            'h1.fs-dttitle',
            'h1.product-title',
            '.ProductCard_cardTitle__HlwIo',
            'h1',
            '.product-name',
            '[data-testid="pdp-product-name"]'
        ]

        for selector in selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                text = element.text.strip()
                if text and len(text) > 3:
                    return text
            except:
                continue
        return "N/A"

    def _extract_brand(self, driver, product_name: str) -> str:
        """Extract brand from various sources"""
        # Try to get brand from breadcrumbs or dedicated elements
        brand_selectors = [
            '.fs-dtbrand',
            '.product-brand',
            '.brand-name',
            '[data-testid="brand"]'
        ]

        for selector in brand_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                text = element.text.strip()
                if text and text.lower() not in ['fptshop', 'fpt']:
                    return text
            except:
                continue

        # Extract from product name
        brands = ['iPhone', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'Huawei', 'Nokia', 'Realme', 'OnePlus', 'Honor', 'Tecno']
        for brand in brands:
            if brand.lower() in product_name.lower():
                return brand

        return "N/A"

    def _extract_prices_improved(self, driver) -> tuple:
        """Improved price extraction with better selectors"""
        # Updated selectors based on actual FPTShop structure
        current_price_selectors = [
            '[data-testid="product-price"]',
            '.Price_currentPrice__PBYcv',
            'p.Price_currentPrice__PBYcv',
            '.ProductCard_price__current',
            '.fs-dtprice-special',
            '.price-current',
            '.current-price',
            '[class*="currentPrice"]',
            '[class*="Price_current"]',
            '[data-price]'
        ]

        price_current = None
        for selector in current_price_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text:
                        price = self.clean_price(text)
                        if price and price > 1000:  # Valid price
                            price_current = price
                            break
                if price_current:
                    break
            except:
                continue

        # Original price selectors
        original_price_selectors = [
            '[data-testid="product-original-price"]',
            '.Price_originalPrice__abc',
            '.ProductCard_price__original',
            '.fs-dtprice-old',
            '.price-old',
            '.original-price',
            '[class*="originalPrice"]',
            '[class*="Price_original"]',
            '.line-through'
        ]

        price_original = None
        for selector in original_price_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text:
                        price = self.clean_price(text)
                        if price and price > price_current:  # Original should be higher
                            price_original = price
                            break
                if price_original:
                    break
            except:
                continue

        return price_current, price_original

    def _calculate_discount(self, current: Optional[int], original: Optional[int]) -> Optional[float]:
        """Calculate discount percentage"""
        if current and original and original > current:
            return round(((original - current) / original) * 100, 2)
        return None

    def _extract_images(self, driver) -> List[str]:
        """Extract product images"""
        image_urls = []
        selectors = [
            '[data-testid="product-image"] img',
            '.ProductCard img',
            '.fs-dtimage img',
            '.product-gallery img',
            'img[alt*="product"]',
            'img[src*="fptshop"]'
        ]

        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements[:5]:  # Limit to 5 images
                    src = element.get_attribute('src')
                    data_src = element.get_attribute('data-src')

                    for url in [src, data_src]:
                        if url and url not in image_urls and ('http' in url or url.startswith('/')):
                            if url.startswith('/'):
                                url = urljoin(self.base_url, url)
                            image_urls.append(url)
            except:
                continue

        return image_urls[:5]

    def get_product_urls_fast(self, category: str, max_products: int = 50) -> List[str]:
        """Fast extraction of product URLs"""
        product_urls = []

        driver = self.setup_undetected_driver()
        try:
            category_url = f"{self.base_url}{self.categories.get(category, '/dien-thoai')}"
            print(f"Getting product URLs from: {category_url}")

            driver.get(category_url)
            self.wait_for_page_load(driver)

            # Scroll to load more products
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Find product links
            link_selectors = [
                'a[href*="/dien-thoai/"]',
                'a[href*="/may-tinh-xach-tay/"]',
                'a[href*="/tai-nghe/"]',
                'a[href*="/dong-ho-thong-minh/"]',
                '.ProductCard a',
                '.product-item a'
            ]

            for selector in link_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and href not in product_urls:
                            if any(cat in href for cat in ['/dien-thoai/', '/may-tinh-xach-tay/', '/tai-nghe/', '/dong-ho-thong-minh/']):
                                product_urls.append(href)
                                if len(product_urls) >= max_products:
                                    break
                    if len(product_urls) >= max_products:
                        break
                except:
                    continue

        except Exception as e:
            print(f"Error getting product URLs: {e}")
        finally:
            driver.quit()

        print(f"Found {len(product_urls)} product URLs")
        return product_urls[:max_products]

    def crawl_products_fast(self, product_urls: List[str]) -> List[Dict]:
        """Fast crawling of products with improved anti-bot bypass"""
        products_data = []

        for i, url in enumerate(product_urls, 1):
            print(f"Crawling product {i}/{len(product_urls)}: {url}")

            driver = self.setup_undetected_driver()
            try:
                driver.get(url)
                self.wait_for_page_load(driver, timeout=15)

                # Wait for price elements to load
                time.sleep(3)

                product_data = self.extract_product_data(driver, url)
                if product_data:
                    products_data.append(product_data)
                    print(f"  OK {product_data['product_name']} - Price: {product_data['price_current']}")
                else:
                    print(f"  FAIL Failed to extract data")

            except Exception as e:
                print(f"  ERROR: {e}")
            finally:
                driver.quit()

            # Delay between products
            self.random_delay()

        return products_data

    def save_data(self, products_data: List[Dict], filename: str):
        """Save data to JSON file"""
        import os
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products_data, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(products_data)} products to {filepath}")

def main():
    crawler = FPTShopImprovedCrawler()

    # Quick test with limited products
    max_products = 20 if len(sys.argv) > 1 and sys.argv[1] == "test" else 100

    try:
        print("=== FPTShop Improved Crawler ===")
        print("Bypassing Cloudflare protection...")

        # Get product URLs
        product_urls = crawler.get_product_urls_fast("dien-thoai", max_products)

        if not product_urls:
            print("No product URLs found!")
            return

        # Crawl products
        products_data = crawler.crawl_products_fast(product_urls)

        # Save results
        timestamp = int(time.time())
        filename = f"fptshop_improved_test_{timestamp}.json"
        crawler.save_data(products_data, filename)

        # Stats
        successful = len([p for p in products_data if p.get('price_current')])
        print(f"\n=== Results ===")
        print(f"Total products: {len(products_data)}")
        print(f"With prices: {successful}")
        print(f"Success rate: {successful/len(products_data)*100:.1f}%" if products_data else "0%")

    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()