#!/usr/bin/env python3
import time
import json
import random
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urljoin, urlparse, parse_qs

from base_crawler import BaseCrawler

class TheGioiDiDongCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            source_name="thegioididong",
            base_url="https://www.thegioididong.com",
            delay_range=(2, 5)
        )

        self.categories = {
            "dien-thoai": "/dtdd",
            "laptop": "/laptop",
            "tai-nghe": "/tai-nghe",
            "dong-ho-thong-minh": "/dong-ho-thong-minh",
            "phu-kien": "/phu-kien"
        }

    def get_category_url(self, category: str, page: int = 1) -> str:
        category_path = self.categories.get(category, "/dtdd")
        if page == 1:
            return f"{self.base_url}{category_path}"
        else:
            return f"{self.base_url}{category_path}?p={page}"

    def get_product_urls_from_category(self, category: str, max_pages: int = 5) -> List[str]:
        product_urls = []

        with self.setup_driver(headless=True) as driver:
            for page in range(1, max_pages + 1):
                try:
                    url = self.get_category_url(category, page)

                    if not self.can_crawl(url):
                        self.logger.warning(f"Robots.txt disallows crawling: {url}")
                        continue

                    self.logger.info(f"Crawling TheGioiDiDong category {category}, page {page}: {url}")
                    driver.get(url)

                    # Wait for products to load
                    self.random_delay()

                    # Find product links
                    product_elements = self.safe_find_elements(
                        driver,
                        By.CSS_SELECTOR,
                        '.item a, .product a, .homeproduct a'
                    )

                    page_urls = []
                    for element in product_elements:
                        href = self.extract_attribute_safe(element, 'href')
                        if href and href != "N/A" and ('thegioididong.com' in href or href.startswith('/')):
                            full_url = urljoin(self.base_url, href)
                            if full_url not in product_urls and any(cat in full_url for cat in ['/dtdd/', '/laptop/', '/tai-nghe/', '/dong-ho/', '/phu-kien/']):
                                page_urls.append(full_url)

                    self.logger.info(f"Found {len(page_urls)} TheGioiDiDong products on page {page}")
                    product_urls.extend(page_urls)

                    # Check if we should continue
                    if not page_urls:
                        self.logger.info(f"No more TheGioiDiDong products found, stopping at page {page}")
                        break

                    # Random delay between pages
                    self.random_delay()

                except Exception as e:
                    self.logger.error(f"Error crawling TheGioiDiDong category {category}, page {page}: {e}")
                    continue

        self.logger.info(f"Total {len(product_urls)} unique TheGioiDiDong product URLs found for {category}")
        return product_urls

    def extract_product_data(self, product_url: str) -> Optional[Dict]:
        if not self.can_crawl(product_url):
            self.logger.warning(f"Robots.txt disallows crawling: {product_url}")
            return None

        with self.setup_driver(headless=True) as driver:
            try:
                self.logger.info(f"Extracting TheGioiDiDong product data from: {product_url}")
                driver.get(product_url)

                # Wait for page to load
                time.sleep(3)

                # Extract product ID from URL
                product_id = self._extract_product_id_from_url(product_url)

                # Extract product name
                product_name = self._extract_product_name(driver)

                # Extract brand
                brand = self._extract_brand(driver)

                # Extract category
                category = self._extract_category(driver)

                # Extract description
                description = self._extract_description(driver)

                # Extract images
                image_urls = self._extract_images(driver)

                # Extract prices
                price_current, price_original = self._extract_prices(driver)

                # Calculate discount
                discount_percent = self._calculate_discount(price_current, price_original)

                # Extract rating info
                rating_avg, rating_count = self._extract_rating_info(driver)

                # Extract sold count (TheGioiDiDong might not have this)
                sold_count = None

                # Extract seller info
                seller_name, seller_type = self._extract_seller_info(driver)

                product_data = self.create_product_data(
                    product_id=product_id,
                    url=product_url,
                    product_name=product_name,
                    brand=brand,
                    category=category,
                    description=description,
                    image_urls=image_urls,
                    price_current=price_current,
                    price_original=price_original,
                    discount_percent=discount_percent,
                    rating_avg=rating_avg,
                    rating_count=rating_count,
                    sold_count=sold_count,
                    seller_name=seller_name,
                    seller_type=seller_type
                )

                self.logger.info(f"Successfully extracted TheGioiDiDong data for: {product_name}")
                return product_data

            except Exception as e:
                self.logger.error(f"Error extracting TheGioiDiDong product data from {product_url}: {e}")
                return None

    def _extract_product_id_from_url(self, url: str) -> str:
        try:
            # TheGioiDiDong URLs typically have format: /dtdd/{product_name}-{product_id}
            import re
            match = re.search(r'-(\d+)$', url.rstrip('/'))
            if match:
                return match.group(1)

            # Alternative: extract from path segments
            path = urlparse(url).path
            path_parts = path.split('/')
            for part in path_parts:
                if part and part.isdigit():
                    return part

            # Last resort: use the last part of the path
            return path_parts[-1] if path_parts else "unknown"
        except:
            return "unknown"

    def _extract_product_name(self, driver) -> str:
        selectors = [
            'h1.box-product-name',
            'h1.name',
            'h1.title',
            '.product-name h1',
            'h1'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and len(text) > 3:
                    return text
        return "N/A"

    def _extract_brand(self, driver) -> str:
        selectors = [
            '.brand-name',
            '.product-brand',
            '.manufacturer'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() not in ['thegioididong', 'tgdd']:
                    return text

        # Try to extract brand from product name
        product_name_element = self.safe_find_element(driver, By.CSS_SELECTOR, 'h1')
        if product_name_element:
            product_name = self.extract_text_safe(product_name_element)
            if product_name != "N/A":
                # Common brands in product names
                brands = ['iPhone', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'Huawei', 'Nokia', 'Realme', 'OnePlus', 'Dell', 'HP', 'Asus', 'Lenovo', 'Acer', 'MSI']
                for brand in brands:
                    if brand.lower() in product_name.lower():
                        return brand

        return "N/A"

    def _extract_category(self, driver) -> str:
        selectors = [
            '.breadcrumb a:nth-last-child(2)',
            '.breadcrumbs a:last-child',
            '.navigation a:last-child'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() not in ['thegioididong', 'trang chủ', 'home']:
                    return text
        return "Điện tử"

    def _extract_description(self, driver) -> str:
        selectors = [
            '.box-product-feature',
            '.product-description',
            '.product-highlights',
            '.feature-list'
        ]

        descriptions = []
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements[:2]:
                text = self.extract_text_safe(element)
                if text != "N/A" and len(text) > 10:
                    descriptions.append(text)

        return " | ".join(descriptions[:3]) if descriptions else "N/A"

    def _extract_images(self, driver) -> List[str]:
        image_urls = []
        selectors = [
            '.box-product-image img',
            '.product-gallery img',
            '.slider img',
            '.gallery img'
        ]

        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                src = self.extract_attribute_safe(element, 'src')
                data_src = self.extract_attribute_safe(element, 'data-src')

                for url in [src, data_src]:
                    if url and url != "N/A" and url not in image_urls and 'http' in url:
                        image_urls.append(url)

        return image_urls[:5]

    def _extract_prices(self, driver) -> tuple:
        # Current price
        current_price_selectors = [
            '.box-product-price .special-price',
            '.price-current',
            '.current-price',
            '.final-price'
        ]

        price_current = None
        for selector in current_price_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                price_text = self.extract_text_safe(element)
                price_current = self.clean_price(price_text)
                if price_current:
                    break

        # Original price
        original_price_selectors = [
            '.box-product-price .old-price',
            '.price-old',
            '.original-price',
            '.regular-price'
        ]

        price_original = None
        for selector in original_price_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                price_text = self.extract_text_safe(element)
                price_original = self.clean_price(price_text)
                if price_original:
                    break

        return price_current, price_original

    def _calculate_discount(self, current: Optional[int], original: Optional[int]) -> Optional[float]:
        if current and original and original > current:
            return round(((original - current) / original) * 100, 2)
        return None

    def _extract_rating_info(self, driver) -> tuple:
        # Rating average
        rating_selectors = [
            '.box-product-rating .rating-average',
            '.rating-score',
            '.star-rating',
            '.review-score'
        ]

        rating_avg = None
        for selector in rating_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                rating_text = self.extract_text_safe(element)
                rating_avg = self.extract_rating(rating_text)
                if rating_avg:
                    break

        # Rating count
        count_selectors = [
            '.box-product-rating .rating-count',
            '.review-count',
            '.total-reviews'
        ]

        rating_count = None
        for selector in count_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                count_text = self.extract_text_safe(element)
                rating_count = self.extract_count(count_text)
                if rating_count:
                    break

        return rating_avg, rating_count

    def _extract_seller_info(self, driver) -> tuple:
        # TheGioiDiDong is primarily a retail store, so most products are sold by TheGioiDiDong
        seller_name = "Thế Giới Di Động"
        seller_type = "Retail"

        # Check if there's any seller info on the page
        seller_selectors = [
            '.seller-info',
            '.shop-name',
            '.store-name'
        ]

        for selector in seller_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() not in ['thegioididong', 'tgdd']:
                    seller_name = text
                    seller_type = "Marketplace"
                    break

        return seller_name, seller_type

    def crawl_category(self, category: str, max_pages: int = 3, max_products: int = 50) -> List[Dict]:
        self.logger.info(f"Starting TheGioiDiDong crawl for category: {category}")

        # Get product URLs
        product_urls = self.get_product_urls_from_category(category, max_pages)

        # Limit number of products
        if len(product_urls) > max_products:
            product_urls = product_urls[:max_products]
            self.logger.info(f"Limited to {max_products} products")

        # Extract data from each product
        products_data = []
        for i, url in enumerate(product_urls, 1):
            self.logger.info(f"Processing TheGioiDiDong product {i}/{len(product_urls)}")

            product_data = self.extract_product_data(url)
            if product_data:
                products_data.append(product_data)

            # Random delay between products
            self.random_delay()

        self.logger.info(f"Successfully crawled {len(products_data)} TheGioiDiDong products from {category}")
        return products_data

def main():
    crawler = TheGioiDiDongCrawler()

    # Test with phone category
    try:
        products = crawler.crawl_category("dien-thoai", max_pages=2, max_products=10)

        if products:
            filename = f"thegioididong_products_{crawler.source_name}_{int(time.time())}.jsonl"
            crawler.save_data(products, filename)
            print(f"Crawled {len(products)} TheGioiDiDong products and saved to {filename}")
        else:
            print("No TheGioiDiDong products were crawled")

    except Exception as e:
        print(f"Error during TheGioiDiDong crawling: {e}")
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()