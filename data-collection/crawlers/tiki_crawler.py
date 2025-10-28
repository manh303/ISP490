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

class TikiCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            source_name="tiki",
            base_url="https://tiki.vn",
            delay_range=(2, 5)
        )

        self.categories = {
            "dien-thoai": "/dien-thoai-smartphone/c1795",
            "laptop": "/laptop/c1846",
            "tai-nghe": "/tai-nghe/c1883",
            "dong-ho-thong-minh": "/dong-ho-thong-minh/c1862",
            "phu-kien": "/phu-kien-dien-thoai/c1942"
        }

    def get_category_url(self, category: str, page: int = 1) -> str:
        category_path = self.categories.get(category, "/dien-thoai-smartphone/c1795")
        return f"{self.base_url}{category_path}?page={page}"

    def get_product_urls_from_category(self, category: str, max_pages: int = 5) -> List[str]:
        product_urls = []

        with self.setup_driver(headless=True) as driver:
            for page in range(1, max_pages + 1):
                try:
                    url = self.get_category_url(category, page)

                    if not self.can_crawl(url):
                        self.logger.warning(f"Robots.txt disallows crawling: {url}")
                        continue

                    self.logger.info(f"Crawling Tiki category {category}, page {page}: {url}")
                    driver.get(url)

                    # Wait for products to load
                    self.random_delay()

                    # Find product links
                    product_elements = self.safe_find_elements(
                        driver,
                        By.CSS_SELECTOR,
                        'a[data-view-id="pdp_main_view"], .product-item a, [data-view-label="product_list_item"] a'
                    )

                    page_urls = []
                    for element in product_elements:
                        href = self.extract_attribute_safe(element, 'href')
                        if href and href != "N/A" and ('.html' in href or '/p' in href):
                            full_url = urljoin(self.base_url, href)
                            if full_url not in product_urls:
                                page_urls.append(full_url)

                    self.logger.info(f"Found {len(page_urls)} products on page {page}")
                    product_urls.extend(page_urls)

                    # Check if we should continue
                    if not page_urls:
                        self.logger.info(f"No more products found, stopping at page {page}")
                        break

                    # Random delay between pages
                    self.random_delay()

                except Exception as e:
                    self.logger.error(f"Error crawling Tiki category {category}, page {page}: {e}")
                    continue

        self.logger.info(f"Total {len(product_urls)} unique product URLs found for {category}")
        return product_urls

    def extract_product_data(self, product_url: str) -> Optional[Dict]:
        if not self.can_crawl(product_url):
            self.logger.warning(f"Robots.txt disallows crawling: {product_url}")
            return None

        with self.setup_driver(headless=True) as driver:
            try:
                self.logger.info(f"Extracting Tiki product data from: {product_url}")
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

                # Extract sold count
                sold_count = self._extract_sold_count(driver)

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

                self.logger.info(f"Successfully extracted Tiki data for: {product_name}")
                return product_data

            except Exception as e:
                self.logger.error(f"Error extracting Tiki product data from {product_url}: {e}")
                return None

    def _extract_product_id_from_url(self, url: str) -> str:
        try:
            # Tiki product URLs typically have format: /product-name-p{product_id}.html
            import re
            match = re.search(r'-p(\d+)\.html', url)
            if match:
                return match.group(1)

            # Alternative format: /p/{product_id}/
            match = re.search(r'/p/(\d+)', url)
            if match:
                return match.group(1)

            # Fallback: try to get from query params
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            if 'spid' in query_params:
                return query_params['spid'][0]

            return url.split('/')[-1].split('.')[0]
        except:
            return "unknown"

    def _extract_product_name(self, driver) -> str:
        selectors = [
            'h1[data-view-id="pdp_details_view_product_title"]',
            'h1.title',
            '.header h1',
            'h1.product-name',
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
            '.brand-and-author a',
            '.brand-name',
            '[data-view-id="pdp_details_view_brand"]',
            '.product-brand a'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() not in ['tiki', 'shop']:
                    return text
        return "N/A"

    def _extract_category(self, driver) -> str:
        selectors = [
            '.breadcrumb a:nth-last-child(2)',
            '.breadcrumbs a:last-child',
            '[data-view-id="breadcrumb"] a:last-child'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() not in ['tiki', 'trang chủ', 'home']:
                    return text
        return "Điện tử"

    def _extract_description(self, driver) -> str:
        selectors = [
            '.product-essential-info',
            '.sku-prop-content',
            '.product-content-detail',
            '.highlight-content'
        ]

        descriptions = []
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements[:2]:  # Limit to first 2 elements
                text = self.extract_text_safe(element)
                if text != "N/A" and len(text) > 10:
                    descriptions.append(text)

        return " | ".join(descriptions[:3]) if descriptions else "N/A"

    def _extract_images(self, driver) -> List[str]:
        image_urls = []
        selectors = [
            '.product-gallery img',
            '.group-images img',
            '.thumbnail img',
            '.product-images img'
        ]

        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                src = self.extract_attribute_safe(element, 'src')
                data_src = self.extract_attribute_safe(element, 'data-src')

                for url in [src, data_src]:
                    if url and url != "N/A" and url not in image_urls and 'http' in url:
                        image_urls.append(url)

        return image_urls[:5]  # Limit to 5 images

    def _extract_prices(self, driver) -> tuple:
        # Current price
        current_price_selectors = [
            '[data-view-id="pdp_details_view_price"] .product-price__current-price',
            '.product-price__current-price',
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
            '[data-view-id="pdp_details_view_price"] .product-price__list-price',
            '.product-price__list-price',
            '.list-price',
            '.original-price'
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
            '[data-view-id="pdp_details_view_review_score"] .rating-average',
            '.rating-average',
            '.review-rating__point',
            '.stars .rating'
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
            '[data-view-id="pdp_details_view_review_score"] .review-count',
            '.review-count',
            '.review-rating__total',
            '.rating-count'
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

    def _extract_sold_count(self, driver) -> Optional[int]:
        selectors = [
            '.quantity-sold',
            '.sold-qty',
            '.review-seller__sold',
            '.quantity'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                sold_text = self.extract_text_safe(element)
                if 'đã bán' in sold_text.lower() or 'sold' in sold_text.lower():
                    sold_count = self.extract_count(sold_text)
                    if sold_count:
                        return sold_count
        return None

    def _extract_seller_info(self, driver) -> tuple:
        # Seller name
        seller_selectors = [
            '.seller-info .seller-name',
            '.current-seller .seller-name',
            '.store-info .store-name',
            '.seller-name a'
        ]

        seller_name = "N/A"
        for selector in seller_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() != 'tiki':
                    seller_name = text
                    break

        # If no seller found, default to Tiki
        if seller_name == "N/A":
            seller_name = "Tiki"

        # Determine seller type
        seller_type = "Marketplace"
        if seller_name.lower() == "tiki" or "tiki trading" in seller_name.lower():
            seller_type = "Official Store"
        elif "official" in seller_name.lower() or "chính hãng" in seller_name.lower():
            seller_type = "Official Store"

        return seller_name, seller_type

    def crawl_category(self, category: str, max_pages: int = 3, max_products: int = 50) -> List[Dict]:
        self.logger.info(f"Starting Tiki crawl for category: {category}")

        # Get product URLs
        product_urls = self.get_product_urls_from_category(category, max_pages)

        # Limit number of products
        if len(product_urls) > max_products:
            product_urls = product_urls[:max_products]
            self.logger.info(f"Limited to {max_products} products")

        # Extract data from each product
        products_data = []
        for i, url in enumerate(product_urls, 1):
            self.logger.info(f"Processing Tiki product {i}/{len(product_urls)}")

            product_data = self.extract_product_data(url)
            if product_data:
                products_data.append(product_data)

            # Random delay between products
            self.random_delay()

        self.logger.info(f"Successfully crawled {len(products_data)} Tiki products from {category}")
        return products_data

def main():
    crawler = TikiCrawler()

    # Test with phone category
    try:
        products = crawler.crawl_category("dien-thoai", max_pages=2, max_products=10)

        if products:
            filename = f"tiki_products_{crawler.source_name}_{int(time.time())}.jsonl"
            crawler.save_data(products, filename)
            print(f"Crawled {len(products)} Tiki products and saved to {filename}")
        else:
            print("No Tiki products were crawled")

    except Exception as e:
        print(f"Error during Tiki crawling: {e}")
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()