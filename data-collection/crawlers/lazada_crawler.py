#!/usr/bin/env python3
import time
import json
import random
import os
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime

from base_crawler import BaseCrawler

class LazadaCrawler(BaseCrawler):
    def __init__(self, mode="fast"):
        # Dynamic delays based on mode
        delay_range = (0.5, 1) if mode == "fast" else (1, 2) if mode == "mass" else (3, 7)

        super().__init__(
            source_name=f"lazada_{mode}",
            base_url="https://www.lazada.vn",
            delay_range=delay_range
        )

        self.mode = mode

        # Load seed URLs from seeds.txt
        self.seed_urls = self._load_seed_urls()

        # Legacy categories for backward compatibility
        self.categories = {
            "dien-thoai": "/dien-thoai-may-tinh-bang/",
            "laptop": "/may-tinh-laptop/",
            "tai-nghe": "/tai-nghe/",
            "dong-ho-thong-minh": "/dong-ho-thong-minh/",
            "loa-bluetooth": "/loa-bluetooth/",
            "phu-kien-dien-thoai": "/phu-kien-dien-thoai/",
            "may-tinh-bang": "/may-tinh-bang/",
            "camera": "/camera-hanh-dong/",
            "tivi": "/tivi-video/",
            "may-choi-game": "/may-choi-game/"
        }

        # Statistics tracking
        self.stats = {
            'total_pages_crawled': 0,
            'total_products_found': 0,
            'total_products_extracted': 0,
            'categories_completed': 0,
            'start_time': None
        }

    def _load_seed_urls(self) -> Dict[str, str]:
        """Load seed URLs from seeds.txt file"""
        seed_urls = {}
        seeds_file = os.path.join(os.path.dirname(__file__), 'seeds.txt')

        try:
            with open(seeds_file, 'r', encoding='utf-8') as f:
                current_category = None
                for line in f:
                    line = line.strip()
                    if line.startswith('#'):
                        # Category name
                        current_category = line[1:].strip().replace(' ', '_').lower()
                    elif line.startswith('http') and current_category:
                        # Clean URL to remove page parameter
                        base_url = line.split('&page=')[0]
                        seed_urls[current_category] = base_url

            print(f"Loaded {len(seed_urls)} seed URLs from seeds.txt")
            return seed_urls

        except FileNotFoundError:
            print("seeds.txt not found, using default categories")
            return {}
        except Exception as e:
            print(f"Error loading seeds.txt: {e}")
            return {}

    def get_category_url(self, category: str, page: int = 1) -> str:
        category_path = self.categories.get(category, "/dien-thoai-may-tinh-bang/")
        return f"{self.base_url}{category_path}?page={page}"

    def get_product_urls_from_category(self, category: str, max_pages: int = 100) -> List[str]:
        """Optimized URL collection with automatic pagination detection"""
        product_urls = set()
        driver = None

        try:
            driver = self.setup_driver(headless=True)
            driver.set_page_load_timeout(10)  # Faster timeout

            page = 1
            consecutive_empty_pages = 0

            print(f"Starting URL collection for {category}...")

            while page <= max_pages and consecutive_empty_pages < 3:
                try:
                    url = self.get_category_url(category, page)
                    print(f"  Scanning page {page}: {url}")

                    driver.get(url)

                    # Optimized loading wait
                    time.sleep(2 if self.mode == "fast" else 3)

                    # Trigger lazy loading
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                    time.sleep(1)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)

                    # Use proven selector logic from super_fast_lazada
                    page_urls = set()
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/products/"]')
                        for element in elements:
                            href = element.get_attribute('href')
                            if href and '/products/' in href:
                                clean_url = href.split('?')[0]
                                if clean_url not in page_urls:
                                    page_urls.add(clean_url)
                    except Exception as e:
                        print(f"    Error finding URLs: {e}")

                    new_urls = page_urls - product_urls

                    if len(new_urls) == 0:
                        consecutive_empty_pages += 1
                        print(f"    No new products found (empty page {consecutive_empty_pages}/3)")
                    else:
                        consecutive_empty_pages = 0
                        product_urls.update(new_urls)
                        print(f"    Found {len(new_urls)} new products (total: {len(product_urls)})")

                    self.stats['total_pages_crawled'] += 1
                    page += 1

                    # Progressive delay for anti-bot
                    if page % 10 == 0 and self.mode == "mass":
                        print(f"    Taking break at page {page}...")
                        time.sleep(random.uniform(5, 10))
                    else:
                        delay = random.uniform(*self.delay_range)
                        time.sleep(delay)

                except Exception as e:
                    print(f"    Error on page {page}: {e}")
                    consecutive_empty_pages += 1
                    page += 1
                    continue

        finally:
            if driver:
                driver.quit()

        urls_list = list(product_urls)
        print(f"URL collection completed: {len(urls_list)} unique URLs from {page-1} pages")
        self.stats['total_products_found'] = len(urls_list)
        return urls_list

    def extract_product_data_batch(self, product_urls: List[str], batch_size: int = 15) -> List[Dict]:
        """Optimized batch extraction using single browser session"""
        all_products = []
        total_batches = (len(product_urls) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(product_urls))
            batch_urls = product_urls[start_idx:end_idx]

            print(f"  Processing batch {batch_num + 1}/{total_batches} ({len(batch_urls)} products)")

            batch_products = self._extract_single_batch(batch_urls)
            all_products.extend(batch_products)

            print(f"    Batch result: {len(batch_products)}/{len(batch_urls)} extracted")
            print(f"    Total progress: {len(all_products)}/{len(product_urls)} products")

            # Short break between batches
            if batch_num < total_batches - 1:
                time.sleep(random.uniform(2, 4))

        return all_products

    def _extract_single_batch(self, urls: List[str]) -> List[Dict]:
        """Extract products using single browser session for efficiency"""
        products = []
        driver = None

        try:
            driver = self.setup_driver(headless=True)
            driver.set_page_load_timeout(8)  # Aggressive timeout

            for i, url in enumerate(urls, 1):
                try:
                    driver.get(url)
                    time.sleep(1)  # Minimal wait

                    product_data = self._extract_product_fast(driver, url)
                    if product_data:
                        products.append(product_data)
                        print(f"      {i}/{len(urls)}: SUCCESS")
                    else:
                        print(f"      {i}/{len(urls)}: FAILED")

                except Exception as e:
                    print(f"      {i}/{len(urls)}: ERROR")
                    continue

        finally:
            if driver:
                driver.quit()

        return products

    def _extract_product_fast(self, driver, url: str) -> Optional[Dict]:
        """Fast product extraction with minimal selectors"""
        try:
            # Product ID
            import re
            match = re.search(r'-i(\d+)', url)
            product_id = match.group(1) if match else "unknown"

            # Product name - fast selectors only
            name = "N/A"
            for selector in ['h1.pdp-product-title', 'h1[class*="title"]', 'h1']:
                try:
                    element = WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    name = element.text.strip()
                    if name and len(name) > 3:
                        break
                except:
                    continue

            if name == "N/A":
                return None

            # Price - single fast selector
            price = None
            try:
                element = driver.find_element(By.CSS_SELECTOR, '.pdp-price_color_orange')
                price_text = element.text.strip()
                price = self.clean_price(price_text)
            except:
                pass

            # Brand detection from name
            brand = "N/A"
            brands = ['iPhone', 'Samsung', 'Xiaomi', 'Oppo', 'Apple', 'HP', 'Dell', 'Asus']
            for b in brands:
                if b.lower() in name.lower():
                    brand = b
                    break

            # Images - limited for speed
            images = []
            try:
                img_elements = driver.find_elements(By.CSS_SELECTOR, 'img')[:3]
                for img in img_elements:
                    src = img.get_attribute('src')
                    if src and 'http' in src and any(ext in src for ext in ['.jpg', '.png', '.jpeg']):
                        images.append(src)
                        if len(images) >= 2:
                            break
            except:
                pass

            return {
                "source": self.source_name,
                "product_id": product_id,
                "url": url,
                "crawl_date": datetime.now().isoformat(),
                "product_name": name,
                "brand": brand,
                "category": "Electronics",
                "description": "N/A",
                "image_urls": images,
                "price_current": price,
                "price_original": None,
                "discount_percent": None,
                "rating_avg": None,
                "rating_count": None,
                "sold_count": None,
                "favorite_count": None,
                "seller_name": "N/A",
                "seller_type": "Marketplace"
            }

        except:
            return None

    def _extract_product_id_from_url(self, url: str) -> str:
        try:
            # Lazada product URLs typically have format: /products/name-i{product_id}.html
            import re
            match = re.search(r'-i(\d+)\.html', url)
            if match:
                return match.group(1)

            # Fallback: try to get from query params
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            if 'itemId' in query_params:
                return query_params['itemId'][0]

            # Last resort: extract from path
            path_parts = parsed_url.path.split('/')
            for part in path_parts:
                if part.startswith('i') and part[1:].isdigit():
                    return part[1:]

            return url.split('/')[-1].split('.')[0]
        except:
            return "unknown"

    def _extract_product_name(self, driver) -> str:
        selectors = [
            'h1[data-spm-anchor-id="a2o4n.pdp.product_title"]',
            'h1.x-product-title-label',
            'h1[data-spm="product_title"]',
            'h1.pdp-mod-product-badge-title',
            'h1.pdp-product-title',
            '.pdp-product-title',
            'h1[class*="title"]',
            'h1[class*="product"]',
            'h1',
            '.title',
            '[data-spm*="title"]'
        ]

        for selector in selectors:
            try:
                element = self.safe_find_element(driver, By.CSS_SELECTOR, selector, timeout=3)
                if element:
                    text = self.extract_text_safe(element)
                    if text != "N/A" and len(text) > 3:
                        self.logger.info(f"Found product name with selector {selector}: {text[:50]}...")
                        return text
            except Exception as e:
                continue

        # Nếu không tìm thấy, thử extract từ title tag
        try:
            title = driver.title
            if title and len(title) > 10 and 'lazada' not in title.lower():
                return title.split(' | ')[0].split(' - ')[0]
        except:
            pass

        return "N/A"

    def _extract_brand(self, driver) -> str:
        selectors = [
            '.pdp-product-brand a',
            '.brand-name',
            '[data-spm="brand"]',
            '.pdp-mod-product-badge-title-brand'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A":
                    return text
        return "N/A"

    def _extract_category(self, driver) -> str:
        selectors = [
            '.breadcrumb a:last-child',
            '.nav-breadcrumb a:last-child',
            '[data-spm="breadcrumb"] a:last-child'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A" and text.lower() not in ['home', 'trang chủ']:
                    return text
        return "Điện tử"

    def _extract_description(self, driver) -> str:
        selectors = [
            '.pdp-product-highlights',
            '.product-highlights',
            '.pdp-mod-specification',
            '.key-features'
        ]

        descriptions = []
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = self.extract_text_safe(element)
                if text != "N/A" and len(text) > 10:
                    descriptions.append(text)

        return " | ".join(descriptions[:3]) if descriptions else "N/A"

    def _extract_images(self, driver) -> List[str]:
        image_urls = []
        selectors = [
            '.item-gallery__thumbnail img',
            '.pdp-gallery img',
            '.gallery-preview-panel img',
            '.pdp-mod-common-image img'
        ]

        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                src = self.extract_attribute_safe(element, 'src')
                if src != "N/A" and src not in image_urls:
                    image_urls.append(src)

        return image_urls[:5]  # Limit to 5 images

    def _extract_prices(self, driver) -> tuple:
        # Current price - nhiều selector hơn
        current_price_selectors = [
            '.pdp-price_color_orange',
            '.pdp-mod-product-price-current',
            '.current-price',
            '[data-spm="price"]',
            '.price-current',
            '[class*="price"][class*="current"]',
            '[class*="special"][class*="price"]',
            '.pdp-price .pdp-price_size_xl',
            'span[class*="price"]',
            '.price'
        ]

        price_current = None
        for selector in current_price_selectors:
            try:
                element = self.safe_find_element(driver, By.CSS_SELECTOR, selector, timeout=2)
                if element:
                    price_text = self.extract_text_safe(element)
                    price_current = self.clean_price(price_text)
                    if price_current and price_current > 1000:  # Giá hợp lý
                        self.logger.info(f"Found current price with {selector}: {price_current}")
                        break
            except:
                continue

        # Original price
        original_price_selectors = [
            '.pdp-price_type_deleted',
            '.pdp-mod-product-price-original',
            '.original-price',
            '.origin-price',
            '[class*="price"][class*="original"]',
            '[class*="price"][class*="old"]',
            '.price-old',
            'del',
            's'
        ]

        price_original = None
        for selector in original_price_selectors:
            try:
                element = self.safe_find_element(driver, By.CSS_SELECTOR, selector, timeout=2)
                if element:
                    price_text = self.extract_text_safe(element)
                    price_original = self.clean_price(price_text)
                    if price_original and price_original > price_current:
                        self.logger.info(f"Found original price with {selector}: {price_original}")
                        break
            except:
                continue

        return price_current, price_original

    def _calculate_discount(self, current: Optional[int], original: Optional[int]) -> Optional[float]:
        if current and original and original > current:
            return round(((original - current) / original) * 100, 2)
        return None

    def _extract_rating_info(self, driver) -> tuple:
        # Rating average
        rating_selectors = [
            '.score-average',
            '.pdp-review-summary__score',
            '.rate-score',
            '[data-spm="rating_score"]'
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
            '.review-count',
            '.pdp-review-summary__count',
            '.rate-count',
            '[data-spm="rating_count"]'
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
            '.sold-count',
            '[data-spm="sold"]'
        ]

        for selector in selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                sold_text = self.extract_text_safe(element)
                sold_count = self.extract_count(sold_text)
                if sold_count:
                    return sold_count
        return None

    def _extract_seller_info(self, driver) -> tuple:
        # Seller name
        seller_selectors = [
            '.seller-name a',
            '.pdp-seller-name',
            '[data-spm="seller"] a',
            '.seller-info .seller-name'
        ]

        seller_name = "N/A"
        for selector in seller_selectors:
            element = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
            if element:
                text = self.extract_text_safe(element)
                if text != "N/A":
                    seller_name = text
                    break

        # Determine seller type
        seller_type = "Marketplace"
        if "official" in seller_name.lower() or "chính hãng" in seller_name.lower():
            seller_type = "Official Store"

        return seller_name, seller_type

    def crawl_category(self, category: str, max_pages: int = 50, max_products: int = 1000) -> List[Dict]:
        """Enhanced category crawling with batch processing"""
        print(f"Starting {self.mode.upper()} Lazada crawl for category: {category}")
        print(f"Settings: max_pages={max_pages}, max_products={max_products}")

        self.stats['start_time'] = time.time()

        # Phase 1: Collect URLs
        print(f"\nPhase 1: Collecting product URLs...")
        product_urls = self.get_product_urls_from_category(category, max_pages)

        if len(product_urls) > max_products:
            product_urls = product_urls[:max_products]
            print(f"Limited to {max_products} products for processing")

        # Phase 2: Extract data
        print(f"\nPhase 2: Extracting product data from {len(product_urls)} products...")
        products_data = self.extract_product_data_batch(product_urls)

        self.stats['total_products_extracted'] = len(products_data)

        duration = time.time() - self.stats['start_time']
        success_rate = len(products_data) / len(product_urls) * 100 if product_urls else 0

        print(f"\nCategory {category} completed:")
        print(f"  Duration: {duration:.1f} seconds")
        print(f"  URLs found: {len(product_urls)}")
        print(f"  Products extracted: {len(products_data)}")
        print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Speed: {len(products_data)/(duration/60):.1f} products/minute")

        return products_data

    def crawl_all_electronics(self, max_pages_per_category: int = 30, max_products_per_category: int = 1000) -> Dict:
        """Mass crawl ALL electronics categories"""
        print(f"=== MASS ELECTRONICS CRAWL ===")
        print(f"Target: ALL electronics products")
        print(f"Max pages per category: {max_pages_per_category}")
        print(f"Max products per category: {max_products_per_category}")
        print(f"Categories: {len(self.categories)}")

        self.stats['start_time'] = time.time()

        all_results = {
            'categories': {},
            'summary': {},
            'files_created': []
        }

        # Crawl each electronics category
        for category_name in self.categories.keys():
            print(f"\n{'='*60}")
            print(f"CRAWLING CATEGORY: {category_name.upper()}")
            print(f"{'='*60}")

            try:
                # Crawl this category
                category_results = self.crawl_category(
                    category_name,
                    max_pages_per_category,
                    max_products_per_category
                )

                all_results['categories'][category_name] = {
                    'products': category_results,
                    'count': len(category_results)
                }

                # Save category data immediately
                if category_results:
                    filename = f"lazada_{category_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
                    self.save_category_data(category_results, filename)
                    all_results['files_created'].append(filename)

                self.stats['categories_completed'] += 1

                # Break between categories
                time.sleep(random.uniform(3, 7))

            except Exception as e:
                print(f"ERROR in category {category_name}: {e}")
                all_results['categories'][category_name] = {
                    'error': str(e),
                    'products': [],
                    'count': 0
                }

        # Create final summary
        all_results['summary'] = self._create_final_summary(all_results)

        # Save master summary
        summary_filename = f"lazada_electronics_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self._save_summary(all_results['summary'], summary_filename)

        self._print_final_report(all_results)

        return all_results

    def crawl_seed_urls_with_pagination(self, max_pages_per_seed: int = 50, max_products_per_seed: int = 1000) -> Dict:
        """Crawl all seed URLs from seeds.txt with pagination"""
        print(f"=== SEED URLS CRAWL WITH PAGINATION ===")
        print(f"Loaded {len(self.seed_urls)} seed URLs from seeds.txt")
        print(f"Max pages per seed: {max_pages_per_seed}")
        print(f"Max products per seed: {max_products_per_seed}")

        self.stats['start_time'] = time.time()

        all_results = {
            'seed_categories': {},
            'summary': {},
            'files_created': []
        }

        # Crawl each seed URL
        for category_name, base_url in self.seed_urls.items():
            print(f"\n{'='*60}")
            print(f"CRAWLING SEED: {category_name.upper()}")
            print(f"Base URL: {base_url}")
            print(f"{'='*60}")

            try:
                # Crawl this seed URL with pagination
                seed_results = self.crawl_seed_with_pagination(
                    category_name,
                    base_url,
                    max_pages_per_seed,
                    max_products_per_seed
                )

                all_results['seed_categories'][category_name] = {
                    'products': seed_results,
                    'count': len(seed_results)
                }

                # Save seed data immediately
                if seed_results:
                    filename = f"seed_{category_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
                    self.save_category_data(seed_results, filename)
                    all_results['files_created'].append(filename)

                self.stats['categories_completed'] += 1

                # Break between seeds
                time.sleep(random.uniform(3, 7))

            except Exception as e:
                print(f"ERROR in seed {category_name}: {e}")
                all_results['seed_categories'][category_name] = {
                    'error': str(e),
                    'products': [],
                    'count': 0
                }

        # Create final summary
        all_results['summary'] = self._create_seed_summary(all_results)

        # Save master summary
        summary_filename = f"seed_crawl_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self._save_summary(all_results['summary'], summary_filename)

        self._print_seed_report(all_results)

        return all_results

    def crawl_seed_with_pagination(self, category_name: str, base_url: str, max_pages: int, max_products: int) -> List[Dict]:
        """Crawl a single seed URL with pagination"""
        print(f"Starting pagination crawl for {category_name}")
        print(f"Settings: max_pages={max_pages}, max_products={max_products}")

        start_time = time.time()

        # Phase 1: Collect URLs with pagination
        print(f"\nPhase 1: Collecting product URLs with pagination...")
        product_urls = self.get_product_urls_from_seed(base_url, max_pages)

        if len(product_urls) > max_products:
            product_urls = product_urls[:max_products]
            print(f"Limited to {max_products} products for processing")

        # Phase 2: Extract data
        print(f"\nPhase 2: Extracting product data from {len(product_urls)} products...")
        products_data = self.extract_product_data_batch(product_urls)

        duration = time.time() - start_time
        success_rate = len(products_data) / len(product_urls) * 100 if product_urls else 0

        print(f"\nSeed {category_name} completed:")
        print(f"  Duration: {duration:.1f} seconds")
        print(f"  URLs found: {len(product_urls)}")
        print(f"  Products extracted: {len(products_data)}")
        print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Speed: {len(products_data)/(duration/60):.1f} products/minute")

        return products_data

    def get_product_urls_from_seed(self, base_url: str, max_pages: int) -> List[str]:
        """Collect product URLs from seed URL with pagination"""
        product_urls = set()
        driver = None

        try:
            driver = self.setup_driver(headless=True)
            driver.set_page_load_timeout(10)

            page = 1
            consecutive_empty_pages = 0

            print(f"Starting URL collection from seed...")

            while page <= max_pages and consecutive_empty_pages < 3:
                try:
                    # Build paginated URL
                    if '&page=' in base_url:
                        # Replace existing page parameter
                        url = base_url.replace('&page=2', f'&page={page}')
                    elif '?' in base_url:
                        # URL already has query parameters, use &
                        url = f"{base_url}&page={page}"
                    else:
                        # URL has no query parameters, use ?
                        url = f"{base_url}?page={page}"

                    print(f"  Scanning page {page}: {url}")

                    driver.get(url)

                    # Optimized loading wait
                    time.sleep(2 if self.mode == "fast" else 3)

                    # Trigger lazy loading
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                    time.sleep(1)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)

                    # Find product URLs
                    page_urls = set()
                    try:
                        # Try multiple selectors for product links
                        selectors = [
                            'a[href*="/products/"]',
                            'a[data-spm*="product"]',
                            '.c16H9d a[href*="-i"]',
                            '[data-qa-locator="product-item"] a'
                        ]

                        for selector in selectors:
                            try:
                                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                for element in elements:
                                    href = element.get_attribute('href')
                                    if href and ('/products/' in href or '-i' in href):
                                        clean_url = href.split('?')[0]
                                        if clean_url not in page_urls:
                                            page_urls.add(clean_url)
                            except:
                                continue

                    except Exception as e:
                        print(f"    Error finding URLs: {e}")

                    new_urls = page_urls - product_urls

                    if len(new_urls) == 0:
                        consecutive_empty_pages += 1
                        print(f"    No new products found (empty page {consecutive_empty_pages}/3)")
                    else:
                        consecutive_empty_pages = 0
                        product_urls.update(new_urls)
                        print(f"    Found {len(new_urls)} new products (total: {len(product_urls)})")

                    self.stats['total_pages_crawled'] += 1
                    page += 1

                    # Progressive delay for anti-bot
                    if page % 10 == 0 and self.mode == "mass":
                        print(f"    Taking break at page {page}...")
                        time.sleep(random.uniform(5, 10))
                    else:
                        delay = random.uniform(*self.delay_range)
                        time.sleep(delay)

                except Exception as e:
                    print(f"    Error on page {page}: {e}")
                    consecutive_empty_pages += 1
                    page += 1
                    continue

        finally:
            if driver:
                driver.quit()

        urls_list = list(product_urls)
        print(f"Seed URL collection completed: {len(urls_list)} unique URLs from {page-1} pages")
        return urls_list

    def _create_seed_summary(self, all_results: Dict) -> Dict:
        """Create seed crawl summary"""
        total_products = sum(cat_data.get('count', 0) for cat_data in all_results['seed_categories'].values())
        duration = time.time() - self.stats['start_time']

        return {
            'crawl_date': datetime.now().isoformat(),
            'crawl_type': 'seed_urls_pagination',
            'mode': self.mode,
            'total_duration_seconds': round(duration, 2),
            'total_duration_hours': round(duration / 3600, 2),
            'seed_categories_crawled': len(self.seed_urls),
            'seed_categories_completed': self.stats['categories_completed'],
            'total_pages_crawled': self.stats['total_pages_crawled'],
            'total_products_extracted': total_products,
            'products_per_minute': round(total_products / (duration / 60), 2) if duration > 0 else 0,
            'seed_breakdown': {cat: data.get('count', 0) for cat, data in all_results['seed_categories'].items()}
        }

    def _print_seed_report(self, results: Dict):
        """Print seed crawl report"""
        summary = results['summary']

        print(f"\n{'='*80}")
        print(f"SEED URLS PAGINATION CRAWL - FINAL REPORT")
        print(f"{'='*80}")
        print(f"Mode: {summary['mode'].upper()}")
        print(f"Crawl Date: {summary['crawl_date']}")
        print(f"Total Duration: {summary['total_duration_hours']:.2f} hours ({summary['total_duration_seconds']:.0f} seconds)")
        print(f"Seed Categories Processed: {summary['seed_categories_completed']}/{summary['seed_categories_crawled']}")
        print(f"Total Pages Crawled: {summary['total_pages_crawled']}")
        print(f"Total Products Extracted: {summary['total_products_extracted']:,}")
        print(f"Speed: {summary['products_per_minute']:.1f} products/minute")

        print(f"\nSEED CATEGORY BREAKDOWN:")
        print("-" * 70)
        for cat_name, count in summary['seed_breakdown'].items():
            print(f"{cat_name:30s} | Products: {count:6,}")

        print(f"\nFILES CREATED:")
        print("-" * 30)
        for filename in results['files_created']:
            print(f"  - {filename}")

        print(f"\nSEED CRAWL COMPLETED!")
        print(f"Total products collected: {summary['total_products_extracted']:,}")
        print(f"Data saved in: data/mass_crawl/")

    def save_category_data(self, products: List[Dict], filename: str):
        """Save category data to file"""
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'mass_crawl')
        os.makedirs(data_dir, exist_ok=True)

        filepath = os.path.join(data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for product in products:
                json.dump(product, f, ensure_ascii=False)
                f.write('\n')

        print(f"  Saved {len(products)} products to {filename}")

    def _save_summary(self, summary: Dict, filename: str):
        """Save crawl summary"""
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'mass_crawl')
        os.makedirs(data_dir, exist_ok=True)

        filepath = os.path.join(data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(f"Summary saved to {filename}")

    def _create_final_summary(self, all_results: Dict) -> Dict:
        """Create final crawl summary"""
        total_products = sum(cat_data.get('count', 0) for cat_data in all_results['categories'].values())
        duration = time.time() - self.stats['start_time']

        return {
            'crawl_date': datetime.now().isoformat(),
            'mode': self.mode,
            'total_duration_seconds': round(duration, 2),
            'total_duration_hours': round(duration / 3600, 2),
            'categories_crawled': len(self.categories),
            'categories_completed': self.stats['categories_completed'],
            'total_pages_crawled': self.stats['total_pages_crawled'],
            'total_products_extracted': total_products,
            'products_per_minute': round(total_products / (duration / 60), 2) if duration > 0 else 0,
            'categories_breakdown': {cat: data.get('count', 0) for cat, data in all_results['categories'].items()}
        }

    def _print_final_report(self, results: Dict):
        """Print comprehensive final report"""
        summary = results['summary']

        print(f"\n{'='*80}")
        print(f"LAZADA ELECTRONICS CRAWL - FINAL REPORT")
        print(f"{'='*80}")
        print(f"Mode: {summary['mode'].upper()}")
        print(f"Crawl Date: {summary['crawl_date']}")
        print(f"Total Duration: {summary['total_duration_hours']:.2f} hours ({summary['total_duration_seconds']:.0f} seconds)")
        print(f"Categories Processed: {summary['categories_completed']}/{summary['categories_crawled']}")
        print(f"Total Pages Crawled: {summary['total_pages_crawled']}")
        print(f"Total Products Extracted: {summary['total_products_extracted']:,}")
        print(f"Speed: {summary['products_per_minute']:.1f} products/minute")

        print(f"\nCATEGORY BREAKDOWN:")
        print("-" * 50)
        for cat_name, count in summary['categories_breakdown'].items():
            print(f"{cat_name:20s} | Products: {count:6,}")

        print(f"\nFILES CREATED:")
        print("-" * 30)
        for filename in results['files_created']:
            print(f"  - {filename}")

        print(f"\nCRAWL COMPLETED!")
        print(f"Total products collected: {summary['total_products_extracted']:,}")
        print(f"Data saved in: data/mass_crawl/")

def main():
    import sys

    # Parse command line arguments for mode
    mode = "fast"  # default
    if len(sys.argv) > 1:
        if sys.argv[1] in ["fast", "mass", "demo"]:
            mode = sys.argv[1]

    print(f"Starting Lazada Crawler in {mode.upper()} mode")

    crawler = LazadaCrawler(mode=mode)

    try:
        if mode == "fast":
            # Fast mode: 5-10 products for quick testing
            print("FAST MODE: Quick testing with 10 products")
            products = crawler.crawl_category("dien-thoai", max_pages=2, max_products=10)

            if products:
                filename = f"lazada_fast_{int(time.time())}.jsonl"
                crawler.save_data(products, filename)
                print(f"Fast crawl completed: {len(products)} products saved to {filename}")

        elif mode == "demo":
            # Demo mode: 50-100 products to demonstrate capabilities
            print("DEMO MODE: Demonstrating pagination with 100 products")
            products = crawler.crawl_category("dien-thoai", max_pages=10, max_products=100)

            if products:
                filename = f"lazada_demo_{int(time.time())}.jsonl"
                crawler.save_data(products, filename)
                print(f"Demo crawl completed: {len(products)} products saved to {filename}")

        elif mode == "mass":
            # Mass mode: ALL electronics products with full pagination
            print("MASS MODE: Crawling ALL electronics categories")
            print("This will take several hours and collect thousands of products")

            confirm = input("Continue with mass crawl? (y/N): ")
            if confirm.lower() == 'y':
                results = crawler.crawl_all_electronics(
                    max_pages_per_category=50,  # Up to 50 pages per category
                    max_products_per_category=2000  # Up to 2000 products per category
                )
                print(f"\nMASS CRAWL COMPLETED!")
                print(f"Total products: {results['summary']['total_products_extracted']:,}")
                print(f"Duration: {results['summary']['total_duration_hours']:.1f} hours")
            else:
                print("Mass crawl cancelled")

    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
    except Exception as e:
        print(f"Error during crawling: {e}")
    finally:
        crawler.cleanup()

# Quick access functions for different modes
def crawl_fast():
    """Quick crawl for testing (10 products)"""
    crawler = LazadaCrawler(mode="fast")
    try:
        products = crawler.crawl_category("dien-thoai", max_pages=2, max_products=10)
        if products:
            filename = f"lazada_fast_{int(time.time())}.jsonl"
            crawler.save_data(products, filename)
            print(f"Fast crawl: {len(products)} products → {filename}")
        return products
    finally:
        crawler.cleanup()

def crawl_demo():
    """Demo crawl showing pagination (100 products)"""
    crawler = LazadaCrawler(mode="demo")
    try:
        products = crawler.crawl_category("dien-thoai", max_pages=10, max_products=100)
        if products:
            filename = f"lazada_demo_{int(time.time())}.jsonl"
            crawler.save_data(products, filename)
            print(f"Demo crawl: {len(products)} products → {filename}")
        return products
    finally:
        crawler.cleanup()

def crawl_mass():
    """Mass crawl for ALL electronics (10,000+ products)"""
    crawler = LazadaCrawler(mode="mass")
    try:
        results = crawler.crawl_all_electronics()
        print(f"Mass crawl: {results['summary']['total_products_extracted']:,} products")
        return results
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()