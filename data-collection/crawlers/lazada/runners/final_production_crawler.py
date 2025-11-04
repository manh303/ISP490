#!/usr/bin/env python3
"""
Final Production Lazada Crawler
Enhanced crawler with working product collection and improved price extraction
"""

import time
import json
import logging
import random
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('production_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProductionLazadaCrawler:
    """Production-ready Lazada crawler with proven product collection"""

    def __init__(self, headless: bool = True, output_dir: str = None):
        self.headless = headless
        self.output_dir = Path(output_dir) if output_dir else Path("../data/production_lazada")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.wait = None

        # Production configuration
        self.config = {
            'page_load_timeout': 30,
            'element_timeout': 15,
            'scroll_pause': 3,
            'extraction_delay': (1, 3),
            'page_delay': (5, 10)
        }

        # Enhanced price patterns
        self.price_patterns = [
            r'₫\s*([\d,\.]+)',
            r'VND\s*([\d,\.]+)',
            r'"price":\s*"?(\d+)"?',
            r'"current_price":\s*"?(\d+)"?',
            r'"selling_price":\s*"?(\d+)"?',
            r'data-price="(\d+)"',
            r'data-currency-value="(\d+)"',
            r'(\d{4,9})'  # 4-9 digit numbers (reasonable price range)
        ]

        # Comprehensive price selectors
        self.price_selectors = [
            # Main product page selectors
            '.pdp-price .currency-value',
            '.price-current .currency-value',
            '.currency-value',
            '.price .currency',

            # Listing page selectors
            '[data-qa-locator="product-price"] .currency-value',
            '[data-qa-locator="product-price"]',
            '.item-price .currency-value',
            '.product-price .currency-value',

            # Alternative selectors
            '.price-box .price',
            '.selling-price',
            '.current-price',
            '[data-spm-anchor-id*="price"]'
        ]

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'products_found': 0,
            'products_with_prices': 0,
            'products_with_ratings': 0,
            'pages_processed': 0,
            'categories_processed': 0,
            'price_extraction_attempts': 0,
            'successful_price_extractions': 0
        }

        # Categories configuration
        self.categories = {
            'smartphones': {
                'url': 'https://www.lazada.vn/dien-thoai-di-dong/',
                'name': 'Điện Thoại Di Động'
            },
            'laptops': {
                'url': 'https://www.lazada.vn/may-tinh-xach-tay/',
                'name': 'Máy Tính Xách Tay'
            },
            'tablets': {
                'url': 'https://www.lazada.vn/may-tinh-bang/',
                'name': 'Máy Tính Bảng'
            },
            'headphones': {
                'url': 'https://www.lazada.vn/tai-nghe/',
                'name': 'Tai Nghe'
            }
        }

    def setup_production_driver(self) -> bool:
        """Setup production Chrome driver"""
        try:
            logger.info("Setting up production Chrome driver...")

            options = Options()

            if self.headless:
                options.add_argument("--headless=new")

            # Essential options for stability
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")

            # Window and user agent
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            # Performance options
            options.add_argument("--disable-extensions")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")

            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(self.config['page_load_timeout'])
            self.wait = WebDriverWait(self.driver, self.config['element_timeout'])

            logger.info("SUCCESS: Production driver ready")
            return True

        except Exception as e:
            logger.error(f"Driver setup failed: {e}")
            return False

    def wait_and_scroll(self):
        """Wait for page load and perform intelligent scrolling"""
        try:
            # Wait for basic page load
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")

            # Wait for product containers
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-qa-locator='product-item']")))
            except TimeoutException:
                logger.debug("Product containers not immediately found")

            # Gradual scroll to trigger lazy loading
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            # Scroll in steps
            for step in range(0, last_height, 400):
                self.driver.execute_script(f"window.scrollTo(0, {step});")
                time.sleep(0.8)

            # Wait for any dynamic content to load
            time.sleep(self.config['scroll_pause'])

            # Return to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

        except Exception as e:
            logger.debug(f"Scroll process warning: {e}")

    def extract_price_comprehensive(self, product_element, product_url: str = None) -> float:
        """Comprehensive price extraction with multiple methods"""
        self.stats['price_extraction_attempts'] += 1

        try:
            # Method 1: Direct element text extraction
            for selector in self.price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = elem.text.strip()
                        if price_text:
                            # Apply regex patterns
                            for pattern in self.price_patterns:
                                matches = re.findall(pattern, price_text)
                                if matches:
                                    for match in matches:
                                        clean_price = re.sub(r'[,\.\s]', '', str(match))
                                        if clean_price.isdigit() and 1000 <= int(clean_price) <= 100000000:
                                            self.stats['successful_price_extractions'] += 1
                                            return float(clean_price)
                except Exception:
                    continue

            # Method 2: Data attribute extraction
            try:
                price_attrs = ['data-price', 'data-currency-value', 'data-original-price', 'data-selling-price']
                for attr in price_attrs:
                    elements = product_element.find_elements(By.CSS_SELECTOR, f"[{attr}]")
                    for elem in elements:
                        attr_value = elem.get_attribute(attr)
                        if attr_value and attr_value.isdigit():
                            price_val = int(attr_value)
                            if 1000 <= price_val <= 100000000:
                                self.stats['successful_price_extractions'] += 1
                                return float(price_val)
            except Exception:
                pass

            # Method 3: JavaScript execution for dynamic content
            try:
                js_price_script = """
                var element = arguments[0];
                var priceSelectors = ['.currency-value', '.price', '[data-price]'];

                for (var selector of priceSelectors) {
                    var priceElem = element.querySelector(selector);
                    if (priceElem) {
                        var text = priceElem.textContent || priceElem.innerText;
                        if (text) {
                            var numbers = text.match(/\\d+/g);
                            if (numbers) {
                                var price = numbers.join('');
                                if (price.length >= 4) return price;
                            }
                        }

                        var dataPrice = priceElem.getAttribute('data-price') ||
                                       priceElem.getAttribute('data-currency-value');
                        if (dataPrice) return dataPrice;
                    }
                }
                return null;
                """

                js_result = self.driver.execute_script(js_price_script, product_element)
                if js_result:
                    try:
                        price_val = int(js_result)
                        if 1000 <= price_val <= 100000000:
                            self.stats['successful_price_extractions'] += 1
                            return float(price_val)
                    except:
                        pass

            except Exception:
                pass

            # Method 4: Visit product page for detailed price (fallback)
            if product_url and random.random() < 0.1:  # Only 10% of products to avoid overload
                try:
                    current_url = self.driver.current_url
                    self.driver.get(product_url)
                    time.sleep(2)

                    # Try to find price on product detail page
                    detail_selectors = ['.pdp-price .currency-value', '.price-box .price', '.current-price']
                    for selector in detail_selectors:
                        try:
                            price_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                            price_text = price_elem.text.strip()
                            if price_text:
                                numbers = re.findall(r'\\d+', price_text.replace(',', '').replace('.', ''))
                                if numbers:
                                    combined = ''.join(numbers)
                                    if len(combined) >= 4:
                                        price_val = int(combined)
                                        if 1000 <= price_val <= 100000000:
                                            self.driver.get(current_url)  # Go back
                                            time.sleep(1)
                                            self.stats['successful_price_extractions'] += 1
                                            return float(price_val)
                        except:
                            continue

                    # Return to listing page
                    self.driver.get(current_url)
                    time.sleep(1)

                except Exception:
                    pass

            return 0.0

        except Exception as e:
            logger.debug(f"Price extraction error: {e}")
            return 0.0

    def extract_product_data(self, product_element) -> Optional[Dict[str, Any]]:
        """Extract comprehensive product data"""
        try:
            product_data = {}

            # URL and ID
            try:
                link_elements = product_element.find_elements(By.CSS_SELECTOR, "a[href]")
                if link_elements:
                    product_url = link_elements[0].get_attribute("href")
                    product_data['url'] = product_url

                    # Extract ID
                    id_match = re.search(r'i(\\d+)', product_url)
                    product_data['product_id'] = id_match.group(1) if id_match else str(uuid.uuid4())
                else:
                    return None
            except Exception:
                return None

            # Product name
            try:
                name_selectors = ["[title]", "a[title]", "h3", ".title", ".product-title"]
                product_name = "Unknown Product"

                for selector in name_selectors:
                    try:
                        elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                        for elem in elements:
                            title = elem.get_attribute("title") or elem.text
                            if title and title.strip() and len(title.strip()) > 5:
                                product_name = title.strip()
                                break
                        if product_name != "Unknown Product":
                            break
                    except:
                        continue

                product_data['name'] = product_name
            except Exception:
                product_data['name'] = "Unknown Product"

            # Price extraction
            price = self.extract_price_comprehensive(product_element, product_data.get('url'))
            product_data['price'] = price

            # Rating
            try:
                rating_selectors = [".rating-average", ".review-score", ".stars", "[data-qa-locator='product-rating']"]
                rating = None

                for selector in rating_selectors:
                    try:
                        rating_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        rating_text = rating_elem.text.strip()
                        rating_match = re.search(r'(\\d+\\.?\\d*)', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                            break
                    except:
                        continue

                product_data['rating'] = rating
                if rating:
                    self.stats['products_with_ratings'] += 1

            except Exception:
                product_data['rating'] = None

            # Images
            try:
                img_elements = product_element.find_elements(By.CSS_SELECTOR, "img[src]")
                images = []
                for img in img_elements:
                    src = img.get_attribute("src")
                    if src and ('lazcdn' in src or 'alicdn' in src):
                        images.append(src)
                        break  # Just take the first valid image
                product_data['images'] = images
            except Exception:
                product_data['images'] = []

            # Metadata
            product_data.update({
                'brand': None,
                'category': 'Electronics',
                'seller_name': None,
                'description': None,
                'availability': True,
                'crawl_timestamp': datetime.now(),
                'platform': 'lazada_vn'
            })

            # Update stats
            self.stats['products_found'] += 1
            if price > 0:
                self.stats['products_with_prices'] += 1

            return product_data

        except Exception as e:
            logger.error(f"Product data extraction error: {e}")
            return None

    def crawl_category(self, category_url: str, category_name: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """Crawl category with production stability"""
        products = []
        logger.info(f"Starting production crawl: {category_name} ({max_pages} pages)")

        try:
            for page in range(1, max_pages + 1):
                try:
                    # Build page URL
                    page_url = f"{category_url}?page={page}" if '?' not in category_url else f"{category_url}&page={page}"
                    logger.info(f"Processing page {page}/{max_pages}")

                    # Navigate and wait
                    self.driver.get(page_url)
                    self.wait_and_scroll()

                    # Find products
                    product_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")

                    if not product_elements:
                        logger.warning(f"No products found on page {page}")
                        continue

                    logger.info(f"Found {len(product_elements)} products on page {page}")

                    # Extract products
                    page_products = []
                    for i, element in enumerate(product_elements):
                        try:
                            product_data = self.extract_product_data(element)
                            if product_data:
                                product_data['category'] = category_name
                                page_products.append(product_data)

                            # Small delay every few products
                            if i > 0 and i % 10 == 0:
                                time.sleep(random.uniform(*self.config['extraction_delay']))

                        except Exception as e:
                            logger.debug(f"Product {i+1} extraction failed: {e}")

                    products.extend(page_products)
                    self.stats['pages_processed'] += 1

                    price_count = sum(1 for p in page_products if p.get('price', 0) > 0)
                    logger.info(f"Page {page} completed: {len(page_products)} products, {price_count} with prices")

                    # Delay between pages
                    time.sleep(random.uniform(*self.config['page_delay']))

                except Exception as e:
                    logger.error(f"Page {page} processing failed: {e}")

            self.stats['categories_processed'] += 1
            logger.info(f"Category {category_name} completed: {len(products)} total products")

        except Exception as e:
            logger.error(f"Category crawl failed: {e}")

        return products

    def run_production_crawl(self, categories: List[str] = None, max_pages_per_category: int = 5) -> Dict[str, Any]:
        """Run production crawl"""
        logger.info("Starting Production Lazada Crawler")

        try:
            # Setup
            if not self.setup_production_driver():
                raise Exception("Driver setup failed")

            # Categories to crawl
            categories_to_crawl = categories or ['smartphones']
            all_products = []

            # Crawl each category
            for category_key in categories_to_crawl:
                if category_key not in self.categories:
                    logger.warning(f"Unknown category: {category_key}")
                    continue

                category_info = self.categories[category_key]
                category_products = self.crawl_category(
                    category_info['url'],
                    category_info['name'],
                    max_pages_per_category
                )

                all_products.extend(category_products)

                # Inter-category delay
                if len(categories_to_crawl) > 1:
                    time.sleep(random.uniform(10, 20))

            # Save results
            if all_products:
                self.save_production_results(all_products)

            # Calculate metrics
            duration = datetime.now() - self.stats['start_time']
            price_success_rate = (self.stats['products_with_prices'] / max(self.stats['products_found'], 1)) * 100
            extraction_success_rate = (self.stats['successful_price_extractions'] / max(self.stats['price_extraction_attempts'], 1)) * 100

            return {
                'success': True,
                'summary': {
                    'products_collected': len(all_products),
                    'products_with_prices': self.stats['products_with_prices'],
                    'products_with_ratings': self.stats['products_with_ratings'],
                    'price_success_rate': price_success_rate,
                    'extraction_success_rate': extraction_success_rate,
                    'pages_processed': self.stats['pages_processed'],
                    'categories_processed': self.stats['categories_processed'],
                    'duration': str(duration)
                },
                'products': all_products
            }

        except Exception as e:
            logger.error(f"Production crawl failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'partial_stats': self.stats
            }

        finally:
            if self.driver:
                self.driver.quit()

    def save_production_results(self, products: List[Dict[str, Any]]):
        """Save production results with enhanced metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Enhanced results with metadata
        results = {
            'metadata': {
                'crawl_timestamp': datetime.now().isoformat(),
                'crawler_version': 'production_v1.0',
                'total_products': len(products),
                'products_with_prices': sum(1 for p in products if p.get('price', 0) > 0),
                'statistics': self.stats
            },
            'products': products
        }

        # Save JSON
        json_file = self.output_dir / f"production_lazada_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)

        # Save CSV
        df = pd.DataFrame(products)
        csv_file = self.output_dir / f"production_lazada_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        # Save summary report
        summary_file = self.output_dir / f"crawl_summary_{timestamp}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(results['metadata'], f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Production results saved: {json_file}")

def main():
    """Production crawler test"""
    print("Production Lazada Crawler")
    print("=" * 50)

    crawler = ProductionLazadaCrawler(headless=True)
    result = crawler.run_production_crawl(
        categories=['smartphones'],
        max_pages_per_category=3
    )

    if result['success']:
        summary = result['summary']
        print("SUCCESS: Production crawl completed!")
        print(f"Products collected: {summary['products_collected']}")
        print(f"Products with prices: {summary['products_with_prices']}")
        print(f"Price success rate: {summary['price_success_rate']:.1f}%")
        print(f"Pages processed: {summary['pages_processed']}")
        print(f"Duration: {summary['duration']}")

        # Show price samples
        products_with_prices = [p for p in result['products'] if p.get('price', 0) > 0]
        if products_with_prices:
            print("\\nPrice Samples:")
            for i, product in enumerate(products_with_prices[:3]):
                print(f"{i+1}. {product['name'][:50]}... - {product['price']:,.0f} VND")
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()