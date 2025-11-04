#!/usr/bin/env python3
"""
JavaScript-Enabled Lazada Crawler
Sử dụng Selenium với JavaScript để load dynamic content và extract prices
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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('js_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class JavaScriptEnabledCrawler:
    """Crawler with JavaScript execution for dynamic content"""

    def __init__(self, headless: bool = False, output_dir: str = None):  # Default to non-headless for debugging
        self.headless = headless
        self.output_dir = Path(output_dir) if output_dir else Path("../data/js_lazada_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.wait = None

        # Less aggressive configuration for stability
        self.config = {
            'page_load_timeout': 30,
            'element_timeout': 15,
            'scroll_pause': 2,
            'human_delays': (2, 5),
            'max_retries': 3
        }

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_products': 0,
            'products_with_prices': 0,
            'pages_processed': 0,
            'javascript_executions': 0
        }

    def setup_javascript_driver(self) -> bool:
        """Setup Chrome with JavaScript enabled"""
        try:
            logger.info("Setting up JavaScript-enabled Chrome driver...")

            options = Options()

            if self.headless:
                options.add_argument("--headless=new")

            # Essential options only
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            # Enable JavaScript (default, but explicit)
            # Don't disable JavaScript this time!

            # Window size for proper rendering
            options.add_argument("--window-size=1920,1080")

            # User agent
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            # Disable some features for stability
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--no-first-run")

            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(self.config['page_load_timeout'])

            # Setup wait
            self.wait = WebDriverWait(self.driver, self.config['element_timeout'])

            logger.info("SUCCESS: JavaScript-enabled driver ready")
            return True

        except Exception as e:
            logger.error(f"Driver setup failed: {e}")
            return False

    def wait_for_dynamic_content(self, timeout: int = 15):
        """Wait for dynamic content to load"""
        try:
            # Wait for page to be fully loaded
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")

            # Wait specifically for product containers to appear
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-qa-locator='product-item'], .product-item, [data-spm*='product']")))
                logger.debug("Product containers detected")
            except TimeoutException:
                logger.warning("No product containers found, but continuing...")

            # Additional wait for dynamic content
            time.sleep(self.config['scroll_pause'])

        except Exception as e:
            logger.debug(f"Dynamic content wait warning: {e}")

    def scroll_to_load_content(self):
        """Scroll page to trigger lazy loading"""
        try:
            # Get initial height
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            # Scroll gradually
            scroll_pause_time = 1
            scroll_step = 300

            for i in range(0, last_height, scroll_step):
                self.driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(scroll_pause_time)

                # Check if new content loaded
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    last_height = new_height

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            logger.debug("Page scrolling completed")

        except Exception as e:
            logger.warning(f"Scrolling error: {e}")

    def extract_price_with_javascript(self, product_element=None) -> float:
        """Extract price using JavaScript execution"""
        try:
            self.stats['javascript_executions'] += 1

            # JavaScript to find prices in various ways
            price_extraction_script = '''
            // Method 1: Look for price in various selectors
            var priceSelectors = [
                '.currency-value',
                '.price-current',
                '.price .currency',
                '[data-qa-locator="product-price"] .currency-value',
                '.pdp-price .currency-value',
                '.item-price .currency-value'
            ];

            var price = null;

            for (var selector of priceSelectors) {
                var elements = document.querySelectorAll(selector);
                for (var element of elements) {
                    var text = element.textContent || element.innerText;
                    if (text && text.trim()) {
                        var numbers = text.match(/[0-9,\.]+/g);
                        if (numbers && numbers.length > 0) {
                            var priceStr = numbers[0].replace(/[,\.]/g, '');
                            if (priceStr.length >= 4) {
                                price = priceStr;
                                break;
                            }
                        }
                    }
                }
                if (price) break;
            }

            // Method 2: Look in data attributes
            if (!price) {
                var allElements = document.querySelectorAll('[data-price], [data-original-price], [data-currency-value]');
                for (var element of allElements) {
                    var dataPrice = element.getAttribute('data-price') ||
                                   element.getAttribute('data-original-price') ||
                                   element.getAttribute('data-currency-value');
                    if (dataPrice && dataPrice.match(/^[0-9]+$/)) {
                        price = dataPrice;
                        break;
                    }
                }
            }

            // Method 3: Look for specific patterns in all text
            if (!price) {
                var bodyText = document.body.innerHTML;
                var pricePatterns = [
                    /"price"[:\s]*"?([0-9]+)"?/g,
                    /"current_price"[:\s]*"?([0-9]+)"?/g,
                    /"selling_price"[:\s]*"?([0-9]+)"?/g,
                    /data-price="([0-9]+)"/g
                ];

                for (var pattern of pricePatterns) {
                    var match = pattern.exec(bodyText);
                    if (match && match[1]) {
                        price = match[1];
                        break;
                    }
                }
            }

            return price;
            '''

            # Execute the script
            price_result = self.driver.execute_script(price_extraction_script)

            if price_result:
                try:
                    price_value = float(price_result)
                    if 1000 <= price_value <= 100000000:  # Reasonable price range
                        logger.debug(f"Price extracted via JS: {price_value}")
                        return price_value
                except:
                    pass

            return 0.0

        except Exception as e:
            logger.debug(f"JavaScript price extraction error: {e}")
            return 0.0

    def extract_product_with_js(self, product_element) -> Optional[Dict[str, Any]]:
        """Extract product information with JavaScript assistance"""
        try:
            product_data = {}

            # Basic info extraction
            try:
                # Find product link
                link_elements = product_element.find_elements(By.CSS_SELECTOR, "a[href]")
                if link_elements:
                    product_url = link_elements[0].get_attribute("href")
                    product_data['url'] = product_url

                    # Extract product ID
                    product_id_match = re.search(r'i(\d+)', product_url)
                    if product_id_match:
                        product_data['product_id'] = product_id_match.group(1)
                    else:
                        product_data['product_id'] = str(uuid.uuid4())
                else:
                    return None

            except Exception as e:
                logger.debug(f"URL extraction failed: {e}")
                return None

            # Product name
            try:
                name_selectors = [
                    "[title]",
                    "a[title]",
                    "h3",
                    ".title",
                    "[data-qa-locator='product-item'] a"
                ]

                product_name = "Unknown Product"
                for selector in name_selectors:
                    try:
                        name_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                        for name_elem in name_elements:
                            title = name_elem.get_attribute("title") or name_elem.text
                            if title and title.strip():
                                product_name = title.strip()
                                break
                        if product_name != "Unknown Product":
                            break
                    except:
                        continue

                product_data['name'] = product_name

            except Exception as e:
                product_data['name'] = "Unknown Product"

            # Price extraction with JavaScript
            price = 0.0

            # First try: Look within this product element
            try:
                price_selectors = [
                    ".currency-value",
                    ".price-current",
                    ".price .currency",
                    "[data-qa-locator='product-price']"
                ]

                for selector in price_selectors:
                    try:
                        price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                        for price_elem in price_elements:
                            price_text = price_elem.text.strip()
                            if price_text:
                                numbers = re.findall(r'[0-9,\.]+', price_text)
                                if numbers:
                                    price_str = numbers[0].replace(',', '').replace('.', '')
                                    if price_str.isdigit() and len(price_str) >= 4:
                                        price = float(price_str)
                                        break
                        if price > 0:
                            break
                    except:
                        continue

            except Exception as e:
                logger.debug(f"Direct price extraction failed: {e}")

            # Second try: JavaScript extraction
            if price == 0.0:
                try:
                    # Scroll element into view
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", product_element)
                    time.sleep(0.5)

                    price = self.extract_price_with_javascript(product_element)
                except Exception as e:
                    logger.debug(f"JS price extraction failed: {e}")

            product_data['price'] = price

            # Basic metadata
            product_data.update({
                'brand': None,
                'category': 'Electronics',
                'rating': None,
                'images': [],
                'crawl_timestamp': datetime.now(),
                'platform': 'lazada_vn'
            })

            # Update statistics
            self.stats['total_products'] += 1
            if price > 0:
                self.stats['products_with_prices'] += 1

            return product_data

        except Exception as e:
            logger.error(f"Product extraction error: {e}")
            return None

    def crawl_category_with_js(self, category_url: str, category_name: str, max_pages: int = 2) -> List[Dict[str, Any]]:
        """Crawl category with JavaScript enabled"""
        products = []
        logger.info(f"Starting JS crawl: {category_name}")

        try:
            for page in range(1, max_pages + 1):
                try:
                    # Construct URL
                    page_url = f"{category_url}?page={page}" if '?' not in category_url else f"{category_url}&page={page}"

                    logger.info(f"Processing page {page}/{max_pages}: {page_url}")

                    # Navigate to page
                    self.driver.get(page_url)

                    # Wait for dynamic content
                    self.wait_for_dynamic_content()

                    # Scroll to load lazy content
                    self.scroll_to_load_content()

                    # Find products with multiple selectors
                    product_selectors = [
                        "[data-qa-locator='product-item']",
                        ".product-item",
                        "[data-spm*='product']",
                        ".gridItem",
                        "[data-automation-id*='product']"
                    ]

                    product_elements = []
                    for selector in product_selectors:
                        try:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if elements:
                                product_elements = elements
                                logger.info(f"Found {len(elements)} products using selector: {selector}")
                                break
                        except Exception as e:
                            logger.debug(f"Selector {selector} failed: {e}")

                    if not product_elements:
                        logger.warning(f"No products found on page {page}")

                        # Debug: Save page source
                        debug_file = self.output_dir / f"debug_page_{page}.html"
                        with open(debug_file, 'w', encoding='utf-8') as f:
                            f.write(self.driver.page_source)
                        logger.info(f"Page source saved for debugging: {debug_file}")

                        continue

                    # Extract products
                    page_products = []
                    for i, element in enumerate(product_elements):
                        try:
                            product_data = self.extract_product_with_js(element)
                            if product_data:
                                product_data['category'] = category_name
                                page_products.append(product_data)

                            # Small delay between extractions
                            if i > 0 and i % 5 == 0:
                                time.sleep(random.uniform(1, 2))

                        except Exception as e:
                            logger.debug(f"Product {i+1} extraction error: {e}")

                    products.extend(page_products)
                    self.stats['pages_processed'] += 1

                    logger.info(f"Page {page} completed: {len(page_products)} products, {sum(1 for p in page_products if p.get('price', 0) > 0)} with prices")

                    # Delay between pages
                    time.sleep(random.uniform(*self.config['human_delays']))

                except Exception as e:
                    logger.error(f"Page {page} processing error: {e}")

            logger.info(f"Category {category_name} completed: {len(products)} total products")

        except Exception as e:
            logger.error(f"Category crawl error: {e}")

        return products

    def run_js_crawl(self, categories: List[str] = None, max_pages_per_category: int = 2) -> Dict[str, Any]:
        """Run JavaScript-enabled crawl"""
        logger.info("Starting JavaScript-Enabled Crawler")

        try:
            # Setup driver
            if not self.setup_javascript_driver():
                raise Exception("Failed to setup JavaScript driver")

            # Categories
            electronics_categories = {
                'smartphones': {
                    'url': 'https://www.lazada.vn/dien-thoai-di-dong/',
                    'name': 'Điện Thoại Di Động'
                }
            }

            categories_to_crawl = categories or ['smartphones']
            all_products = []

            for category_key in categories_to_crawl:
                if category_key not in electronics_categories:
                    continue

                category_info = electronics_categories[category_key]
                category_products = self.crawl_category_with_js(
                    category_info['url'],
                    category_info['name'],
                    max_pages_per_category
                )

                all_products.extend(category_products)

            # Save results
            if all_products:
                self.save_js_results(all_products)

            # Calculate success rates
            price_success_rate = (self.stats['products_with_prices'] / max(self.stats['total_products'], 1)) * 100

            return {
                'success': True,
                'products_count': len(all_products),
                'products_with_prices': self.stats['products_with_prices'],
                'price_success_rate': price_success_rate,
                'pages_processed': self.stats['pages_processed'],
                'js_executions': self.stats['javascript_executions'],
                'products': all_products[:5] if all_products else []
            }

        except Exception as e:
            logger.error(f"JS crawl failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }

        finally:
            if self.driver:
                self.driver.quit()

    def save_js_results(self, products: List[Dict[str, Any]]):
        """Save JavaScript crawl results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save JSON
        json_file = self.output_dir / f"js_lazada_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False, default=str)

        # Save CSV
        df = pd.DataFrame(products)
        csv_file = self.output_dir / f"js_lazada_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        logger.info(f"Results saved: {json_file}")

def main():
    """Test JavaScript crawler"""
    print("JavaScript-Enabled Lazada Crawler Test")
    print("=" * 50)

    crawler = JavaScriptEnabledCrawler(headless=True)  # Try headless first
    result = crawler.run_js_crawl(
        categories=['smartphones'],
        max_pages_per_category=1  # Start with 1 page
    )

    if result['success']:
        print(f"SUCCESS: {result['products_count']} products")
        print(f"Price extraction: {result['products_with_prices']} products ({result['price_success_rate']:.1f}%)")
        print(f"Pages processed: {result['pages_processed']}")
        print(f"JS executions: {result['js_executions']}")

        # Show samples
        for product in result['products']:
            price_str = f"{product['price']:,.0f} VND" if product.get('price', 0) > 0 else "NO PRICE"
            print(f"- {product['name'][:60]}... - {price_str}")
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()