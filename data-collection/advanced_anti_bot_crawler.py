#!/usr/bin/env python3
"""
Advanced Anti-Bot Lazada Crawler with Price Extraction
Chống phát hiện bot và thu thập price data động
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
from urllib.parse import urljoin

# Selenium imports with advanced features
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service

# Enhanced imports for stealth
import undetected_chromedriver as uc

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('advanced_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdvancedAntiDetectionCrawler:
    """Advanced crawler with comprehensive anti-detection and price extraction"""

    def __init__(self, headless: bool = True, output_dir: str = None):
        self.headless = headless
        self.output_dir = Path(output_dir) if output_dir else Path("../data/advanced_lazada_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Driver configuration
        self.driver = None
        self.wait = None

        # Enhanced anti-detection configuration
        self.anti_detection_config = {
            'user_agents': [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
            ],
            'viewports': [
                (1920, 1080), (1366, 768), (1536, 864), (1440, 900), (1280, 720)
            ],
            'delays': {
                'page_load': (3, 8),
                'between_actions': (1, 3),
                'between_pages': (5, 12),
                'scroll_delay': (0.5, 1.5)
            }
        }

        # Price extraction selectors (comprehensive)
        self.price_selectors = [
            # Main price selectors
            ".pdp-price .pdp-price_type_normal",
            ".pdp-price .currency-value",
            "[data-qa-locator='product-price'] .currency-value",
            ".price-current .currency-value",
            ".product-price .currency-value",

            # Alternative selectors
            ".price .currency",
            ".price-box .price",
            ".item-price .currency-value",
            "[data-spm-anchor-id*='price']",

            # Listing page selectors
            ".currency-value",
            ".price-current",
            "[data-qa-locator='product-price']",
            ".price .currency"
        ]

        # Statistics tracking
        self.stats = {
            'start_time': datetime.now(),
            'total_products_found': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'products_with_prices': 0,
            'pages_processed': 0,
            'categories_processed': 0,
            'bot_detections': 0,
            'errors': []
        }

        # Categories with better anti-detection
        self.electronics_categories = {
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

    def setup_undetected_driver(self) -> bool:
        """Setup undetected Chrome driver with advanced stealth"""
        try:
            logger.info("Setting up undetected Chrome driver...")

            # Chrome options for stealth
            options = uc.ChromeOptions()

            if self.headless:
                options.add_argument("--headless=new")  # New headless mode

            # Enhanced stealth arguments
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-features=VizDisplayCompositor")

            # Random viewport
            viewport = random.choice(self.anti_detection_config['viewports'])
            options.add_argument(f"--window-size={viewport[0]},{viewport[1]}")

            # Random user agent
            user_agent = random.choice(self.anti_detection_config['user_agents'])
            options.add_argument(f"--user-agent={user_agent}")

            # Additional stealth arguments
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins-discovery")
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--disable-backgrounding-occluded-windows")

            # Create undetected Chrome driver
            self.driver = uc.Chrome(options=options, version_main=None)

            # Execute stealth scripts
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            self.driver.execute_script("""
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
            """)

            self.driver.execute_script("""
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['vi-VN', 'vi', 'en-US', 'en']
                });
            """)

            # Set random screen resolution
            self.driver.execute_script(f"""
                Object.defineProperty(screen, 'width', {{
                    get: () => {viewport[0]}
                }});
                Object.defineProperty(screen, 'height', {{
                    get: () => {viewport[1]}
                }});
            """)

            # Setup wait
            self.wait = WebDriverWait(self.driver, 15)

            logger.info("SUCCESS: Undetected Chrome driver initialized")
            return True

        except Exception as e:
            logger.error(f"FAILED: Driver setup error - {e}")
            return False

    def human_like_delay(self, delay_type: str = 'between_actions'):
        """Human-like random delays"""
        if delay_type in self.anti_detection_config['delays']:
            min_delay, max_delay = self.anti_detection_config['delays'][delay_type]
            delay = random.uniform(min_delay, max_delay)
            logger.debug(f"Human delay ({delay_type}): {delay:.2f}s")
            time.sleep(delay)

    def human_like_scroll(self):
        """Human-like scrolling behavior"""
        try:
            # Get page height
            page_height = self.driver.execute_script("return document.body.scrollHeight")

            # Scroll in random chunks
            current_position = 0
            while current_position < page_height:
                # Random scroll distance
                scroll_distance = random.randint(200, 500)
                current_position += scroll_distance

                self.driver.execute_script(f"window.scrollTo(0, {current_position});")

                # Human-like pause
                self.human_like_delay('scroll_delay')

                # Sometimes scroll back a bit (human behavior)
                if random.random() < 0.3:
                    back_scroll = random.randint(50, 150)
                    self.driver.execute_script(f"window.scrollTo(0, {current_position - back_scroll});")
                    time.sleep(0.5)
                    self.driver.execute_script(f"window.scrollTo(0, {current_position});")

            # Scroll back to top gradually
            for i in range(5):
                scroll_position = page_height - (i * page_height // 5)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                time.sleep(0.3)

        except Exception as e:
            logger.warning(f"Scroll error: {e}")

    def extract_price_with_js(self, element=None) -> float:
        """Extract price using JavaScript execution"""
        try:
            # Try different price extraction methods
            price_scripts = [
                # Method 1: Query all possible price selectors
                """
                var priceSelectors = [
                    '.pdp-price .currency-value',
                    '.price-current .currency-value',
                    '.currency-value',
                    '.price .currency',
                    '[data-qa-locator="product-price"] .currency-value'
                ];

                for (var selector of priceSelectors) {
                    var priceElement = document.querySelector(selector);
                    if (priceElement && priceElement.textContent) {
                        return priceElement.textContent.trim();
                    }
                }
                return null;
                """,

                # Method 2: Check for price in data attributes
                """
                var priceElements = document.querySelectorAll('[data-price], [data-spm-anchor-id*="price"]');
                for (var element of priceElements) {
                    if (element.dataset.price) return element.dataset.price;
                    if (element.textContent.match(/[0-9,\.]+/)) return element.textContent.trim();
                }
                return null;
                """,

                # Method 3: Look for any element containing price pattern
                """
                var allElements = document.querySelectorAll('*');
                for (var element of allElements) {
                    if (element.textContent && element.textContent.match(/^[0-9,\.]+$/)) {
                        var text = element.textContent.trim();
                        if (text.length > 3 && text.length < 15) {
                            return text;
                        }
                    }
                }
                return null;
                """
            ]

            for script in price_scripts:
                try:
                    price_text = self.driver.execute_script(script)
                    if price_text:
                        # Extract numeric value
                        price_numbers = re.findall(r'[\d,\.]+', str(price_text))
                        if price_numbers:
                            price_str = price_numbers[0].replace(',', '').replace('.', '')
                            if len(price_str) >= 3:  # Reasonable price length
                                return float(price_str)
                except Exception as e:
                    logger.debug(f"Price script failed: {e}")
                    continue

            return 0.0

        except Exception as e:
            logger.warning(f"JS price extraction error: {e}")
            return 0.0

    def wait_for_page_load(self):
        """Wait for page to fully load with JavaScript"""
        try:
            # Wait for basic page load
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")

            # Wait a bit more for dynamic content
            time.sleep(2)

            # Wait for any AJAX requests to complete
            self.wait.until(lambda driver: driver.execute_script("return jQuery.active == 0") if driver.execute_script("return typeof jQuery !== 'undefined'") else True)

        except Exception as e:
            logger.debug(f"Page load wait warning: {e}")

    def enhanced_product_extraction(self, product_element) -> Optional[Dict[str, Any]]:
        """Enhanced product extraction with better price detection"""
        try:
            product_data = {}

            # Basic product info
            try:
                link_element = product_element.find_element(By.CSS_SELECTOR, "a[href]")
                product_url = link_element.get_attribute("href")
                product_data['url'] = product_url

                # Extract product ID
                url_parts = product_url.split('/')
                for part in url_parts:
                    if part.startswith('i') and part[1:].isdigit():
                        product_data['product_id'] = part[1:]
                        break
                else:
                    product_data['product_id'] = str(uuid.uuid4())

            except Exception as e:
                logger.debug(f"URL extraction error: {e}")
                return None

            # Product name
            try:
                name_selectors = [
                    "[data-qa-locator='product-item'] [title]",
                    ".title-wrapper a",
                    ".product-title",
                    "h3 a",
                    "[data-qa-locator='product-item'] a[title]",
                    "a[title]"
                ]

                for selector in name_selectors:
                    try:
                        name_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        product_name = name_element.get_attribute("title") or name_element.text
                        if product_name and product_name.strip():
                            product_data['name'] = product_name.strip()
                            break
                    except NoSuchElementException:
                        continue
                else:
                    product_data['name'] = "Unknown Product"

            except Exception as e:
                product_data['name'] = "Unknown Product"

            # Enhanced price extraction
            price = 0.0
            try:
                # Method 1: Direct element extraction
                for selector in self.price_selectors:
                    try:
                        price_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        price_text = price_element.text.strip()

                        if price_text:
                            # Extract numbers
                            price_numbers = re.findall(r'[\d,\.]+', price_text)
                            if price_numbers:
                                price_str = price_numbers[0].replace(',', '').replace('.', '')
                                if len(price_str) >= 3:  # Reasonable price
                                    price = float(price_str)
                                    break
                    except Exception:
                        continue

                # Method 2: JavaScript extraction if direct method failed
                if price == 0.0:
                    # Scroll to element to trigger price loading
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", product_element)
                        time.sleep(0.5)
                        price = self.extract_price_with_js(product_element)
                    except Exception:
                        pass

                product_data['price'] = price

            except Exception as e:
                logger.debug(f"Price extraction error: {e}")
                product_data['price'] = 0.0

            # Rating and reviews
            try:
                rating_selectors = [
                    ".rating-average",
                    ".review-score",
                    "[data-qa-locator='product-rating']",
                    ".rating",
                    ".stars"
                ]

                for selector in rating_selectors:
                    try:
                        rating_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        rating_text = rating_element.text.strip()
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        if rating_match:
                            product_data['rating'] = float(rating_match.group(1))
                            break
                    except Exception:
                        continue
                else:
                    product_data['rating'] = None

            except Exception:
                product_data['rating'] = None

            # Image
            try:
                img_selectors = ["img[src]", ".product-image img", "[data-qa-locator='product-image'] img"]
                for selector in img_selectors:
                    try:
                        img_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        img_src = img_element.get_attribute("src")
                        if img_src:
                            product_data['images'] = [img_src]
                            break
                    except Exception:
                        continue
                else:
                    product_data['images'] = []
            except Exception:
                product_data['images'] = []

            # Set defaults
            product_data.update({
                'brand': None,
                'seller_name': None,
                'description': None,
                'availability': True,
                'category': 'Electronics',
                'crawl_timestamp': datetime.now(),
                'platform': 'lazada_vn'
            })

            # Track price success
            if product_data['price'] > 0:
                self.stats['products_with_prices'] += 1

            return product_data

        except Exception as e:
            logger.error(f"Product extraction error: {e}")
            return None

    def check_for_bot_detection(self) -> bool:
        """Check if we've been detected as a bot"""
        try:
            # Common bot detection indicators
            bot_indicators = [
                "captcha",
                "robot",
                "blocked",
                "access denied",
                "verify you are human",
                "cloudflare",
                "rate limit"
            ]

            page_text = self.driver.page_source.lower()
            for indicator in bot_indicators:
                if indicator in page_text:
                    logger.warning(f"Bot detection possible: {indicator}")
                    self.stats['bot_detections'] += 1
                    return True

            return False

        except Exception:
            return False

    def crawl_category_with_anti_detection(self, category_url: str, category_name: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        """Crawl category with advanced anti-detection"""
        products = []

        logger.info(f"Starting anti-detection crawl: {category_name}")

        try:
            for page in range(1, max_pages + 1):
                try:
                    # Construct page URL
                    if '?' in category_url:
                        page_url = f"{category_url}&page={page}"
                    else:
                        page_url = f"{category_url}?page={page}"

                    logger.info(f"Processing page {page}/{max_pages}: {page_url}")

                    # Navigate with random delay
                    self.driver.get(page_url)
                    self.wait_for_page_load()

                    # Check for bot detection
                    if self.check_for_bot_detection():
                        logger.warning(f"Bot detection on page {page}, adding extra delay...")
                        time.sleep(random.uniform(10, 20))
                        continue

                    # Human-like scrolling to load content
                    self.human_like_scroll()

                    # Wait for products to load
                    try:
                        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-qa-locator='product-item']")))
                    except TimeoutException:
                        logger.warning(f"Timeout waiting for products on page {page}")
                        self.human_like_delay('page_load')
                        continue

                    # Find products
                    product_selectors = [
                        "[data-qa-locator='product-item']",
                        ".product-item",
                        ".item-product"
                    ]

                    product_elements = []
                    for selector in product_selectors:
                        try:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if elements:
                                product_elements = elements
                                logger.info(f"Found {len(elements)} products")
                                break
                        except Exception:
                            continue

                    if not product_elements:
                        logger.warning(f"No products found on page {page}")
                        continue

                    # Extract products with human-like behavior
                    page_products = []
                    for i, element in enumerate(product_elements):
                        try:
                            # Random micro-delay between extractions
                            if i > 0 and random.random() < 0.3:
                                time.sleep(random.uniform(0.1, 0.5))

                            product_data = self.enhanced_product_extraction(element)
                            if product_data:
                                product_data['category'] = category_name
                                page_products.append(product_data)
                                self.stats['successful_extractions'] += 1
                            else:
                                self.stats['failed_extractions'] += 1

                        except Exception as e:
                            logger.debug(f"Product {i+1} extraction error: {e}")
                            self.stats['failed_extractions'] += 1

                    products.extend(page_products)
                    self.stats['pages_processed'] += 1
                    self.stats['total_products_found'] += len(page_products)

                    logger.info(f"Page {page} completed: {len(page_products)} products, {sum(1 for p in page_products if p.get('price', 0) > 0)} with prices")

                    # Human-like delay between pages
                    self.human_like_delay('between_pages')

                except Exception as e:
                    logger.error(f"Page {page} error: {e}")
                    self.stats['errors'].append(f"Page {page}: {str(e)}")
                    continue

            self.stats['categories_processed'] += 1
            logger.info(f"Category {category_name} completed: {len(products)} total products")

        except Exception as e:
            logger.error(f"Category crawl error: {e}")
            self.stats['errors'].append(f"Category {category_name}: {str(e)}")

        return products

    def run_advanced_crawl(self, categories: List[str] = None, max_pages_per_category: int = 2) -> Dict[str, Any]:
        """Run advanced anti-detection crawl"""
        logger.info("Starting Advanced Anti-Detection Crawler")

        try:
            # Setup driver
            if not self.setup_undetected_driver():
                raise Exception("Failed to setup undetected driver")

            # Default categories
            categories_to_crawl = categories or ['smartphones']
            all_products = []

            for category_key in categories_to_crawl:
                if category_key not in self.electronics_categories:
                    logger.warning(f"Unknown category: {category_key}")
                    continue

                category_info = self.electronics_categories[category_key]
                category_products = self.crawl_category_with_anti_detection(
                    category_info['url'],
                    category_info['name'],
                    max_pages_per_category
                )

                all_products.extend(category_products)

                # Long delay between categories for stealth
                if len(categories_to_crawl) > 1:
                    delay = random.uniform(15, 30)
                    logger.info(f"Inter-category delay: {delay:.1f}s")
                    time.sleep(delay)

            # Save products
            if all_products:
                self.save_products_enhanced(all_products)
                logger.info(f"SUCCESS: Collected {len(all_products)} products, {self.stats['products_with_prices']} with prices")
            else:
                logger.warning("No products collected")

            # Generate report
            report = self.generate_advanced_report()

            return {
                'success': True,
                'products_count': len(all_products),
                'products_with_prices': self.stats['products_with_prices'],
                'price_success_rate': (self.stats['products_with_prices'] / max(len(all_products), 1)) * 100,
                'report': report,
                'products': all_products[:5] if all_products else []
            }

        except Exception as e:
            logger.error(f"Advanced crawl failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'report': self.generate_advanced_report()
            }

        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Driver closed")

    def save_products_enhanced(self, products: List[Dict[str, Any]]):
        """Save products with enhanced reporting"""
        if not products:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save JSON
        json_file = self.output_dir / f"advanced_lazada_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False, default=str)

        # Save CSV
        df = pd.DataFrame(products)
        csv_file = self.output_dir / f"advanced_lazada_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        logger.info(f"Saved to: {json_file} and {csv_file}")

    def generate_advanced_report(self) -> Dict[str, Any]:
        """Generate advanced crawl report"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']

        return {
            'crawl_session': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'duration_formatted': str(duration)
            },
            'performance': {
                'categories_processed': self.stats['categories_processed'],
                'pages_processed': self.stats['pages_processed'],
                'total_products_found': self.stats['total_products_found'],
                'successful_extractions': self.stats['successful_extractions'],
                'failed_extractions': self.stats['failed_extractions'],
                'products_with_prices': self.stats['products_with_prices'],
                'price_extraction_rate': (self.stats['products_with_prices'] / max(self.stats['successful_extractions'], 1)) * 100,
                'success_rate': (self.stats['successful_extractions'] / max(self.stats['total_products_found'], 1)) * 100
            },
            'anti_detection': {
                'bot_detections': self.stats['bot_detections'],
                'stealth_effectiveness': 'High' if self.stats['bot_detections'] == 0 else 'Medium'
            },
            'errors': self.stats['errors']
        }

def main():
    """Main function for testing"""
    print("Advanced Anti-Detection Lazada Crawler")
    print("=" * 50)

    # Initialize crawler
    crawler = AdvancedAntiDetectionCrawler(headless=True)

    # Run test crawl
    result = crawler.run_advanced_crawl(
        categories=['smartphones'],
        max_pages_per_category=3
    )

    if result['success']:
        print(f"SUCCESS: {result['products_count']} products collected")
        print(f"Price extraction: {result['products_with_prices']} products ({result['price_success_rate']:.1f}%)")
        print(f"Duration: {result['report']['crawl_session']['duration_formatted']}")
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()