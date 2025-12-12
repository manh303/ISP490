#!/usr/bin/env python3
"""
Working Lazada Reviews Crawler
Production-ready crawler that extracts products and reviews based on real Lazada structure
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
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('working_lazada_reviews.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WorkingLazadaReviewsCrawler:
    """Working Lazada crawler based on real structure analysis"""

    def __init__(self, headless: bool = False, output_dir: str = None, extract_reviews: bool = True):
        self.headless = headless
        self.extract_reviews = extract_reviews
        self.output_dir = Path(output_dir) if output_dir else Path("../data/working_lazada_reviews")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.wait = None
        self.actions = None

        # Working configuration based on tests
        # Increased timeouts to allow manual anti-bot clicking
        self.config = {
            'page_load_timeout': 120,      # Increased from 30
            'element_timeout': 60,          # Increased from 15
            'scroll_pause': 2,
            'extraction_delay': (2, 4),
            'page_delay': (5, 8),
            'review_delay': (3, 5),
            'max_reviews_per_product': 10   # Increased for more reviews
        }

        # Real Lazada selectors discovered from structure analysis
        self.selectors = {
            'product_items': '[data-qa-locator="product-item"]',
            'product_links': 'a[href*="/products/"]',
            'product_title': 'a[title]',
            'price': '.ooOxS',  # Real price class
            'rating_stars': '._9-ogB',  # Rating star elements
            'rating_count': '.qzqFw',  # Rating count in parentheses
            'sold_count': '._1cEkb',  # Sold count
            'location': '.oa6ri'  # Seller location
        }

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'products_found': 0,
            'products_with_prices': 0,
            'products_with_reviews': 0,
            'total_reviews_extracted': 0,
            'pages_processed': 0
        }

    def setup_working_driver(self) -> bool:
        """Setup working driver based on successful debug test"""
        try:
            logger.info("Setting up working driver for Lazada...")

            # Use undetected chromedriver as it worked in debug test
            try:
                options = uc.ChromeOptions()
                if self.headless:
                    options.add_argument("--headless=new")

                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--window-size=1920,1080")

                self.driver = uc.Chrome(options=options)
                self.wait = WebDriverWait(self.driver, self.config['element_timeout'])
                self.actions = ActionChains(self.driver)

                logger.info("SUCCESS: Undetected ChromeDriver ready")
                return True

            except Exception as e:
                logger.warning(f"Undetected ChromeDriver failed: {e}")

                # Fallback to regular ChromeDriver with stealth options
                options = Options()
                if self.headless:
                    options.add_argument("--headless=new")

                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

                self.driver = webdriver.Chrome(options=options)
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.wait = WebDriverWait(self.driver, self.config['element_timeout'])
                self.actions = ActionChains(self.driver)

                logger.info("SUCCESS: Regular ChromeDriver with stealth options ready")
                return True

        except Exception as e:
            logger.error(f"Driver setup failed: {e}")
            return False

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text with Unicode handling"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text

            if text:
                return text.strip()
            return ""
        except:
            return ""

    def extract_price_from_element(self, product_element) -> float:
        """Extract price using real Lazada price class"""
        try:
            # Use the real price selector discovered
            price_elements = product_element.find_elements(By.CSS_SELECTOR, self.selectors['price'])

            for price_elem in price_elements:
                price_text = self.safe_text_extract(price_elem)
                if price_text and '₫' in price_text:
                    # Remove currency symbol and formatting
                    clean_price = re.sub(r'[^\d]', '', price_text)
                    if clean_price.isdigit() and len(clean_price) >= 4:
                        price = int(clean_price)
                        if 1000 <= price <= 100000000:  # Reasonable price range
                            return float(price)

            return 0.0

        except Exception as e:
            logger.debug(f"Price extraction error: {e}")
            return 0.0

    def extract_rating_from_element(self, product_element) -> Dict[str, Any]:
        """Extract rating information using real Lazada rating classes"""
        try:
            rating_info = {'average': None, 'count': None}

            # Extract rating count (number in parentheses)
            try:
                rating_count_elem = product_element.find_element(By.CSS_SELECTOR, self.selectors['rating_count'])
                count_text = self.safe_text_extract(rating_count_elem)
                if count_text:
                    # Extract number from parentheses like "(33)"
                    count_match = re.search(r'\((\d+)\)', count_text)
                    if count_match:
                        rating_info['count'] = int(count_match.group(1))
            except:
                pass

            # Extract star rating by counting filled stars
            try:
                star_elements = product_element.find_elements(By.CSS_SELECTOR, self.selectors['rating_stars'])
                if star_elements:
                    # Count filled stars (assuming filled stars have different class/style)
                    filled_stars = 0
                    for star in star_elements:
                        star_class = star.get_attribute('class')
                        # Check if star is filled (contains "Dy1nx" in the real structure)
                        if 'Dy1nx' in star_class:
                            filled_stars += 1

                    if filled_stars > 0:
                        rating_info['average'] = float(filled_stars)
            except:
                pass

            return rating_info

        except Exception as e:
            logger.debug(f"Rating extraction error: {e}")
            return {'average': None, 'count': None}

    def extract_basic_product_info(self, product_element) -> Optional[Dict[str, Any]]:
        """Extract basic product information from listing page"""
        try:
            product_data = {}

            # Product URL and ID
            try:
                link_elem = product_element.find_element(By.CSS_SELECTOR, self.selectors['product_links'])
                product_url = link_elem.get_attribute('href')

                # Handle relative URLs
                if product_url.startswith('//'):
                    product_url = 'https:' + product_url
                elif product_url.startswith('/'):
                    product_url = 'https://www.lazada.vn' + product_url

                product_data['url'] = product_url

                # Extract product ID
                id_match = re.search(r'i(\d+)', product_url)
                product_data['id'] = id_match.group(1) if id_match else str(uuid.uuid4())

            except:
                return None

            # Product name
            try:
                title_elem = product_element.find_element(By.CSS_SELECTOR, self.selectors['product_title'])
                product_name = self.safe_text_extract(title_elem, 'title')
                if not product_name:
                    product_name = self.safe_text_extract(title_elem)

                product_data['name'] = product_name if product_name else "Unknown Product"
            except:
                product_data['name'] = "Unknown Product"

            # Price
            price = self.extract_price_from_element(product_element)
            product_data['price'] = price
            if price > 0:
                self.stats['products_with_prices'] += 1

            # Rating information
            rating_info = self.extract_rating_from_element(product_element)
            product_data['listing_rating'] = rating_info['average']
            product_data['listing_rating_count'] = rating_info['count']

            # Additional info
            try:
                # Sold count
                sold_elem = product_element.find_element(By.CSS_SELECTOR, self.selectors['sold_count'])
                sold_text = self.safe_text_extract(sold_elem)
                sold_match = re.search(r'([\d.K]+)\s*Đã bán', sold_text)
                if sold_match:
                    sold_str = sold_match.group(1)
                    if 'K' in sold_str:
                        sold_count = int(float(sold_str.replace('K', '')) * 1000)
                    else:
                        sold_count = int(sold_str.replace('.', ''))
                    product_data['sold_count'] = sold_count
                else:
                    product_data['sold_count'] = 0
            except:
                product_data['sold_count'] = 0

            try:
                # Location
                location_elem = product_element.find_element(By.CSS_SELECTOR, self.selectors['location'])
                product_data['location'] = self.safe_text_extract(location_elem)
            except:
                product_data['location'] = None

            # Metadata
            product_data.update({
                'platform': 'lazada_vn',
                'category': 'Electronics',
                'crawl_timestamp': datetime.now(),
                'reviews': []
            })

            self.stats['products_found'] += 1
            return product_data

        except Exception as e:
            logger.debug(f"Product extraction error: {e}")
            return None

    def extract_product_reviews(self, product_url: str, product_name: str) -> List[Dict[str, Any]]:
        """Extract reviews from product detail page"""
        if not self.extract_reviews:
            return []

        reviews = []

        try:
            logger.info(f"Extracting reviews from: {product_name[:50]}...")

            # Navigate to product page
            current_url = self.driver.current_url
            self.driver.get(product_url)
            
            # Wait longer for product page to load fully
            time.sleep(5)  # Initial wait for page load
            
            # Scroll down to potentially load reviews section
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
            time.sleep(3)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.8);")
            time.sleep(3)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Wait for lazy-loaded reviews

            # Look for reviews section - try multiple approaches
            review_elements = []

            # Method 1: Look for common review selectors
            review_selectors = [
                '[class*="review"]',
                '[data-qa*="review"]',
                '.ugc-review-item',
                '.review-item',
                '.comment',
                '[class*="feedback"]'
            ]

            for selector in review_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        review_elements.extend(elements[:self.config['max_reviews_per_product']])
                        break
                except:
                    continue

            # Method 2: JavaScript approach to find review-like content
            if not review_elements:
                try:
                    js_script = """
                    var allDivs = document.querySelectorAll('div');
                    var reviewDivs = [];
                    for (var div of allDivs) {
                        var text = div.textContent || div.innerText || '';
                        // Look for Vietnamese review-like content
                        if (text.length > 20 && text.length < 500 &&
                            (text.includes('tốt') || text.includes('đẹp') || text.includes('chất lượng') ||
                             text.includes('sản phẩm') || text.includes('giao hàng') || text.includes('shop'))) {
                            // Avoid duplicate parent-child elements
                            var isChild = false;
                            for (var existing of reviewDivs) {
                                if (existing.contains(div) || div.contains(existing)) {
                                    isChild = true;
                                    break;
                                }
                            }
                            if (!isChild) {
                                reviewDivs.push(div);
                            }
                        }
                    }
                    return reviewDivs.slice(0, 5); // Max 5 reviews
                    """
                    review_elements = self.driver.execute_script(js_script) or []
                except:
                    pass

            # Extract review data
            for i, review_elem in enumerate(review_elements):
                try:
                    review_text = self.safe_text_extract(review_elem)

                    if review_text and len(review_text.strip()) > 10:
                        # Try to extract rating for this review (may not always be available)
                        review_rating = None
                        try:
                            rating_elements = review_elem.find_elements(By.CSS_SELECTOR, '[class*="star"], [class*="rating"]')
                            if rating_elements:
                                # Try to determine rating from stars or text
                                rating_text = self.safe_text_extract(rating_elements[0])
                                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                                if rating_match:
                                    review_rating = float(rating_match.group(1))
                        except:
                            pass

                        # Try to extract reviewer name
                        reviewer_name = "Anonymous"
                        try:
                            name_elements = review_elem.find_elements(By.CSS_SELECTOR, '[class*="name"], [class*="user"], [class*="author"]')
                            if name_elements:
                                name_text = self.safe_text_extract(name_elements[0])
                                if name_text and len(name_text) < 50:  # Reasonable name length
                                    reviewer_name = name_text
                        except:
                            pass

                        review_data = {
                            'review_id': f"lazada_review_{uuid.uuid4().hex[:8]}",
                            'rating': review_rating,
                            'review_text': review_text.strip()[:500],  # Limit length
                            'reviewer_name': reviewer_name,
                            'review_date': None,  # Date extraction is complex, skip for now
                            'helpful_count': 0
                        }

                        reviews.append(review_data)

                except Exception as e:
                    logger.debug(f"Single review extraction failed: {e}")

            if reviews:
                self.stats['products_with_reviews'] += 1
                self.stats['total_reviews_extracted'] += len(reviews)
                logger.info(f"Extracted {len(reviews)} reviews")

            # Return to listing page
            if current_url and current_url != product_url:
                self.driver.get(current_url)
                time.sleep(1)

        except Exception as e:
            logger.debug(f"Review extraction error: {e}")

        return reviews

    def crawl_lazada_category(self, category_url: str, max_pages: int = 2) -> List[Dict[str, Any]]:
        """Crawl Lazada category with reviews"""
        all_products = []

        try:
            for page in range(1, max_pages + 1):
                logger.info(f"Processing page {page}/{max_pages}")

                # Build page URL
                page_url = f"{category_url}?page={page}" if '?' not in category_url else f"{category_url}&page={page}"

                # Navigate to page
                self.driver.get(page_url)
                time.sleep(random.uniform(*self.config['extraction_delay']))

                # Check for anti-bot redirect
                current_url = self.driver.current_url
                if "punish" in current_url or "captcha" in current_url or "verify" in current_url:
                    logger.warning("="*60)
                    logger.warning("⚠️  ANTI-BOT DETECTED!")
                    logger.warning("Please solve the CAPTCHA/verification in the browser window.")
                    logger.warning("After solving, press ENTER in this terminal to continue...")
                    logger.warning("="*60)
                    input("\n>>> Press ENTER after solving anti-bot... ")
                    time.sleep(2)
                    self.driver.get(page_url)  # Retry after user clicks
                    time.sleep(3)
                    current_url = self.driver.current_url
                    if "punish" in current_url or "captcha" in current_url or "verify" in current_url:
                        logger.warning("Anti-bot still detected after retry, skipping page")
                        continue
                    logger.info("✓ Anti-bot passed! Continuing...")

                # Scroll to load products
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
                time.sleep(self.config['scroll_pause'])

                # Find product elements using working selector
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, self.selectors['product_items'])

                if not product_elements:
                    logger.warning(f"No products found on page {page}")
                    continue

                logger.info(f"Found {len(product_elements)} products on page {page}")
                
                # Wait a bit after finding products to let page stabilize
                time.sleep(3)

                # Extract products (limit to first 10 for efficiency when extracting reviews)
                page_products = []
                for i, element in enumerate(product_elements[:10]):
                    try:
                        product_data = self.extract_basic_product_info(element)

                        if product_data:
                            # Extract reviews if enabled
                            if self.extract_reviews and product_data.get('url'):
                                try:
                                    reviews = self.extract_product_reviews(
                                        product_data['url'],
                                        product_data['name']
                                    )
                                    product_data['reviews'] = reviews
                                    product_data['review_count'] = len(reviews)
                                except Exception as e:
                                    logger.debug(f"Reviews extraction failed: {e}")
                                    product_data['reviews'] = []
                                    product_data['review_count'] = 0
                            else:
                                product_data['reviews'] = []
                                product_data['review_count'] = 0

                            page_products.append(product_data)

                            logger.info(f"Product {i+1}: {product_data['name'][:40]}... - "
                                      f"Price: {product_data['price']:,.0f} VND - "
                                      f"Reviews: {product_data['review_count']}")

                        # Delay between products
                        if i > 0 and i % 5 == 0:
                            time.sleep(random.uniform(*self.config['extraction_delay']))

                    except Exception as e:
                        logger.debug(f"Product {i+1} extraction failed: {e}")

                all_products.extend(page_products)
                self.stats['pages_processed'] += 1

                # Delay between pages
                time.sleep(random.uniform(*self.config['page_delay']))

        except Exception as e:
            logger.error(f"Category crawl error: {e}")

        return all_products

    def run_working_crawl(self, max_pages: int = 2, category_url: str = None) -> Dict[str, Any]:
        """Run the working crawler"""
        logger.info("Starting Working Lazada Reviews Crawler")
        logger.info("="*60)
        logger.info("If anti-bot appears, you'll be prompted to solve it manually.")
        logger.info("The browser will stay open until you press ENTER.")
        logger.info("="*60)

        try:
            if not self.setup_working_driver():
                raise Exception("Driver setup failed")

            # Allow custom category or default to smartphones
            if not category_url:
                category_url = "https://www.lazada.vn/dien-thoai-di-dong/"
            
            logger.info(f"Crawling category: {category_url}")
            products = self.crawl_lazada_category(category_url, max_pages)

            # Save results
            if products:
                self.save_results(products)

            # Calculate metrics
            duration = datetime.now() - self.stats['start_time']

            return {
                'success': True,
                'summary': {
                    'products_collected': len(products),
                    'products_with_prices': self.stats['products_with_prices'],
                    'products_with_reviews': self.stats['products_with_reviews'],
                    'total_reviews_extracted': self.stats['total_reviews_extracted'],
                    'pages_processed': self.stats['pages_processed'],
                    'duration': str(duration)
                },
                'products': products
            }

        except Exception as e:
            logger.error(f"Working crawl failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'partial_stats': self.stats
            }

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

    def save_results(self, products: List[Dict[str, Any]]):
        """Save crawl results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Comprehensive results
        results = {
            'metadata': {
                'crawl_timestamp': datetime.now().isoformat(),
                'crawler_version': 'working_reviews_v1.0',
                'total_products': len(products),
                'products_with_prices': sum(1 for p in products if p.get('price', 0) > 0),
                'products_with_reviews': sum(1 for p in products if p.get('reviews')),
                'total_reviews': sum(len(p.get('reviews', [])) for p in products),
                'statistics': self.stats
            },
            'products': products
        }

        # Save JSON
        json_file = self.output_dir / f"working_lazada_reviews_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)

        # Save products CSV
        df_products = pd.DataFrame(products)
        csv_file = self.output_dir / f"products_{timestamp}.csv"
        df_products.to_csv(csv_file, index=False, encoding='utf-8')

        # Extract and save reviews separately
        all_reviews = []
        for product in products:
            for review in product.get('reviews', []):
                review_with_product = review.copy()
                review_with_product['product_id'] = product.get('id')
                review_with_product['product_name'] = product.get('name')
                all_reviews.append(review_with_product)

        if all_reviews:
            reviews_file = self.output_dir / f"reviews_{timestamp}.csv"
            df_reviews = pd.DataFrame(all_reviews)
            df_reviews.to_csv(reviews_file, index=False, encoding='utf-8')
            logger.info(f"Reviews saved: {reviews_file}")

        logger.info(f"Results saved: {json_file}")

def main():
    """Main execution"""
    print("Working Lazada Reviews Crawler")
    print("=" * 50)

    # Create crawler
    crawler = WorkingLazadaReviewsCrawler(
        headless=False,  # Set to True for production
        extract_reviews=True
    )

    # Run crawl
    result = crawler.run_working_crawl(max_pages=2)

    if result['success']:
        summary = result['summary']
        print("SUCCESS: Working crawl completed!")
        print(f"Products collected: {summary['products_collected']}")
        print(f"Products with prices: {summary['products_with_prices']}")
        print(f"Products with reviews: {summary['products_with_reviews']}")
        print(f"Total reviews extracted: {summary['total_reviews_extracted']}")
        print(f"Duration: {summary['duration']}")

        # Show samples
        if result['products']:
            print("\nSample Products:")
            for i, product in enumerate(result['products'][:3], 1):
                print(f"{i}. {product['name'][:60]}...")
                print(f"   Price: {product['price']:,.0f} VND")
                print(f"   Rating: {product.get('listing_rating', 'N/A')}")
                print(f"   Reviews: {len(product.get('reviews', []))}")
                if product.get('reviews'):
                    print(f"   Sample review: {product['reviews'][0]['review_text'][:80]}...")
                print()
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()