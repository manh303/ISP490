#!/usr/bin/env python3
"""
Enhanced Lazada Crawler with Reviews Extraction
Advanced crawler that extracts both product details and user reviews from Lazada
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
from selenium.webdriver.common.action_chains import ActionChains

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_lazada_reviews.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LazadaReview:
    """Data class for Lazada reviews"""

    def __init__(self, review_id: str, rating: float, review_text: str,
                 reviewer_name: str, review_date: str, helpful_count: int = 0):
        self.review_id = review_id
        self.rating = rating
        self.review_text = review_text
        self.reviewer_name = reviewer_name
        self.review_date = review_date
        self.helpful_count = helpful_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            'review_id': self.review_id,
            'rating': self.rating,
            'review_text': self.review_text,
            'reviewer_name': self.reviewer_name,
            'review_date': self.review_date,
            'helpful_count': self.helpful_count
        }

class EnhancedLazadaCrawler:
    """Enhanced Lazada crawler with product details and reviews extraction"""

    def __init__(self, headless: bool = True, output_dir: str = None, extract_reviews: bool = True):
        self.headless = headless
        self.extract_reviews = extract_reviews
        self.output_dir = Path(output_dir) if output_dir else Path("../data/enhanced_lazada")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.wait = None
        self.actions = None

        # Enhanced configuration for reviews
        self.config = {
            'page_load_timeout': 45,
            'element_timeout': 20,
            'scroll_pause': 3,
            'extraction_delay': (1, 3),
            'page_delay': (5, 10),
            'review_load_delay': (2, 4),
            'max_reviews_per_product': 10
        }

        # Enhanced selectors for reviews
        self.review_selectors = {
            'reviews_container': [
                '.reviews-container',
                '.product-reviews',
                '.review-list',
                '[data-qa-locator="reviews"]',
                '.ugc-review-list',
                '.review-section'
            ],
            'review_items': [
                '.review-item',
                '.single-review',
                '.ugc-review-item',
                '[data-qa-locator="review-item"]',
                '.review-content'
            ],
            'review_rating': [
                '.rating-stars',
                '.review-rating',
                '.stars-rating',
                '[data-qa-locator="review-rating"]',
                '.review-score'
            ],
            'review_text': [
                '.review-text',
                '.review-content-text',
                '.review-comment',
                '.review-body',
                '.ugc-review-text'
            ],
            'reviewer_name': [
                '.reviewer-name',
                '.review-author',
                '.user-name',
                '.reviewer',
                '.review-username'
            ],
            'review_date': [
                '.review-date',
                '.review-time',
                '.date-posted',
                '.review-timestamp'
            ],
            'helpful_count': [
                '.helpful-count',
                '.like-count',
                '.thumbs-up-count'
            ]
        }

        # Statistics tracking
        self.stats = {
            'start_time': datetime.now(),
            'products_found': 0,
            'products_with_prices': 0,
            'products_with_ratings': 0,
            'products_with_reviews': 0,
            'total_reviews_extracted': 0,
            'pages_processed': 0,
            'review_extraction_attempts': 0,
            'successful_review_extractions': 0
        }

        # Product detail extraction from the working crawler
        self.price_patterns = [
            r'₫\s*([\d,\.]+)',
            r'VND\s*([\d,\.]+)',
            r'"price":\s*"?(\d+)"?',
            r'"current_price":\s*"?(\d+)"?',
            r'(\d{4,9})'
        ]

        self.price_selectors = [
            '.pdp-price .currency-value',
            '.price-current .currency-value',
            '.currency-value',
            '.price .currency',
            '[data-qa-locator="product-price"] .currency-value',
            '.item-price .currency-value',
            '.selling-price'
        ]

    def setup_enhanced_driver(self) -> bool:
        """Setup enhanced Chrome driver with additional options for reviews"""
        try:
            logger.info("Setting up enhanced Chrome driver for reviews extraction...")

            options = Options()

            if self.headless:
                options.add_argument("--headless=new")

            # Essential options
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")

            # Enhanced user agent for better compatibility
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            # Performance and compatibility
            options.add_argument("--disable-extensions")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")
            options.add_argument("--disable-blink-features=AutomationControlled")

            # JavaScript execution
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(self.config['page_load_timeout'])
            self.wait = WebDriverWait(self.driver, self.config['element_timeout'])
            self.actions = ActionChains(self.driver)

            # Execute script to hide webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            logger.info("SUCCESS: Enhanced driver ready for reviews extraction")
            return True

        except Exception as e:
            logger.error(f"Enhanced driver setup failed: {e}")
            return False

    def extract_product_reviews(self, product_url: str) -> List[LazadaReview]:
        """Extract reviews from product detail page"""
        if not self.extract_reviews:
            return []

        self.stats['review_extraction_attempts'] += 1
        reviews = []

        try:
            logger.info(f"Extracting reviews from: {product_url}")

            # Navigate to product page
            current_url = self.driver.current_url
            self.driver.get(product_url)

            # Wait for page load
            time.sleep(random.uniform(*self.config['review_load_delay']))

            # Scroll down to load reviews section
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.6);")
            time.sleep(2)

            # Try to find and click reviews tab/section
            review_tab_selectors = [
                '[data-qa-locator="reviews-tab"]',
                'a[href*="review"]',
                '.review-tab',
                '.reviews-link',
                'button:contains("Đánh giá")',
                'button:contains("Reviews")'
            ]

            for selector in review_tab_selectors:
                try:
                    if ':contains(' not in selector:
                        tab_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        self.actions.move_to_element(tab_element).click().perform()
                        time.sleep(2)
                        break
                except:
                    continue

            # Find reviews container
            reviews_container = None
            for container_selector in self.review_selectors['reviews_container']:
                try:
                    reviews_container = self.driver.find_element(By.CSS_SELECTOR, container_selector)
                    break
                except:
                    continue

            if not reviews_container:
                # Try JavaScript method to find reviews
                js_script = """
                var reviewElements = document.querySelectorAll('[class*="review"], [class*="ugc"], [data-qa*="review"]');
                return reviewElements.length > 0 ? reviewElements[0] : null;
                """
                reviews_container = self.driver.execute_script(js_script)

            if reviews_container:
                # Find individual review items
                review_items = []
                for item_selector in self.review_selectors['review_items']:
                    try:
                        items = reviews_container.find_elements(By.CSS_SELECTOR, item_selector)
                        if items:
                            review_items = items
                            break
                    except:
                        continue

                if not review_items:
                    # Fallback: find any element with review-like content
                    js_script = """
                    var container = arguments[0];
                    var allDivs = container.querySelectorAll('div');
                    var reviewDivs = [];
                    for (var div of allDivs) {
                        var text = div.textContent || div.innerText;
                        if (text && text.length > 20 && text.length < 1000) {
                            reviewDivs.push(div);
                        }
                    }
                    return reviewDivs.slice(0, 10); // Max 10 reviews
                    """
                    review_items = self.driver.execute_script(js_script, reviews_container)

                # Extract review data
                for i, item in enumerate(review_items[:self.config['max_reviews_per_product']]):
                    try:
                        review_data = self.extract_single_review(item, i)
                        if review_data:
                            reviews.append(review_data)
                    except Exception as e:
                        logger.debug(f"Failed to extract review {i}: {e}")

                if reviews:
                    self.stats['successful_review_extractions'] += 1
                    self.stats['total_reviews_extracted'] += len(reviews)
                    logger.info(f"Extracted {len(reviews)} reviews from product")

            # Return to original page if needed
            if current_url and current_url != product_url:
                self.driver.get(current_url)
                time.sleep(1)

        except Exception as e:
            logger.error(f"Review extraction error for {product_url}: {e}")

        return reviews

    def extract_single_review(self, review_element, index: int) -> Optional[LazadaReview]:
        """Extract data from a single review element"""
        try:
            # Extract rating
            rating = None
            for rating_selector in self.review_selectors['review_rating']:
                try:
                    rating_elem = review_element.find_element(By.CSS_SELECTOR, rating_selector)
                    rating_text = rating_elem.text or rating_elem.get_attribute('title') or rating_elem.get_attribute('data-rating')

                    if rating_text:
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                            break
                except:
                    continue

            # If no rating found, try data attributes
            if rating is None:
                try:
                    rating_attrs = ['data-rating', 'data-score', 'data-stars']
                    for attr in rating_attrs:
                        attr_value = review_element.get_attribute(attr)
                        if attr_value:
                            rating = float(attr_value)
                            break
                except:
                    pass

            # Extract review text
            review_text = ""
            for text_selector in self.review_selectors['review_text']:
                try:
                    text_elem = review_element.find_element(By.CSS_SELECTOR, text_selector)
                    review_text = text_elem.text.strip()
                    if review_text and len(review_text) > 5:
                        break
                except:
                    continue

            # Fallback: get any meaningful text from the element
            if not review_text:
                try:
                    all_text = review_element.text.strip()
                    if all_text and len(all_text) > 10:
                        # Clean the text (remove extra whitespace, etc.)
                        review_text = re.sub(r'\s+', ' ', all_text)[:500]  # Limit length
                except:
                    pass

            # Extract reviewer name
            reviewer_name = "Anonymous"
            for name_selector in self.review_selectors['reviewer_name']:
                try:
                    name_elem = review_element.find_element(By.CSS_SELECTOR, name_selector)
                    name_text = name_elem.text.strip()
                    if name_text and len(name_text) > 0:
                        reviewer_name = name_text
                        break
                except:
                    continue

            # Extract review date
            review_date = None
            for date_selector in self.review_selectors['review_date']:
                try:
                    date_elem = review_element.find_element(By.CSS_SELECTOR, date_selector)
                    date_text = date_elem.text.strip()
                    if date_text:
                        review_date = date_text
                        break
                except:
                    continue

            # Extract helpful count
            helpful_count = 0
            for helpful_selector in self.review_selectors['helpful_count']:
                try:
                    helpful_elem = review_element.find_element(By.CSS_SELECTOR, helpful_selector)
                    helpful_text = helpful_elem.text.strip()
                    helpful_numbers = re.findall(r'\d+', helpful_text)
                    if helpful_numbers:
                        helpful_count = int(helpful_numbers[0])
                        break
                except:
                    continue

            # Create review object if we have minimum required data
            if review_text and len(review_text) > 5:
                review_id = f"lazada_review_{uuid.uuid4().hex[:8]}"
                return LazadaReview(
                    review_id=review_id,
                    rating=rating or 0.0,
                    review_text=review_text,
                    reviewer_name=reviewer_name,
                    review_date=review_date or "Unknown",
                    helpful_count=helpful_count
                )

            return None

        except Exception as e:
            logger.debug(f"Single review extraction error: {e}")
            return None

    def extract_price_comprehensive(self, product_element, product_url: str = None) -> float:
        """Comprehensive price extraction (reused from production crawler)"""
        try:
            # Method 1: Direct element text extraction
            for selector in self.price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = elem.text.strip()
                        if price_text:
                            for pattern in self.price_patterns:
                                matches = re.findall(pattern, price_text)
                                if matches:
                                    for match in matches:
                                        clean_price = re.sub(r'[,\.\s]', '', str(match))
                                        if clean_price.isdigit() and 1000 <= int(clean_price) <= 100000000:
                                            return float(clean_price)
                except Exception:
                    continue

            # Method 2: Data attribute extraction
            price_attrs = ['data-price', 'data-currency-value', 'data-original-price']
            for attr in price_attrs:
                try:
                    elements = product_element.find_elements(By.CSS_SELECTOR, f"[{attr}]")
                    for elem in elements:
                        attr_value = elem.get_attribute(attr)
                        if attr_value and attr_value.isdigit():
                            price_val = int(attr_value)
                            if 1000 <= price_val <= 100000000:
                                return float(price_val)
                except Exception:
                    pass

            return 0.0

        except Exception:
            return 0.0

    def extract_enhanced_product_data(self, product_element) -> Optional[Dict[str, Any]]:
        """Extract comprehensive product data with reviews"""
        try:
            product_data = {}

            # URL and ID
            try:
                link_elements = product_element.find_elements(By.CSS_SELECTOR, "a[href]")
                if link_elements:
                    product_url = link_elements[0].get_attribute("href")
                    product_data['url'] = product_url

                    # Extract ID
                    id_match = re.search(r'i(\d+)', product_url)
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

            # Basic rating (from listing page)
            try:
                rating_selectors = [".rating-average", ".review-score", ".stars", "[data-qa-locator='product-rating']"]
                rating = None

                for selector in rating_selectors:
                    try:
                        rating_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        rating_text = rating_elem.text.strip()
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                            break
                    except:
                        continue

                product_data['listing_rating'] = rating
                if rating:
                    self.stats['products_with_ratings'] += 1

            except Exception:
                product_data['listing_rating'] = None

            # Extract detailed reviews (if enabled)
            reviews = []
            if self.extract_reviews and product_data.get('url'):
                try:
                    reviews = self.extract_product_reviews(product_data['url'])
                    if reviews:
                        self.stats['products_with_reviews'] += 1
                except Exception as e:
                    logger.debug(f"Reviews extraction failed for {product_data.get('name', 'Unknown')}: {e}")

            product_data['reviews'] = [review.to_dict() for review in reviews]
            product_data['review_count'] = len(reviews)

            # Images
            try:
                img_elements = product_element.find_elements(By.CSS_SELECTOR, "img[src]")
                images = []
                for img in img_elements:
                    src = img.get_attribute("src")
                    if src and ('lazcdn' in src or 'alicdn' in src):
                        images.append(src)
                        break
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
            logger.error(f"Enhanced product data extraction error: {e}")
            return None

    def crawl_category_with_reviews(self, category_url: str, category_name: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        """Crawl category with enhanced product and reviews extraction"""
        products = []
        logger.info(f"Starting enhanced crawl with reviews: {category_name} ({max_pages} pages)")

        try:
            for page in range(1, max_pages + 1):
                try:
                    page_url = f"{category_url}?page={page}" if '?' not in category_url else f"{category_url}&page={page}"
                    logger.info(f"Processing page {page}/{max_pages}")

                    self.driver.get(page_url)

                    # Wait and scroll
                    time.sleep(random.uniform(*self.config['review_load_delay']))
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
                    time.sleep(2)

                    # Find products
                    product_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")

                    if not product_elements:
                        logger.warning(f"No products found on page {page}")
                        continue

                    logger.info(f"Found {len(product_elements)} products on page {page}")

                    # Extract products with reviews
                    page_products = []
                    for i, element in enumerate(product_elements[:5]):  # Limit to 5 products per page for reviews
                        try:
                            product_data = self.extract_enhanced_product_data(element)
                            if product_data:
                                product_data['category'] = category_name
                                page_products.append(product_data)

                                logger.info(f"Product {i+1}: {product_data['name'][:50]}... - "
                                          f"Price: {product_data['price']} - Reviews: {product_data['review_count']}")

                            # Delay between products to avoid overwhelming the server
                            time.sleep(random.uniform(*self.config['extraction_delay']))

                        except Exception as e:
                            logger.debug(f"Product {i+1} extraction failed: {e}")

                    products.extend(page_products)
                    self.stats['pages_processed'] += 1

                    reviews_count = sum(len(p.get('reviews', [])) for p in page_products)
                    logger.info(f"Page {page} completed: {len(page_products)} products, {reviews_count} total reviews")

                    # Longer delay between pages
                    time.sleep(random.uniform(*self.config['page_delay']))

                except Exception as e:
                    logger.error(f"Page {page} processing failed: {e}")

            logger.info(f"Category {category_name} completed: {len(products)} total products")

        except Exception as e:
            logger.error(f"Category crawl failed: {e}")

        return products

    def run_enhanced_crawl(self, max_pages_per_category: int = 2) -> Dict[str, Any]:
        """Run enhanced crawl with reviews extraction"""
        logger.info("Starting Enhanced Lazada Crawler with Reviews")

        try:
            if not self.setup_enhanced_driver():
                raise Exception("Enhanced driver setup failed")

            # Test with smartphones category
            category_url = "https://www.lazada.vn/dien-thoai-di-dong/"
            category_name = "Điện Thoại Di Động"

            all_products = self.crawl_category_with_reviews(
                category_url, category_name, max_pages_per_category
            )

            # Save results
            if all_products:
                self.save_enhanced_results(all_products)

            # Calculate metrics
            duration = datetime.now() - self.stats['start_time']
            review_success_rate = (self.stats['products_with_reviews'] / max(self.stats['products_found'], 1)) * 100

            return {
                'success': True,
                'summary': {
                    'products_collected': len(all_products),
                    'products_with_prices': self.stats['products_with_prices'],
                    'products_with_reviews': self.stats['products_with_reviews'],
                    'total_reviews_extracted': self.stats['total_reviews_extracted'],
                    'review_success_rate': review_success_rate,
                    'pages_processed': self.stats['pages_processed'],
                    'duration': str(duration)
                },
                'products': all_products
            }

        except Exception as e:
            logger.error(f"Enhanced crawl failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'partial_stats': self.stats
            }

        finally:
            if self.driver:
                self.driver.quit()

    def save_enhanced_results(self, products: List[Dict[str, Any]]):
        """Save enhanced results with reviews"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Enhanced results with reviews metadata
        results = {
            'metadata': {
                'crawl_timestamp': datetime.now().isoformat(),
                'crawler_version': 'enhanced_with_reviews_v1.0',
                'total_products': len(products),
                'products_with_reviews': sum(1 for p in products if p.get('reviews')),
                'total_reviews': sum(len(p.get('reviews', [])) for p in products),
                'reviews_extraction_enabled': self.extract_reviews,
                'statistics': self.stats
            },
            'products': products
        }

        # Save comprehensive JSON
        json_file = self.output_dir / f"enhanced_lazada_with_reviews_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)

        # Save products CSV
        df_products = pd.DataFrame(products)
        csv_file = self.output_dir / f"enhanced_products_{timestamp}.csv"
        df_products.to_csv(csv_file, index=False, encoding='utf-8')

        # Extract and save reviews separately
        all_reviews = []
        for product in products:
            product_id = product.get('product_id', 'unknown')
            product_name = product.get('name', 'Unknown')
            for review in product.get('reviews', []):
                review_with_product = review.copy()
                review_with_product['product_id'] = product_id
                review_with_product['product_name'] = product_name
                all_reviews.append(review_with_product)

        if all_reviews:
            df_reviews = pd.DataFrame(all_reviews)
            reviews_csv = self.output_dir / f"extracted_reviews_{timestamp}.csv"
            df_reviews.to_csv(reviews_csv, index=False, encoding='utf-8')

            reviews_json = self.output_dir / f"extracted_reviews_{timestamp}.json"
            with open(reviews_json, 'w', encoding='utf-8') as f:
                json.dump(all_reviews, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Enhanced results saved: {json_file}")
        logger.info(f"Products CSV: {csv_file}")
        if all_reviews:
            logger.info(f"Reviews CSV: {reviews_csv}")

def main():
    """Enhanced crawler test with reviews"""
    print("Enhanced Lazada Crawler with Reviews Extraction")
    print("=" * 60)

    # Create crawler with reviews enabled
    crawler = EnhancedLazadaCrawler(
        headless=False,  # Set to False for debugging
        extract_reviews=True
    )

    result = crawler.run_enhanced_crawl(max_pages_per_category=2)

    if result['success']:
        summary = result['summary']
        print("SUCCESS: Enhanced crawl with reviews completed!")
        print(f"Products collected: {summary['products_collected']}")
        print(f"Products with prices: {summary['products_with_prices']}")
        print(f"Products with reviews: {summary['products_with_reviews']}")
        print(f"Total reviews extracted: {summary['total_reviews_extracted']}")
        print(f"Review success rate: {summary['review_success_rate']:.1f}%")
        print(f"Duration: {summary['duration']}")

        # Show samples
        products_with_reviews = [p for p in result['products'] if p.get('reviews')]
        if products_with_reviews:
            print("\nSample Product with Reviews:")
            sample = products_with_reviews[0]
            print(f"Name: {sample['name'][:60]}...")
            print(f"Price: {sample['price']:,.0f} VND")
            print(f"Reviews count: {len(sample['reviews'])}")
            if sample['reviews']:
                print(f"Sample review: {sample['reviews'][0]['review_text'][:100]}...")
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()