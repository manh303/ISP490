#!/usr/bin/env python3
"""
Lazada Reviews Crawler - Extract reviews from products data
Uses undetected_chromedriver to bypass anti-bot detection

Runs AFTER products crawler and uses product URLs from products data
"""
import json
import time
import random
import os
import re
import uuid
import sys
import platform
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("[WARNING] undetected_chromedriver not available. Install with: pip install undetected-chromedriver")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lazada_reviews.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _get_project_root() -> Path:
    """Get the project root directory (ecommerce-dss-project)"""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent.parent.parent


def _get_lazada_data_dir() -> Path:
    """Get the data directory at project root"""
    return _get_project_root() / "data"


def _is_windows() -> bool:
    return platform.system() == "Windows"


def _is_docker() -> bool:
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup", "r") as f:
            return "docker" in f.read()
    except:
        return False


# Set paths based on environment
if _is_windows() or (not _is_docker()):
    _PROJECT_ROOT = _get_project_root()
    _LAZADA_DATA_DIR = _get_lazada_data_dir()
    
    _OUTPUT_BASE = _LAZADA_DATA_DIR / "outputs"
    _PROFILE_BASE = _LAZADA_DATA_DIR / ".profiles" / "lazada"
    _CHECKPOINT_BASE = _LAZADA_DATA_DIR / ".checkpoints"
    
    _OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    _PROFILE_BASE.mkdir(parents=True, exist_ok=True)
    _CHECKPOINT_BASE.mkdir(parents=True, exist_ok=True)
    
    OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", str(_OUTPUT_BASE))
    COOKIE_FILE = os.environ.get("LAZADA_COOKIE_FILE", str(_PROFILE_BASE / "lazada_cookies.json"))
    CHECKPOINT_FILE = str(_CHECKPOINT_BASE / "lazada_reviews_checkpoint.json")
    
    logger.info(f"Running on {'Windows' if _is_windows() else 'Local'} environment")
    logger.info(f"OUTPUT_DIR: {OUTPUT_DIR}")
else:
    OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
    COOKIE_FILE = os.environ.get("LAZADA_COOKIE_FILE", "/app/data/.profiles/lazada/lazada_cookies.json")
    CHECKPOINT_FILE = "/tmp/crawler_checkpoints/lazada_reviews_checkpoint.json"
    logger.info("Running inside Docker container")

LOG_PREFIX = "[Lazada-Reviews]"


def load_checkpoint() -> Dict:
    """Load crawling checkpoint"""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                logger.info(f"Loaded checkpoint: {checkpoint}")
                return checkpoint
    except Exception as e:
        logger.warning(f"Could not load checkpoint: {e}")
    return {"last_product_idx": 0, "total_reviews_saved": 0}


def save_checkpoint(product_idx: int, total_reviews: int):
    """Save crawling checkpoint"""
    checkpoint = {"last_product_idx": product_idx, "total_reviews_saved": total_reviews}
    try:
        os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)
    except Exception as e:
        logger.warning(f"Could not save checkpoint: {e}")


def load_product_urls_from_today() -> List[Dict[str, str]]:
    """Load product URLs from today's products data"""
    today = "2025-12-10"  # Hardcoded for testing - change to datetime.now().strftime("%Y-%m-%d")
    products_dir = Path(OUTPUT_DIR) / "lazada" / f"date={today}"
    
    products = []
    if products_dir.exists():
        for jsonl_file in products_dir.glob("*.jsonl"):
            logger.info(f"Reading: {jsonl_file.name}")
            try:
                with open(jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            product = json.loads(line.strip())
                            if product.get("url") and product.get("product_name"):
                                products.append({
                                    "url": product["url"],
                                    "name": product["product_name"],
                                    "product_id": product.get("product_id", "")
                                })
                        except:
                            continue
            except Exception as e:
                logger.error(f"Error reading {jsonl_file}: {e}")
    
    logger.info(f"Loaded {len(products)} product URLs from today's data")
    return products


def save_reviews_batch(reviews: List[Dict], batch_num: int):
    """Save reviews batch to JSONL file"""
    if not reviews:
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path(OUTPUT_DIR) / "lazada_reviews" / f"date={today}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%H%M%S")
    filename = f"reviews_batch_{batch_num}_{timestamp}.jsonl"
    filepath = output_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for review in reviews:
            f.write(json.dumps(review, ensure_ascii=False, default=str) + '\n')
    
    logger.info(f"Saved {len(reviews)} reviews to {filepath}")


class LazadaReviewsCrawler:
    """Lazada Reviews Crawler using undetected_chromedriver"""
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.driver = None
        self.wait = None
        
        # Configuration - increased timeouts for manual anti-bot handling
        self.config = {
            'page_load_timeout': 120,
            'element_timeout': 60,
            'scroll_pause': 2,
            'review_delay': (5, 8),
            'max_reviews_per_product': 20
        }
        
        # Review selectors from working crawler
        self.review_selectors = [
            '.mod-reviews .item',
            '[data-qa-locator="review-item"]',
            '.pdp-review-list .pdp-review-item',
            '.review-list .review-item',
            '[class*="review-item"]',
            '[class*="comment-item"]'
        ]
    
    def setup_driver(self) -> bool:
        """Setup undetected chromedriver"""
        try:
            logger.info("Setting up undetected ChromeDriver...")
            
            options = uc.ChromeOptions()
            if self.headless:
                options.add_argument("--headless=new")
            
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            
            # Specify Chrome version to avoid mismatch errors
            # Change this if your Chrome version is different
            self.driver = uc.Chrome(options=options, version_main=142)
            self.wait = WebDriverWait(self.driver, self.config['element_timeout'])
            
            logger.info("SUCCESS: Undetected ChromeDriver ready")
            return True
            
        except Exception as e:
            logger.error(f"Driver setup failed: {e}")
            return False
    
    def safe_text_extract(self, element, attribute=None) -> str:
        """Safely extract text from element"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text
            return text.strip() if text else ""
        except:
            return ""
    
    def check_and_handle_antibot(self, target_url: str) -> bool:
        """Check for anti-bot and prompt user to solve it"""
        current_url = self.driver.current_url
        
        if "punish" in current_url or "captcha" in current_url or "verify" in current_url or "x5sec" in current_url:
            logger.warning("="*60)
            logger.warning("⚠️  ANTI-BOT DETECTED!")
            logger.warning("Please solve the CAPTCHA/verification in the browser window.")
            logger.warning("After solving, press ENTER in this terminal to continue...")
            logger.warning("="*60)
            input("\n>>> Press ENTER after solving anti-bot... ")
            time.sleep(2)
            
            # Retry navigation
            self.driver.get(target_url)
            time.sleep(3)
            
            # Check again
            current_url = self.driver.current_url
            if "punish" in current_url or "captcha" in current_url or "verify" in current_url:
                logger.warning("Anti-bot still detected after retry")
                return False
            
            logger.info("✓ Anti-bot passed! Continuing...")
        
        return True
    
    def extract_reviews_from_product(self, product_url: str, product_name: str, product_id: str) -> List[Dict]:
        """Extract reviews from a product page"""
        reviews = []
        
        try:
            logger.info(f"Extracting reviews from: {product_name[:50]}...")
            
            # Navigate to product page
            self.driver.get(product_url)
            time.sleep(5)  # Wait for page load
            
            # Check for anti-bot
            if not self.check_and_handle_antibot(product_url):
                return reviews
            
            # Scroll to load reviews section
            for scroll_pct in [0.3, 0.5, 0.7, 0.9, 1.0]:
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct});")
                time.sleep(2)
            
            # Try to click Reviews tab
            try:
                reviews_tab = self.driver.find_elements(By.CSS_SELECTOR, 
                    'a[href="#reviews"], [data-spm="reviews"], .pdp-tab-reviews, [data-tab="reviews"]')
                if reviews_tab:
                    reviews_tab[0].click()
                    time.sleep(2)
                    logger.info("Clicked Reviews tab")
            except:
                pass
            
            # Find review elements
            review_elements = []
            for selector in self.review_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        review_elements = elements[:self.config['max_reviews_per_product']]
                        logger.info(f"Found {len(review_elements)} review elements with selector: {selector}")
                        break
                except:
                    continue
            
            # Extract review data
            for i, elem in enumerate(review_elements):
                try:
                    review_text = self.safe_text_extract(elem)
                    
                    if review_text and len(review_text) > 20:
                        # Try to extract rating
                        rating = None
                        try:
                            stars = elem.find_elements(By.CSS_SELECTOR, '[class*="star"]')
                            if stars:
                                filled_count = sum(1 for s in stars if 'full' in s.get_attribute('class').lower() or 'active' in s.get_attribute('class').lower())
                                if filled_count > 0:
                                    rating = float(filled_count)
                        except:
                            pass
                        
                        # Try to extract reviewer name
                        reviewer_name = "Anonymous"
                        try:
                            name_elem = elem.find_elements(By.CSS_SELECTOR, '[class*="name"], [class*="user"], [class*="author"]')
                            if name_elem:
                                name = self.safe_text_extract(name_elem[0])
                                if name and len(name) < 50:
                                    reviewer_name = name
                        except:
                            pass
                        
                        # Try to extract date
                        review_date = None
                        try:
                            date_elem = elem.find_elements(By.CSS_SELECTOR, '[class*="date"], [class*="time"], time')
                            if date_elem:
                                review_date = self.safe_text_extract(date_elem[0])
                        except:
                            pass
                        
                        review_data = {
                            "review_id": f"lazada_{product_id}_{uuid.uuid4().hex[:8]}",
                            "product_id": product_id,
                            "product_name": product_name,
                            "product_url": product_url,
                            "rating": rating,
                            "review_text": review_text[:1000],
                            "reviewer_name": reviewer_name,
                            "review_date": review_date,
                            "helpful_count": 0,
                            "crawl_timestamp": datetime.now().isoformat(),
                            "platform": "lazada_vn"
                        }
                        
                        reviews.append(review_data)
                
                except Exception as e:
                    logger.debug(f"Single review extraction failed: {e}")
            
            if reviews:
                logger.info(f"✓ Extracted {len(reviews)} reviews")
            else:
                logger.info(f"No reviews found")
            
        except Exception as e:
            logger.error(f"Review extraction error: {e}")
        
        return reviews
    
    def crawl_reviews(self, products: List[Dict], max_products: int = 10000) -> tuple:
        """Main crawling function"""
        if not SELENIUM_AVAILABLE:
            logger.error("Selenium/undetected_chromedriver not available")
            return [], False
        
        # Load checkpoint
        checkpoint = load_checkpoint()
        start_idx = checkpoint["last_product_idx"]
        total_reviews_saved = checkpoint["total_reviews_saved"]
        
        all_reviews = []
        batch_reviews = []
        batch_num = 1
        BATCH_SIZE = 20
        max_idx = min(len(products), max_products)
        hit_antibot = False
        antibot_count = 0
        
        # Reset if checkpoint outdated
        if start_idx >= max_idx:
            logger.info(f"Checkpoint outdated, resetting...")
            start_idx = 0
            total_reviews_saved = 0
            save_checkpoint(0, 0)
        
        logger.info(f"Resuming from product {start_idx}/{max_idx}")
        logger.info(f"Batch saving every {BATCH_SIZE} reviews")
        
        try:
            if not self.setup_driver():
                return [], True
            
            # Warm-up: visit homepage first
            logger.info("Warming up: visiting Lazada homepage...")
            self.driver.get("https://www.lazada.vn/")
            time.sleep(3)
            
            if not self.check_and_handle_antibot("https://www.lazada.vn/"):
                logger.error("Anti-bot on homepage, stopping")
                return [], True
            
            # Process products
            for idx in range(start_idx, max_idx):
                product = products[idx]
                product_url = product["url"]
                product_name = product["name"]
                product_id = product.get("product_id", "")
                
                logger.info(f"\nProduct {idx+1}/{max_idx}")
                
                try:
                    reviews = self.extract_reviews_from_product(product_url, product_name, product_id)
                    
                    if reviews:
                        all_reviews.extend(reviews)
                        batch_reviews.extend(reviews)
                        antibot_count = 0  # Reset on success
                    
                    # Check for anti-bot in URL
                    current_url = self.driver.current_url
                    if "punish" in current_url or "x5sec" in current_url:
                        antibot_count += 1
                        if antibot_count >= 3:
                            logger.warning(f"⚠️  Anti-bot threshold reached ({antibot_count}). Stopping.")
                            hit_antibot = True
                            break
                    
                    # Save batch
                    if len(batch_reviews) >= BATCH_SIZE:
                        save_reviews_batch(batch_reviews, batch_num)
                        total_reviews_saved += len(batch_reviews)
                        batch_num += 1
                        batch_reviews = []
                    
                    # Save checkpoint
                    save_checkpoint(idx + 1, total_reviews_saved)
                    
                    # Random delay between products
                    delay = random.uniform(3, 6)
                    time.sleep(delay)
                    
                except KeyboardInterrupt:
                    logger.info("Interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Error processing product: {e}")
                    antibot_count += 1
            
            # Save remaining reviews
            if batch_reviews:
                save_reviews_batch(batch_reviews, batch_num)
                total_reviews_saved += len(batch_reviews)
            
        except Exception as e:
            logger.error(f"Crawling error: {e}")
        
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Crawling completed!")
        logger.info(f"Total reviews extracted: {len(all_reviews)}")
        logger.info(f"Total reviews saved: {total_reviews_saved}")
        logger.info(f"{'='*60}")
        
        return all_reviews, hit_antibot


def main():
    """Main entry point"""
    print(f"{LOG_PREFIX} Starting Reviews Crawler (undetected_chromedriver)")
    print("="*60)
    
    # Load products
    products = load_product_urls_from_today()
    
    if not products:
        print(f"{LOG_PREFIX} No products found to crawl reviews")
        return
    
    print(f"{LOG_PREFIX} Found {len(products)} products to crawl reviews")
    
    # Create crawler (headed mode for manual anti-bot)
    crawler = LazadaReviewsCrawler(headless=False)
    
    # Run crawler
    reviews, hit_antibot = crawler.crawl_reviews(products)
    
    if hit_antibot:
        print(f"{LOG_PREFIX} ⚠️  Stopped due to anti-bot detection")
    
    print(f"{LOG_PREFIX} Done! Extracted {len(reviews)} reviews")


if __name__ == "__main__":
    main()
