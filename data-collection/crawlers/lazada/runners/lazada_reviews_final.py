#!/usr/bin/env python3
"""
Lazada Reviews Crawler - Final Version
Based on actual HTML structure from product_detail.html
"""
import json
import time
import random
import os
import re
import uuid
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
LOG_PREFIX = "[Lazada-Reviews]"

class LazadaReviewsCrawler:
    def __init__(self):
        # Use direct product URLs instead of category pages
        self.test_products = [
            "https://www.lazada.vn/products/i2974492628.html",  # Example product
        ]
        self.max_reviews_per_product = 10
        self.driver = None

    def setup_driver(self):
        """Setup Selenium driver"""
        try:
            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--headless=new")
            self.driver = uc.Chrome(options=options)
            self.driver.implicitly_wait(10)
            print(f"{LOG_PREFIX} Driver ready")
            return True
        except Exception as e:
            print(f"{LOG_PREFIX} Driver setup failed: {e}")
            return False

    def extract_reviews_from_product(self, product_url: str) -> List[Dict[str, Any]]:
        """Extract reviews using actual HTML structure"""
        reviews = []
        
        try:
            print(f"{LOG_PREFIX} Loading: {product_url}")
            self.driver.get(product_url)
            
            # Wait for page to load
            time.sleep(10)
            
            # Scroll to reviews section
            self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight * 0.7)')
            time.sleep(3)
            
            # Extract product info
            product_id = re.search(r'i(\d+)', product_url)
            product_id = product_id.group(1) if product_id else str(uuid.uuid4())
            
            try:
                product_name = self.driver.find_element(By.CSS_SELECTOR, 'h1').text
            except:
                product_name = "Unknown Product"
            
            # Find review items using actual structure: div.mod-reviews div.item
            review_items = self.driver.find_elements(By.CSS_SELECTOR, 'div.mod-reviews div.item')
            print(f"{LOG_PREFIX} Found {len(review_items)} review items")
            
            for i, item in enumerate(review_items[:self.max_reviews_per_product]):
                try:
                    # Extract reviewer name: span.reviewer
                    reviewer_name = "Anonymous"
                    try:
                        reviewer_elem = item.find_element(By.CSS_SELECTOR, 'span.reviewer')
                        reviewer_name = reviewer_elem.text.strip()
                    except:
                        pass
                    
                    # Extract review date: span.time
                    review_date = None
                    try:
                        time_elem = item.find_element(By.CSS_SELECTOR, 'span.time')
                        review_date = time_elem.text.strip()
                    except:
                        pass
                    
                    # Extract rating: count star images in div.container-star
                    rating = 0
                    try:
                        stars = item.find_elements(By.CSS_SELECTOR, 'div.container-star img.star')
                        rating = len(stars)
                    except:
                        pass
                    
                    # Extract review text: div.item-content-main-content-reviews-item span
                    review_text = ""
                    try:
                        review_elem = item.find_element(By.CSS_SELECTOR, 'div.item-content-main-content-reviews-item span')
                        review_text = review_elem.text.strip()
                    except:
                        pass
                    
                    # Extract SKU info (color, variant): div.skuInfo-item
                    sku_info = ""
                    try:
                        sku_elems = item.find_elements(By.CSS_SELECTOR, 'div.skuInfo-item')
                        sku_parts = []
                        for sku in sku_elems:
                            sku_parts.append(sku.text.strip())
                        sku_info = ", ".join(sku_parts)
                    except:
                        pass
                    
                    # Extract helpful count: span.item-content-like-content-text
                    helpful_count = 0
                    try:
                        helpful_elem = item.find_element(By.CSS_SELECTOR, 'span.item-content-like-content-text')
                        helpful_text = helpful_elem.text
                        match = re.search(r'Helpful\((\d+)\)', helpful_text)
                        if match:
                            helpful_count = int(match.group(1))
                    except:
                        pass
                    
                    if review_text:  # Only save if has review text
                        review_data = {
                            'review_id': f"lazada_rev_{uuid.uuid4().hex[:8]}",
                            'product_id': product_id,
                            'product_name': product_name,
                            'product_url': product_url,
                            'reviewer_name': reviewer_name,
                            'review_date': review_date,
                            'rating': rating,
                            'review_text': review_text[:500],
                            'sku_info': sku_info,
                            'helpful_count': helpful_count,
                            'crawl_timestamp': datetime.now().isoformat()
                        }
                        reviews.append(review_data)
                        print(f"{LOG_PREFIX}   Review {i+1}: {rating}★ - {review_text[:50]}...")
                
                except Exception as e:
                    print(f"{LOG_PREFIX} Failed to extract review {i+1}: {e}")
                    continue
            
            print(f"{LOG_PREFIX} Extracted {len(reviews)} reviews")
            
        except Exception as e:
            print(f"{LOG_PREFIX} Error: {e}")
        
        return reviews

    def crawl_from_product_urls(self, product_urls: List[str]) -> List[Dict[str, Any]]:
        """Crawl reviews from list of product URLs"""
        all_reviews = []
        
        for i, url in enumerate(product_urls, 1):
            print(f"{LOG_PREFIX} Product {i}/{len(product_urls)}")
            try:
                reviews = self.extract_reviews_from_product(url)
                all_reviews.extend(reviews)
                time.sleep(random.uniform(3, 5))
            except Exception as e:
                print(f"{LOG_PREFIX} Failed product {i}: {e}")
                continue
        
        return all_reviews

    def save_reviews(self, reviews: List[Dict[str, Any]], category: str = "products"):
        """Save reviews to JSONL file"""
        if not reviews:
            print(f"{LOG_PREFIX} No reviews to save")
            return
        
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
        
        today = datetime.now().strftime("%Y-%m-%d")
        date_dir = Path(output_dir) / "lazada_reviews" / f"date={today}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lazada_reviews_{category}_{timestamp}.jsonl"
        filepath = date_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for review in reviews:
                f.write(json.dumps(review, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(reviews)} reviews to {filepath}")

    def run(self, product_urls: List[str] = None):
        """Run the reviews crawler"""
        if not SELENIUM_AVAILABLE:
            print(f"{LOG_PREFIX} Selenium not available")
            return
        
        print(f"{LOG_PREFIX} Starting reviews crawler...")
        
        if not self.setup_driver():
            return
        
        try:
            urls = product_urls or self.test_products
            reviews = self.crawl_from_product_urls(urls)
            
            if reviews:
                self.save_reviews(reviews)
            
            print(f"{LOG_PREFIX} Completed! Total reviews: {len(reviews)}")
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

def main():
    # Test with sample product URLs
    test_urls = [
        "https://www.lazada.vn/products/i2974492628.html",
        # Add more product URLs here
    ]
    
    crawler = LazadaReviewsCrawler()
    crawler.run(test_urls)

if __name__ == "__main__":
    main()
