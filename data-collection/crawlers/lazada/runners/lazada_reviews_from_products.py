#!/usr/bin/env python3
"""
Lazada Reviews Crawler - Extract reviews from products data
Runs AFTER products crawler and uses product URLs from products data
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
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
COOKIE_FILE = "/tmp/profiles/lazada/cookies.json"
CHECKPOINT_FILE = os.environ.get("CRAWLER_CHECKPOINT_DIR", "/tmp/crawler_checkpoints") + "/lazada_reviews_checkpoint.json"
LOG_PREFIX = "[Lazada-Reviews]"

def load_checkpoint():
    """Load checkpoint to resume from last product"""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                print(f"{LOG_PREFIX} Loaded checkpoint: {checkpoint}")
                return checkpoint
    except Exception as e:
        print(f"{LOG_PREFIX} Error loading checkpoint: {e}")
    return {"last_product_idx": 0, "total_reviews_saved": 0}

def save_checkpoint(idx: int, total_reviews: int):
    """Save progress checkpoint"""
    try:
        checkpoint_dir = os.path.dirname(CHECKPOINT_FILE)
        os.makedirs(checkpoint_dir, exist_ok=True)
        checkpoint = {"last_product_idx": idx, "total_reviews_saved": total_reviews}
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{LOG_PREFIX} Error saving checkpoint: {e}")

def clear_checkpoint():
    """Clear checkpoint when completed successfully"""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            print(f"{LOG_PREFIX} Checkpoint cleared")
    except Exception as e:
        print(f"{LOG_PREFIX} Error clearing checkpoint: {e}")

def load_product_urls_from_today() -> List[Dict[str, str]]:
    """Load product URLs from today's products data"""
    today = datetime.now().strftime("%Y-%m-%d")
    products_dir = Path(OUTPUT_DIR) / "lazada" / f"date={today}"
    
    if not products_dir.exists():
        print(f"{LOG_PREFIX} No products data found for today: {products_dir}")
        return []
    
    products = []
    for jsonl_file in products_dir.glob("*.jsonl"):
        print(f"{LOG_PREFIX} Reading: {jsonl_file.name}")
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    product = json.loads(line)
                    if product.get('url'):
                        products.append({
                            'url': product['url'],
                            'id': product.get('product_id', ''),
                            'name': product.get('product_name', 'Unknown')
                        })
                except:
                    continue
    
    print(f"{LOG_PREFIX} Loaded {len(products)} product URLs from today's data")
    return products

def extract_reviews_from_product(page, product_url: str, product_id: str, product_name: str, max_reviews: int = 20) -> List[Dict[str, Any]]:
    """Extract reviews from product detail page"""
    reviews = []
    
    try:
        print(f"{LOG_PREFIX} Loading: {product_name[:50]}...")
        page.goto(product_url, wait_until='domcontentloaded', timeout=90000)
        page.wait_for_timeout(5000)
        
        # Scroll to reviews
        for _ in range(3):
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            page.wait_for_timeout(2000)
        
        # Extract reviews
        review_items = page.query_selector_all('div.mod-reviews div.item')
        print(f"{LOG_PREFIX} Found {len(review_items)} review items")
        
        for i, item in enumerate(review_items[:max_reviews]):
            try:
                reviewer_name = "Anonymous"
                try:
                    reviewer_elem = item.query_selector('span.reviewer')
                    if reviewer_elem:
                        reviewer_name = reviewer_elem.inner_text().strip()
                except:
                    pass
                
                review_date = None
                try:
                    time_elem = item.query_selector('span.time')
                    if time_elem:
                        review_date = time_elem.inner_text().strip()
                except:
                    pass
                
                rating = 0
                try:
                    stars = item.query_selector_all('div.container-star img.star')
                    rating = len(stars)
                except:
                    pass
                
                review_text = ""
                try:
                    review_elem = item.query_selector('div.item-content-main-content-reviews-item span')
                    if review_elem:
                        review_text = review_elem.inner_text().strip()
                except:
                    pass
                
                sku_info = ""
                try:
                    sku_elems = item.query_selector_all('div.skuInfo-item')
                    sku_parts = [sku.inner_text().strip() for sku in sku_elems]
                    sku_info = ", ".join(sku_parts)
                except:
                    pass
                
                helpful_count = 0
                try:
                    helpful_elem = item.query_selector('span.item-content-like-content-text')
                    if helpful_elem:
                        match = re.search(r'Helpful\((\d+)\)', helpful_elem.inner_text())
                        if match:
                            helpful_count = int(match.group(1))
                except:
                    pass
                
                if review_text:
                    reviews.append({
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
                    })
                    print(f"{LOG_PREFIX}   Review {i+1}: {rating}★ - {review_text[:50]}...")
            
            except Exception as e:
                print(f"{LOG_PREFIX} Failed review {i+1}: {e}")
                continue
        
        print(f"{LOG_PREFIX} Extracted {len(reviews)} reviews")
        
    except Exception as e:
        print(f"{LOG_PREFIX} Error: {e}")
    
    return reviews

def crawl_reviews_from_products(products: List[Dict[str, str]], max_products: int = 10000) -> List[Dict[str, Any]]:
     """Crawl reviews from product URLs with checkpoint support"""
     if not PLAYWRIGHT_AVAILABLE:
         print(f"{LOG_PREFIX} Playwright not available")
         return []
     
     # Load checkpoint to resume from last position
     checkpoint = load_checkpoint()
     start_idx = checkpoint["last_product_idx"]
     total_reviews_saved = checkpoint["total_reviews_saved"]
     
     all_reviews = []
     max_idx = min(len(products), max_products)
     
     print(f"{LOG_PREFIX} Resuming from product {start_idx}/{max_idx}")
     
     with sync_playwright() as p:
         browser = p.chromium.launch(
             headless=True,
             args=['--no-sandbox', '--disable-dev-shm-usage']
         )
         
         context = browser.new_context(
             viewport={'width': 1920, 'height': 1080},
             user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
         )
         
         # Load cookies if available
         if os.path.exists(COOKIE_FILE):
             with open(COOKIE_FILE, 'r') as f:
                 cookies = json.load(f)
                 context.add_cookies(cookies)
             print(f"{LOG_PREFIX} Cookies loaded")
         
         page = context.new_page()
         
         try:
             for i in range(start_idx, max_idx):
                 product = products[i]
                 progress = i + 1
                 print(f"\n{LOG_PREFIX} Product {progress}/{max_idx}")
                 
                 try:
                     reviews = extract_reviews_from_product(
                         page,
                         product['url'],
                         product['id'],
                         product['name']
                     )
                     all_reviews.extend(reviews)
                     
                     # Save checkpoint after each product
                     save_checkpoint(i + 1, total_reviews_saved + len(reviews))
                     total_reviews_saved += len(reviews)
                     
                     time.sleep(random.uniform(3, 5))
                     
                 except Exception as e:
                     print(f"{LOG_PREFIX} Failed product {progress}: {e}")
                     # Save checkpoint even on error to resume from next product
                     save_checkpoint(i + 1, total_reviews_saved)
                     continue
             
         finally:
             context.close()
             browser.close()
     
     return all_reviews

def save_reviews(reviews: List[Dict[str, Any]]):
    """Save reviews to JSONL file"""
    if not reviews:
        print(f"{LOG_PREFIX} No reviews to save")
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    date_dir = Path(OUTPUT_DIR) / "lazada_reviews" / f"date={today}"
    date_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"lazada_reviews_{timestamp}.jsonl"
    filepath = date_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for review in reviews:
            f.write(json.dumps(review, ensure_ascii=False) + '\n')
    
    print(f"\n{LOG_PREFIX} Saved {len(reviews)} reviews to: {filepath}")

def main():
     """Main execution"""
     print(f"{LOG_PREFIX} Starting Reviews Crawler")
     print("=" * 60)
     
     # Step 1: Load product URLs from today's products data
     products = load_product_urls_from_today()
     
     if not products:
         print(f"{LOG_PREFIX} No products found. Run products crawler first!")
         return
     
     print(f"{LOG_PREFIX} Found {len(products)} products to crawl reviews")
     
     # Step 2: Crawl reviews from products (limit to 10 for efficiency)
     reviews = crawl_reviews_from_products(products, max_products=10000)
     
     # Step 3: Save reviews
     if reviews:
         save_reviews(reviews)
         clear_checkpoint()  # Clear checkpoint on successful completion
         print(f"\n{LOG_PREFIX} SUCCESS! Total reviews: {len(reviews)}")
     else:
         print(f"\n{LOG_PREFIX} No reviews extracted")
     
     print("=" * 60)

if __name__ == "__main__":
    main()
