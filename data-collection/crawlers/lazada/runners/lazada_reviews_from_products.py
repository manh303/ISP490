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
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
PROFILE_DIR = os.environ.get("LAZADA_PROFILE_DIR", "/app/data/.profiles/lazada")
COOKIE_FILE = os.environ.get("LAZADA_COOKIE_FILE", str(Path(PROFILE_DIR) / "lazada_cookies.json"))
CHECKPOINT_FILE = os.environ.get("CRAWLER_CHECKPOINT_DIR", "/tmp/crawler_checkpoints") + "/lazada_reviews_checkpoint.json"
LOG_PREFIX = "[Lazada-Reviews]"

# Flush logs immediately to see progress when running in Docker
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)


def _build_canonical_url(product_url: str, product_id: str) -> str:
    """Normalize product URL to avoid anti-bot redirect and malformed paths"""
    # Lazada product ids usually appear as "i123456789". Extract the digits and build canonical URL.
    pid_match = re.search(r"i(\d+)", product_url) or re.search(r"i(\d+)", product_id or "")
    product_code = pid_match.group(1) if pid_match else None
    if product_code:
        return f"https://www.lazada.vn/products/i{product_code}.html"

    cleaned = re.sub(r"(?<!:)//+", "/", product_url).split("?")[0]
    if not cleaned.startswith("http"):
        cleaned = "https://" + cleaned.lstrip("/")
    return cleaned

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
    
    # Ensure we have a real product URL
    if not product_url or not product_url.startswith("http"):
        print(f"{LOG_PREFIX} Skip - invalid product URL: {product_url}")
        return reviews
    
    canonical_url = _build_canonical_url(product_url, product_id)
    
    try:
        print(f"{LOG_PREFIX} Loading: {product_name[:50]}...")
        try:
            page.goto(canonical_url, wait_until="domcontentloaded", timeout=45000, referer="https://www.lazada.vn/")

        except PlaywrightTimeoutError:
            print(f"{LOG_PREFIX} Timeout loading product URL. Skipping: {canonical_url}")
            return reviews
        print(f"{LOG_PREFIX} Landed on: {page.url}")
        
        # Detect punish/anti-bot redirect and retry once with canonical URL
        if "punish" in page.url or "x5sec" in page.url or "__" in page.url:
            print(f"{LOG_PREFIX} Detected anti-bot redirect. Retrying canonical URL: {canonical_url}")
            try:
                page.goto(canonical_url, wait_until="domcontentloaded", timeout=45000, referer="https://www.lazada.vn/")

            except PlaywrightTimeoutError:
                print(f"{LOG_PREFIX} Timeout on retry. Skipping product.")
                return reviews
            print(f"{LOG_PREFIX} After retry landed on: {page.url}")
            if "punish" in page.url or "x5sec" in page.url or "__" in page.url:
                print(f"{LOG_PREFIX} Still hitting anti-bot. Skipping product.")
                return reviews

        page.wait_for_timeout(5000)
        
        # Scroll to reviews; Lazada loads review block near bottom
        for _ in range(4):
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            page.wait_for_timeout(1500)
        
        # Prefer real structure from product_detail.html
        review_items = []
        mod_reviews = page.query_selector('div.mod-reviews')
        if mod_reviews:
            review_items = mod_reviews.query_selector_all('div.item')
            print(f"{LOG_PREFIX} Found {len(review_items)} review items in mod-reviews")
        
        # Fallback selectors if structure changes
        if not review_items:
            review_selectors = [
                '[data-qa-locator="review-item"]',
                '.pdp-review-item',
                '.review-list .item',
                '.ugc-review-item',
                'div.review-item',
                '[class*="review"]'
            ]
            for selector in review_selectors:
                try:
                    review_items = page.query_selector_all(selector)
                    if review_items:
                        print(f"{LOG_PREFIX} Found {len(review_items)} review items (selector: {selector})")
                        break
                except Exception:
                    continue

        if not review_items:
            print(f"{LOG_PREFIX} Found 0 review items with known selectors")
        
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
                    # Real structure: div.container-star.review-star > img.star
                    stars = item.query_selector_all('div.container-star img.star')
                    rating = len(stars)

                    # Fallback: look for rating text or aria-label on any star/rating element
                    if rating == 0:
                        rating_elem = item.query_selector('[class*="star"], [class*="rating"]')
                        if rating_elem:
                            text = rating_elem.inner_text()
                            match = re.search(r'([0-5](?:\\.\\d)?)', text)
                            if match:
                                rating = float(match.group(1))
                    if rating == 0:
                        # Sometimes rating is in aria-label of icons
                        icon = item.query_selector('[aria-label*="out of"]')
                        if icon:
                            match = re.search(r'([0-5](?:\\.\\d)?)', icon.get_attribute('aria-label') or '')
                            if match:
                                rating = float(match.group(1))
                except:
                    pass
                
                review_text = ""
                try:
                    review_elem = item.query_selector('div.item-content-main-content-reviews-item span')
                    if review_elem:
                        review_text = review_elem.inner_text().strip()
                    if not review_text:
                        # Fallback: grab visible text of review card
                        review_text = item.inner_text().strip()
                except:
                    pass
                
                sku_info = ""
                try:
                    sku_elems = item.query_selector_all('div.skuInfo-item')
                    sku_parts = []
                    for sku in sku_elems:
                        sku_parts.append(sku.inner_text().strip())
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
        HEADLESS = os.getenv("LAZADA_HEADLESS", "1") == "1"
        USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        ua = random.choice(USER_AGENTS)
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=HEADLESS,
            viewport={'width': 1920, 'height': 1080},
            user_agent=ua,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
            extra_http_headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "sec-ch-ua": '"Google Chrome";v="120", "Not=A?Brand";v="24", "Chromium";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
            },
        )

        if os.path.exists(COOKIE_FILE):
            with open(COOKIE_FILE, 'r') as f:
                context.add_cookies(json.load(f))
            print(f"{LOG_PREFIX} Cookies loaded from {COOKIE_FILE}")
        else:
            print(f"{LOG_PREFIX} Cookie file not found at {COOKIE_FILE} (running without login cookies)")

        page = context.new_page()
        page.set_default_navigation_timeout(120000)
        # warm-up để thiết lập cookie trên domain
        page.goto("https://www.lazada.vn/", wait_until="domcontentloaded", timeout=45000)
         
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
