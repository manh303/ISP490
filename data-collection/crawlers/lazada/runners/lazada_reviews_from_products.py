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
from typing import List, Dict, Any, Optional

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


# ============================================================
#  ETL METADATA LOGGING (schema METADATA.*)
# ============================================================

def _get_pg_conn():
    """
    Kết nối Postgres từ DATABASE_URL.
    Nếu không có hoặc lỗi -> trả về None (không làm fail crawler).
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print(f"{LOG_PREFIX} [META] DATABASE_URL not set, skip ETL logging.")
        return None

    try:
        import psycopg2  # type: ignore
    except ImportError:
        print(f"{LOG_PREFIX} [META] psycopg2 not installed, skip ETL logging.")
        return None

    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"{LOG_PREFIX} [META] Failed to connect to Postgres: {e}")
        return None


def log_etl_start(job_code: str, run_date: str) -> Optional[int]:
    """
    Ghi log bắt đầu crawler vào METADATA.etl_run.
    Trả về run_id hoặc None.
    """
    conn = _get_pg_conn()
    if conn is None:
        return None

    try:
        cur = conn.cursor()
        
        # Ensure job exists
        cur.execute(
            """
            INSERT INTO METADATA.etl_job (job_code, job_name, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_code) DO NOTHING
            """,
            (job_code, "Lazada Reviews Crawler", "Extract reviews from Lazada product pages")
        )
        
        # Get job_id
        cur.execute("SELECT job_id FROM METADATA.etl_job WHERE job_code = %s", (job_code,))
        row = cur.fetchone()
        if not row:
            print(f"{LOG_PREFIX} [META] Cannot find etl_job for {job_code}")
            conn.close()
            return None
        
        job_id = row[0]
        
        # Create run
        cur.execute(
            """
            INSERT INTO METADATA.etl_run (job_id, run_date, started_at, status)
            VALUES (%s, %s, %s, %s)
            RETURNING run_id
            """,
            (job_id, run_date, datetime.utcnow(), "RUNNING")
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"{LOG_PREFIX} [META] Created etl_run id={run_id} for {job_code}")
        return run_id
    except Exception as e:
        print(f"{LOG_PREFIX} [META] Error creating etl_run: {e}")
        try:
            conn.close()
        except:
            pass
        return None


def log_etl_finish(run_id: Optional[int], status: str, rows_read: int = 0, rows_written: int = 0, error_message: str = None):
    """
    Ghi log kết thúc crawler vào METADATA.etl_run.
    """
    if run_id is None:
        return

    conn = _get_pg_conn()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE METADATA.etl_run
            SET finished_at = %s,
                status = %s,
                rows_read = %s,
                rows_written = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (datetime.utcnow(), status, rows_read, rows_written, error_message, run_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"{LOG_PREFIX} [META] Finished etl_run id={run_id} with status={status}")
    except Exception as e:
        print(f"{LOG_PREFIX} [META] Error updating etl_run: {e}")
        try:
            conn.close()
        except:
            pass


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
        except Exception as e:
            # Catch browser closed errors
            if "closed" in str(e).lower():
                print(f"{LOG_PREFIX} ⚠️  Browser closed/crashed. Stopping crawler.")
                raise  # Re-raise to stop crawling
            print(f"{LOG_PREFIX} Error loading page: {e}")
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

def crawl_reviews_from_products(products: List[Dict[str, str]], max_products: int = 10000) -> tuple[List[Dict[str, Any]], bool]:
     """Crawl reviews from product URLs with checkpoint support
     
     Returns:
         tuple: (all_reviews, hit_antibot) - reviews list and anti-bot detection flag
     """
     if not PLAYWRIGHT_AVAILABLE:
         print(f"{LOG_PREFIX} Playwright not available")
         return [], False
     
     # Load checkpoint to resume from last position
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
     ANTIBOT_THRESHOLD = 3  # Skip after 3 consecutive anti-bot hits
     
     # Fix: Reset checkpoint if it's outdated (e.g., new day with fewer products)
     if start_idx >= max_idx:
         print(f"{LOG_PREFIX} ⚠️  Checkpoint outdated: last_product_idx={start_idx} >= total_products={max_idx}")
         print(f"{LOG_PREFIX} ℹ️  Resetting checkpoint to start from beginning")
         start_idx = 0
         total_reviews_saved = 0
         save_checkpoint(0, 0)
     
     print(f"{LOG_PREFIX} Resuming from product {start_idx}/{max_idx}")
     print(f"{LOG_PREFIX} Batch saving every {BATCH_SIZE} reviews")
     
     try:
         with sync_playwright() as p:
             HEADLESS = os.getenv("LAZADA_HEADLESS", "1") == "1"
             USER_AGENTS = [
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
             ]
             ua = random.choice(USER_AGENTS)
             
             # Browser args optimized for Docker/headless environment
             # Reference: https://peter.sh/experiments/chromium-command-line-switches/
             browser_args = [
                 '--no-sandbox',
                 '--disable-setuid-sandbox',
                 '--disable-dev-shm-usage',
                 '--disable-gpu',
                 '--disable-software-rasterizer',
                 '--disable-gl-drawing-for-tests',
                 '--disable-accelerated-2d-canvas',
                 '--disable-features=VizDisplayCompositor',
                 '--disable-features=IsolateOrigins,site-per-process',
                 '--disable-blink-features=AutomationControlled',
                 '--disable-infobars',
                 '--disable-background-networking',
                 '--disable-background-timer-throttling',
                 '--disable-backgrounding-occluded-windows',
                 '--disable-breakpad',
                 '--disable-component-extensions-with-background-pages',
                 '--disable-component-update',
                 '--disable-default-apps',
                 '--disable-extensions',
                 '--disable-hang-monitor',
                 '--disable-ipc-flooding-protection',
                 '--disable-popup-blocking',
                 '--disable-prompt-on-repost',
                 '--disable-renderer-backgrounding',
                 '--disable-sync',
                 '--disable-translate',
                 '--metrics-recording-only',
                 '--no-first-run',
                 '--password-store=basic',
                 '--use-mock-keychain',
                 '--single-process',  # Avoid GPU process initialization in Docker
                 '--ignore-certificate-errors',
                 '--window-size=1920,1080',
             ]
             
             # Increase browser launch timeout for slow Docker environments
             BROWSER_LAUNCH_TIMEOUT = 120000  # 2 minutes
             
             try:
                 context = p.chromium.launch_persistent_context(
                     user_data_dir=PROFILE_DIR,
                     headless=HEADLESS,
                     viewport={'width': 1920, 'height': 1080},
                     user_agent=ua,
                     args=browser_args,
                     timeout=BROWSER_LAUNCH_TIMEOUT,
                     ignore_https_errors=True,
                     java_script_enabled=True,
                     bypass_csp=True,
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
                         "upgrade-insecure-requests": "1",
                     },
                 )
             except PlaywrightTimeoutError as e:
                 print(f"{LOG_PREFIX} ⚠️  Browser launch timeout (likely GPU/display issue in Docker)")
                 print(f"{LOG_PREFIX} Error details: {e}")
                 print(f"{LOG_PREFIX} TIP: Ensure Xvfb is running (DISPLAY={os.getenv('DISPLAY', 'not set')})")
                 print(f"{LOG_PREFIX} TIP: Try increasing shared memory (--shm-size=2g in docker run)")
                 print(f"{LOG_PREFIX} Treating as SKIPPED - pipeline will continue")
                 return [], True  # Return empty + hit_antibot=True to skip gracefully
             except Exception as browser_error:
                 print(f"{LOG_PREFIX} ⚠️  Browser launch failed: {browser_error}")
                 print(f"{LOG_PREFIX} DISPLAY={os.getenv('DISPLAY', 'not set')}")
                 print(f"{LOG_PREFIX} Treating as SKIPPED - pipeline will continue")
                 return [], True

             if os.path.exists(COOKIE_FILE):
                 with open(COOKIE_FILE, 'r') as f:
                     context.add_cookies(json.load(f))
                 print(f"{LOG_PREFIX} Cookies loaded from {COOKIE_FILE}")
             else:
                 print(f"{LOG_PREFIX} Cookie file not found at {COOKIE_FILE} (running without login cookies)")

             page = context.new_page()
             page.set_default_navigation_timeout(120000)
             
             # warm-up để thiết lập cookie trên domain
             try:
                 page.goto("https://www.lazada.vn/", wait_until="domcontentloaded", timeout=45000)
                 # Check if landing page has anti-bot
                 if "punish" in page.url or "x5sec" in page.url or "__" in page.url:
                     print(f"{LOG_PREFIX} ⚠️  ANTI-BOT detected on homepage! Skipping crawler...")
                     hit_antibot = True
                     context.close()
                     return [], True
             except Exception as e:
                 print(f"{LOG_PREFIX} Error on homepage warmup: {e}")
          
             try:
                 for i in range(start_idx, max_idx):
                     if hit_antibot:
                         print(f"{LOG_PREFIX} ⚠️  Anti-bot threshold reached. Stopping crawler.")
                         break
                     
                     
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
                         
                         # Check if hit anti-bot (no reviews + suspicious URL)
                         if not reviews and ("punish" in page.url or "x5sec" in page.url or "__" in page.url):
                             antibot_count += 1
                             print(f"{LOG_PREFIX} ⚠️  Anti-bot detected ({antibot_count}/{ANTIBOT_THRESHOLD})")
                             if antibot_count >= ANTIBOT_THRESHOLD:
                                 hit_antibot = True
                                 print(f"{LOG_PREFIX} ⚠️  Anti-bot threshold reached. Stopping crawler.")
                                 break
                         else:
                             antibot_count = 0  # Reset counter on success
                         
                         all_reviews.extend(reviews)
                         batch_reviews.extend(reviews)
                         
                         # Batch save every BATCH_SIZE reviews
                         if len(batch_reviews) >= BATCH_SIZE:
                             print(f"{LOG_PREFIX} 💾 Saving batch {batch_num} ({len(batch_reviews)} reviews)")
                             save_reviews(batch_reviews, batch_num)
                             batch_reviews = []
                             batch_num += 1
                         
                         # Save checkpoint after each product
                         save_checkpoint(i + 1, total_reviews_saved + len(reviews))
                         total_reviews_saved += len(reviews)
                         
                         time.sleep(random.uniform(3, 5))
                         
                     except Exception as e:
                         error_str = str(e).lower()
                         # Check if browser crashed/closed
                         if "closed" in error_str or "browser" in error_str:
                             print(f"{LOG_PREFIX} ⚠️  Browser crashed/closed: {e}")
                             print(f"{LOG_PREFIX} ⚠️  Stopping crawler - will resume from checkpoint")
                             hit_antibot = True  # Treat as skip
                             break
                         
                         print(f"{LOG_PREFIX} Failed product {progress}: {e}")
                         # Save checkpoint even on error to resume from next product
                         save_checkpoint(i + 1, total_reviews_saved)
                         continue
                 
                 # Save remaining reviews in batch
                 if batch_reviews:
                     print(f"{LOG_PREFIX} 💾 Saving final batch {batch_num} ({len(batch_reviews)} reviews)")
                     save_reviews(batch_reviews, batch_num)
                  
             finally:
                 context.close()
      
     except Exception as e:
         print(f"{LOG_PREFIX} ⚠️  Playwright error: {e}")
         print(f"{LOG_PREFIX} This might be a browser/environment issue")
         # If it's a timeout or launch error, treat as skip
         if "Timeout" in str(e) or "launch" in str(e):
             print(f"{LOG_PREFIX} Treating as SKIPPED - pipeline will continue")
             return [], True
         # Otherwise re-raise
         raise
     
     return all_reviews, hit_antibot

def save_reviews(reviews: List[Dict[str, Any]], batch_num: int = None):
    """Save reviews to JSONL file (supports batch saving)"""
    if not reviews:
        print(f"{LOG_PREFIX} No reviews to save")
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    date_dir = Path(OUTPUT_DIR) / "lazada_reviews" / f"date={today}"
    date_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_suffix = f"_batch{batch_num}" if batch_num is not None else ""
    filename = f"lazada_reviews_{timestamp}{batch_suffix}.jsonl"
    filepath = date_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for review in reviews:
            f.write(json.dumps(review, ensure_ascii=False) + '\n')
    
    print(f"\n{LOG_PREFIX} Saved {len(reviews)} reviews to: {filepath}")

def main():
     """Main execution with ETL metadata logging"""
     print(f"{LOG_PREFIX} Starting Reviews Crawler")
     print("=" * 60)
     
     # ETL logging setup
     today = datetime.now().strftime("%Y-%m-%d")
     job_code = "LAZADA_REVIEWS_CRAWLER"
     run_id = log_etl_start(job_code, today)
     
     products_count = 0
     reviews_count = 0
     error_msg = None
     
     try:
         # Step 1: Load product URLs from today's products data
         products = load_product_urls_from_today()
         products_count = len(products)
         
         if not products:
             error_msg = "No products found. Run products crawler first!"
             print(f"{LOG_PREFIX} {error_msg}")
             log_etl_finish(run_id, "SKIPPED", rows_read=0, rows_written=0, error_message=error_msg)
             sys.exit(0)  # Exit 0 to skip gracefully in DAG
         
         print(f"{LOG_PREFIX} Found {len(products)} products to crawl reviews")
         
         # Step 2: Crawl reviews from products (with batch saving)
         reviews, hit_antibot = crawl_reviews_from_products(products, max_products=10000)
         reviews_count = len(reviews)
         
         # Step 3: Handle anti-bot detection, browser crash, or launch issues
         if hit_antibot:
             if reviews_count == 0 and products_count < 3:
                 error_msg = "Browser launch timeout/GPU initialization failed (Docker environment issue)"
             elif reviews_count > 0:
                 error_msg = f"Browser crashed after processing {products_count} products (collected {reviews_count} reviews before crash)"
             else:
                 error_msg = "ANTI-BOT DETECTED - Skipping reviews crawler (graceful skip)"
             print(f"\n{LOG_PREFIX} ⚠️  {error_msg}")
             print(f"{LOG_PREFIX} ℹ️  Pipeline will continue without review data")
             print(f"{LOG_PREFIX} ℹ️  Checkpoint saved - will resume from product #{products_count} next time")
             print("=" * 60)
             log_etl_finish(run_id, "SKIPPED", rows_read=products_count, rows_written=reviews_count, error_message=error_msg)
             sys.exit(0)  # Exit 0 to skip task gracefully (DAG won't fail)
         
         # Step 4: Report results
         if reviews:
             clear_checkpoint()  # Clear checkpoint on successful completion
             print(f"\n{LOG_PREFIX} ✅ SUCCESS! Total reviews: {len(reviews)}")
             log_etl_finish(run_id, "SUCCESS", rows_read=products_count, rows_written=reviews_count)
         else:
             error_msg = "No reviews extracted from products"
             print(f"\n{LOG_PREFIX} {error_msg}")
             log_etl_finish(run_id, "SUCCESS", rows_read=products_count, rows_written=0, error_message=error_msg)
         
         print("=" * 60)
         
     except Exception as e:
         error_msg = f"Crawler failed: {str(e)}"
         print(f"\n{LOG_PREFIX} ❌ ERROR: {error_msg}")
         import traceback
         traceback.print_exc()
         log_etl_finish(run_id, "FAILED", rows_read=products_count, rows_written=reviews_count, error_message=error_msg[:500])
         sys.exit(1)  # Exit 1 on real errors

if __name__ == "__main__":
    main()
