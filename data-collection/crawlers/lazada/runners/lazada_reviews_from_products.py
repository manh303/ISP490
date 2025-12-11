#!/usr/bin/env python3
"""
Lazada Reviews Crawler - Extract reviews from products data
Runs AFTER products crawler and uses product URLs from products data

Supports both Docker and Windows PowerShell environments:
- Docker: Uses /tmp/data/outputs, /app/data/.profiles/lazada
- Windows: Uses project-relative paths (data-collection/crawlers/lazada/data/outputs, etc.)
"""
import json
import time
import random
import os
import re
import uuid
import sys
import platform
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def _get_project_root() -> Path:
    """Get the project root directory (ecommerce-dss-project)"""
    current_file = Path(__file__).resolve()
    # Navigate up from: runners -> lazada -> crawlers -> data-collection -> ecommerce-dss-project
    return current_file.parent.parent.parent.parent.parent


def _get_lazada_data_dir() -> Path:
    """Get the data directory at project root (ecommerce-dss-project/data)"""
    return _get_project_root() / "data"


def _is_windows() -> bool:
    """Check if running on Windows"""
    return platform.system() == "Windows"


def _is_docker() -> bool:
    """Check if running inside Docker container"""
    # Check for Docker-specific files
    if os.path.exists("/.dockerenv"):
        return True
    # Check cgroup for Docker
    try:
        with open("/proc/1/cgroup", "r") as f:
            return "docker" in f.read()
    except:
        return False


# Determine environment and set paths accordingly
if _is_windows() or (not _is_docker()):
    # Running on Windows PowerShell or outside Docker
    _PROJECT_ROOT = _get_project_root()
    _LAZADA_DATA_DIR = _get_lazada_data_dir()
    
    # Create data directories if they don't exist
    _OUTPUT_BASE = _LAZADA_DATA_DIR / "outputs"
    _PROFILE_BASE = _LAZADA_DATA_DIR / ".profiles" / "lazada"
    _CHECKPOINT_BASE = _LAZADA_DATA_DIR / ".checkpoints"
    
    _OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    _PROFILE_BASE.mkdir(parents=True, exist_ok=True)
    _CHECKPOINT_BASE.mkdir(parents=True, exist_ok=True)
    
    OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", str(_OUTPUT_BASE))
    PROFILE_DIR = os.environ.get("LAZADA_PROFILE_DIR", str(_PROFILE_BASE))
    COOKIE_FILE = os.environ.get("LAZADA_COOKIE_FILE", str(_PROFILE_BASE / "lazada_cookies.json"))
    CHECKPOINT_FILE = os.environ.get("CRAWLER_CHECKPOINT_DIR", str(_CHECKPOINT_BASE)) + "/lazada_reviews_checkpoint.json"
    
    print(f"[ENV] Running on {'Windows' if _is_windows() else 'Local'} environment")
    print(f"[ENV] OUTPUT_DIR: {OUTPUT_DIR}")
    print(f"[ENV] PROFILE_DIR: {PROFILE_DIR}")
    print(f"[ENV] CHECKPOINT_DIR: {os.path.dirname(CHECKPOINT_FILE)}")
else:
    # Running inside Docker
    OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
    PROFILE_DIR = os.environ.get("LAZADA_PROFILE_DIR", "/app/data/.profiles/lazada")
    COOKIE_FILE = os.environ.get("LAZADA_COOKIE_FILE", str(Path(PROFILE_DIR) / "lazada_cookies.json"))
    CHECKPOINT_FILE = os.environ.get("CRAWLER_CHECKPOINT_DIR", "/tmp/crawler_checkpoints") + "/lazada_reviews_checkpoint.json"
    
    print(f"[ENV] Running inside Docker container")
    print(f"[ENV] OUTPUT_DIR: {OUTPUT_DIR}")
    print(f"[ENV] PROFILE_DIR: {PROFILE_DIR}")

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
    # today = datetime.now().strftime("%Y-%m-%d")
    today = "2025-12-10"
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
            # Use 'load' or 'networkidle' to wait for full page including JS
            page.goto(canonical_url, wait_until="load", timeout=60000, referer="https://www.lazada.vn/")
            # Additional wait for dynamic content to load
            page.wait_for_timeout(3000)
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
                page.goto(canonical_url, wait_until="load", timeout=60000, referer="https://www.lazada.vn/")
                page.wait_for_timeout(3000)
            except PlaywrightTimeoutError:
                print(f"{LOG_PREFIX} Timeout on retry. Skipping product.")
                return reviews
            print(f"{LOG_PREFIX} After retry landed on: {page.url}")
            if "punish" in page.url or "x5sec" in page.url or "__" in page.url:
                print(f"{LOG_PREFIX} Still hitting anti-bot. Skipping product.")
                return reviews

        # Simulate human behavior: random mouse movement
        try:
            page.mouse.move(random.randint(100, 500), random.randint(100, 300))
            page.wait_for_timeout(500)
        except:
            pass

        # ==================== DEBUG: Check what loaded ====================
        print(f"{LOG_PREFIX} [DEBUG] Checking page content...")
        
        # Check for price
        price_found = False
        try:
            price_elem = page.query_selector('.pdp-price, [class*="price"], .product-price, span[data-price]')
            if price_elem:
                price_text = price_elem.inner_text().strip()
                print(f"{LOG_PREFIX} [DEBUG] ✓ Price found: {price_text[:50]}")
                price_found = True
            else:
                print(f"{LOG_PREFIX} [DEBUG] ✗ No price element found")
        except Exception as e:
            print(f"{LOG_PREFIX} [DEBUG] ✗ Price check error: {e}")
        
        # Check for stock status
        try:
            stock_elems = page.query_selector_all('[class*="stock"], [class*="out-of-stock"], [class*="sold-out"]')
            if stock_elems:
                for elem in stock_elems[:3]:
                    text = elem.inner_text().strip()
                    if text:
                        print(f"{LOG_PREFIX} [DEBUG] Stock status text: {text[:50]}")
        except:
            pass
        
        # Check for reviews section  
        try:
            reviews_section = page.query_selector('#module_product_review, .mod-reviews, [class*="review-list"], [data-spm="reviews"]')
            if reviews_section:
                print(f"{LOG_PREFIX} [DEBUG] ✓ Reviews section found")
            else:
                print(f"{LOG_PREFIX} [DEBUG] ✗ No reviews section found")
        except:
            print(f"{LOG_PREFIX} [DEBUG] ✗ Reviews section check failed")
        
        # Check body length (blocked pages usually have short body)
        try:
            body_html = page.content()
            print(f"{LOG_PREFIX} [DEBUG] Page HTML length: {len(body_html)} chars")
            if len(body_html) < 50000:
                print(f"{LOG_PREFIX} [DEBUG] ⚠️  Page might be blocked (HTML too short)")
        except:
            pass
        
        # Save debug screenshot  
        try:
            debug_screenshot = Path(OUTPUT_DIR) / "debug_screenshot.png"
            if not debug_screenshot.exists():
                page.screenshot(path=str(debug_screenshot))
                print(f"{LOG_PREFIX} [DEBUG] Screenshot saved: {debug_screenshot}")
        except:
            pass
        # ==================== END DEBUG ====================

        # Scroll naturally like a human (small increments with pauses)
        for scroll_pos in [300, 600, 1000, 1500, 2000, 3000]:
            page.evaluate(f'window.scrollTo(0, {scroll_pos})')
            page.wait_for_timeout(random.randint(800, 1500))
        
        # Scroll to bottom to trigger all lazy loads
        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        page.wait_for_timeout(2000)
        
        # Try to click on "Reviews" tab (with short timeout, don't block if fails)
        reviews_clicked = False
        try:
            # Method 1: Direct selector
            reviews_tab = page.query_selector('a[href="#reviews"], [data-spm="reviews"], .pdp-tab-reviews, div[data-tab="reviews"]')
            if reviews_tab:
                try:
                    reviews_tab.click(timeout=3000, force=True)
                    page.wait_for_timeout(1500)
                    reviews_clicked = True
                    print(f"{LOG_PREFIX} Clicked Reviews tab (method 1)")
                except:
                    pass
        except:
            pass
        
        if not reviews_clicked:
            try:
                # Method 2: Try by text content
                reviews_text = page.locator('text=/Reviews|Đánh giá/i').first
                if reviews_text:
                    reviews_text.click(timeout=3000, force=True)
                    page.wait_for_timeout(1500)
                    reviews_clicked = True
                    print(f"{LOG_PREFIX} Clicked Reviews tab (method 2 - text)")
            except:
                pass
        
        if not reviews_clicked:
            print(f"{LOG_PREFIX} Could not click Reviews tab, proceeding anyway...")
        
        # Scroll to reviews section
        try:
            review_section = page.query_selector('#module_product_review, .mod-reviews, [id*="review"], [class*="review-list"]')
            if review_section:
                review_section.scroll_into_view_if_needed()
                page.wait_for_timeout(1500)
        except:
            pass
        
        # DEBUG: Save page HTML for first product to analyze structure
        try:
            if product_id and "debug" not in product_id:
                debug_file = Path(OUTPUT_DIR) / "debug_lazada_page.html"
                if not debug_file.exists():  # Only save once
                    html_content = page.content()
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    print(f"{LOG_PREFIX} [DEBUG] Saved page HTML to {debug_file}")
        except Exception as e:
            print(f"{LOG_PREFIX} [DEBUG] Could not save HTML: {e}")
        
        # Prefer real structure from product_detail.html
        review_items = []
        mod_reviews = page.query_selector('div.mod-reviews')
        if mod_reviews:
            review_items = mod_reviews.query_selector_all('div.item')
            print(f"{LOG_PREFIX} Found {len(review_items)} review items in mod-reviews")
        
        # Fallback selectors if structure changes - from most specific to least
        if not review_items:
            review_selectors = [
                '[data-qa-locator="review-item"]',
                '.pdp-review-list .pdp-review-item',
                '.review-list .review-item', 
                '.mod-reviews .item',
                '.pdp-review-item',
                '.review-list .item',
                '.ugc-review-item',
                'div.review-item',
            ]
            for selector in review_selectors:
                try:
                    review_items = page.query_selector_all(selector)
                    if review_items and len(review_items) > 0:
                        print(f"{LOG_PREFIX} Found {len(review_items)} review items (selector: {selector})")
                        break
                except Exception:
                    continue

        if not review_items:
            print(f"{LOG_PREFIX} Found 0 review items with known selectors")
        
        for i, item in enumerate(review_items[:max_reviews]):
            try:
                # Get full text content first for debugging
                full_text = ""
                try:
                    full_text = item.inner_text().strip()
                except:
                    pass
                
                # Skip if this looks like a header/tab rather than a review
                if len(full_text) < 20 or full_text.lower() in ['reviews', 'product details', 'recommendations']:
                    continue
                
                reviewer_name = "Anonymous"
                # Try multiple selectors for reviewer name
                reviewer_selectors = ['span.reviewer', '.reviewer-name', '.review-user-name', 'span.middle']
                for sel in reviewer_selectors:
                    try:
                        reviewer_elem = item.query_selector(sel)
                        if reviewer_elem:
                            name = reviewer_elem.inner_text().strip()
                            if name and len(name) > 0:
                                reviewer_name = name
                                break
                    except:
                        continue
                
                review_date = None
                # Try multiple selectors for date
                date_selectors = ['span.time', '.review-time', '.review-date', 'span.title-right']
                for sel in date_selectors:
                    try:
                        time_elem = item.query_selector(sel)
                        if time_elem:
                            date_text = time_elem.inner_text().strip()
                            if date_text and len(date_text) > 0:
                                review_date = date_text
                                break
                    except:
                        continue
                
                rating = 0
                try:
                    # Method 1: Count star images
                    stars = item.query_selector_all('img.star, .star-icon, [class*="star-fill"], .rate-star')
                    if stars:
                        rating = len(stars)
                    
                    # Method 2: Look for filled stars specifically
                    if rating == 0:
                        filled_stars = item.query_selector_all('[class*="star"][class*="full"], [class*="star"][class*="fill"]')
                        rating = len(filled_stars)
                    
                    # Method 3: Extract from rating text like "4.3/5"
                    if rating == 0:
                        rating_elem = item.query_selector('[class*="star"], [class*="rating"], .score')
                        if rating_elem:
                            text = rating_elem.inner_text()
                            match = re.search(r'(\d(?:\.\d)?)', text)
                            if match:
                                rating = float(match.group(1))
                    
                    # Method 4: aria-label
                    if rating == 0:
                        icon = item.query_selector('[aria-label*="out of"], [aria-label*="star"]')
                        if icon:
                            match = re.search(r'(\d(?:\.\d)?)', icon.get_attribute('aria-label') or '')
                            if match:
                                rating = float(match.group(1))
                except:
                    pass
                
                review_text = ""
                # Try multiple selectors for review content
                content_selectors = [
                    '.item-content .content',
                    '.review-content', 
                    '.review-text',
                    '.item-content-main-content-reviews-item span',
                    '.item-content-main span',
                    '.content',
                    'p'
                ]
                for sel in content_selectors:
                    try:
                        review_elem = item.query_selector(sel)
                        if review_elem:
                            text = review_elem.inner_text().strip()
                            # Filter out very short or navigation-like text
                            if text and len(text) > 10 and text.lower() not in ['helpful', 'report', 'reviews']:
                                review_text = text
                                break
                    except:
                        continue
                
                # Final fallback: use full text but clean it
                if not review_text and full_text and len(full_text) > 20:
                    # Remove common non-review text patterns
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    # Find the longest line that looks like actual content
                    content_lines = [l for l in lines if len(l) > 15 and 'helpful' not in l.lower()]
                    if content_lines:
                        review_text = ' '.join(content_lines[:3])  # Take first 3 content lines
                
                sku_info = ""
                try:
                    sku_elems = item.query_selector_all('div.skuInfo-item, .sku-info, .variation-label')
                    sku_parts = []
                    for sku in sku_elems:
                        sku_parts.append(sku.inner_text().strip())
                    sku_info = ", ".join(sku_parts)
                except:
                    pass
                
                helpful_count = 0
                try:
                    helpful_elem = item.query_selector('span.item-content-like-content-text, .helpful-count')
                    if helpful_elem:
                        match = re.search(r'Helpful\((\d+)\)|(\d+)', helpful_elem.inner_text())
                        if match:
                            helpful_count = int(match.group(1) or match.group(2))
                except:
                    pass
                
                # Save review if we got meaningful text
                if review_text and len(review_text) > 10:
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
                else:
                    # Debug: print what we found
                    if i < 3:  # Only print first 3 for debugging
                        print(f"{LOG_PREFIX}   [DEBUG] Item {i+1} text (first 100 chars): {full_text[:100]}...")
            
            except Exception as e:
                print(f"{LOG_PREFIX} Failed review {i+1}: {e}")
                continue
        
        print(f"{LOG_PREFIX} Extracted {len(reviews)} reviews")
        
    except Exception as e:
        error_str = str(e).lower()
        # Re-raise browser closed/crashed exceptions to stop the main loop
        if "closed" in error_str or "browser" in error_str or "target page" in error_str:
            print(f"{LOG_PREFIX} ⚠️  Browser closed/crashed. Stopping crawler.")
            print(f"{LOG_PREFIX} Error: {e}")
            raise  # Propagate to main loop to stop crawling
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
            # On Windows: show browser by default so user can click anti-bot
            # In Docker: headless by default
            if _is_windows():
                HEADLESS = os.getenv("LAZADA_HEADLESS", "0") == "1"  # Default: show browser
            else:
                HEADLESS = os.getenv("LAZADA_HEADLESS", "1") == "1"  # Default: headless
            
            print(f"{LOG_PREFIX} Browser mode: {'Headless' if HEADLESS else 'Visible (headed)'}")
            USER_AGENTS = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
            ]
            ua = random.choice(USER_AGENTS)
            
            # Browser args - different for Windows vs Docker
            if _is_windows():
                # Windows: args to hide automation banner and look like real Chrome
                browser_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-infobars',
                    '--disable-popup-blocking',
                    '--no-first-run',
                    '--ignore-certificate-errors',
                    '--window-size=1920,1080',
                    # Critical: these args hide the automation banner
                    '--disable-automation',
                    '--disable-extensions',
                    '--disable-component-extensions-with-background-pages',
                    '--disable-background-networking',
                    '--disable-sync',
                    '--metrics-recording-only',
                    '--disable-default-apps',
                    '--mute-audio',
                    '--no-default-browser-check',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-component-update',
                ]
            else:
                # Docker/Linux: full args for headless environment
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
                # On Windows: use real Chrome browser (channel='chrome') to bypass bot detection
                # On Docker/Linux: use Chromium (no channel)
                if _is_windows():
                    print(f"{LOG_PREFIX} Using real Chrome browser (channel='chrome')")
                    browser = p.chromium.launch(
                        headless=HEADLESS,
                        channel="chrome",  # Use real Chrome instead of Chromium
                        args=browser_args,
                        timeout=BROWSER_LAUNCH_TIMEOUT,
                    )
                else:
                    browser = p.chromium.launch(
                        headless=HEADLESS,
                        args=browser_args,
                        timeout=BROWSER_LAUNCH_TIMEOUT,
                    )
                
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent=ua,
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
            
            # IMPORTANT: Inject stealth scripts to hide automation fingerprints
            # This must happen before any navigation
            stealth_script = """
            () => {
                // Hide webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Override the plugins to look like a normal browser
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        {
                            0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                            description: "Portable Document Format",
                            filename: "internal-pdf-viewer",
                            length: 1,
                            name: "Chrome PDF Plugin"
                        },
                        {
                            0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                            description: "Portable Document Format",
                            filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                            length: 1,
                            name: "Chrome PDF Viewer"
                        }
                    ],
                });
                
                // Override languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['vi-VN', 'vi', 'en-US', 'en'],
                });
                
                // Remove automation-related properties
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
                
                // Override chrome object
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };
                
                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                console.log('[Stealth] Anti-detection patches applied');
            }
            """
            
            # Add initialization script to run on every new page
            context.add_init_script(stealth_script)
            print(f"{LOG_PREFIX} Stealth mode enabled - hiding automation fingerprints")
            
            # warm-up để thiết lập cookie trên domain
            try:
                page.goto("https://www.lazada.vn/", wait_until="domcontentloaded", timeout=45000)
                page.wait_for_timeout(2000)  # Wait for stealth to take effect
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
