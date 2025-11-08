#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lazada crawler (Playwright + Network JSON)
- Né anti-bot tốt hơn bằng cách:
  * Chromium headless (hoặc non-headless qua LAZADA_HEADLESS=0)
  * Block ảnh/font để nhẹ + giảm dấu vết
  * User-Agent/Accept-Language spoof
  * Bắt các response JSON chứa listItems/mods từ network (không cần render đầy đủ DOM)
  * Fallback parse HTML khi cần
  * Retry có backoff nhẹ; hỗ trợ proxy luân phiên qua LAZADA_HTTP_PROXY
- Output: JSON/CSV vào thư mục output/
Env hỗ trợ:
  LAZADA_UA            : User-Agent tuỳ chỉnh
  LAZADA_HTTP_PROXY    : http://user:pass@host:port
  LAZADA_HEADLESS      : "0" để non-headless (nếu có Xvfb), mặc định headless
  LAZADA_MAX_PAGES     : số trang / category (mặc định 20)
  LAZADA_SLEEP_BASE    : base sleep giãn tần suất (mặc định 1.2)
"""

import os, re, json, time, random, csv, sys
from typing import List, Dict, Any, Optional
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------- Config danh mục (bạn có thể chỉnh) ----------
CATEGORIES = {
    "smartphones": "https://www.lazada.vn/tag/mobiles/?q=mobiles",
    "laptops":     "https://www.lazada.vn/tag/laptops/?q=laptops",
    "tablets":     "https://www.lazada.vn/tag/tablets/?q=tablets",
    "smartwatch":  "https://www.lazada.vn/tag/smartwatch/?q=smartwatch",
    "tvs":         "https://www.lazada.vn/tag/tv/?q=tv",
    "headphones":  "https://www.lazada.vn/tag/headphones/?q=headphones",
    "cameras":     "https://www.lazada.vn/tag/cameras/?q=cameras",
    "monitors":    "https://www.lazada.vn/tag/monitors/?q=monitors",
    "desktops":    "https://www.lazada.vn/tag/desktop-computer/?q=desktop+computer",
}

ANTI_PATTERNS = [
    "are you human", "captcha", "security verification",
    "access denied", "forbidden",
    "tạm thời không thể truy cập", "xác minh bảo mật"
]

def rand_sleep(a: float, b: float):
    time.sleep(random.uniform(a, b))

def is_blocked_html(html: str) -> bool:
    low = (html or "").lower()
    return any(p in low for p in ANTI_PATTERNS)

def productize(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Chuẩn hóa một record sản phẩm từ JSON network."""
    out: Dict[str, Any] = {}
    out["name"] = obj.get("name") or obj.get("itemTitle") or obj.get("title")
    price = obj.get("price") or obj.get("itemPrice") or obj.get("priceShow")
    if isinstance(price, str):
        digits = re.sub(r"[^\d.]", "", price)
        out["price"] = float(digits) if digits else None
        out["price_text"] = price
    else:
        out["price"] = price
        out["price_text"] = price if price is not None else ""
    out["seller"] = obj.get("sellerName") or obj.get("seller") or obj.get("seller_name")
    out["rating"] = obj.get("ratingScore") or obj.get("rating") or obj.get("itemRating")
    url = obj.get("productUrl") or obj.get("itemUrl") or obj.get("productUrlPath")
    if url and url.startswith("//"):
        url = "https:" + url
    out["url"] = url
    out["productId"] = obj.get("itemId") or obj.get("productId") or obj.get("nid")
    return out

def extract_from_html(html: str) -> List[Dict[str, Any]]:
    """Fallback: một số trang nhúng JSON (mods/listItems) trong HTML."""
    txt = html
    soup = BeautifulSoup(html, "lxml")
    try:
        # văn bản gộp có khi dễ match hơn
        txt = soup.get_text(" ", strip=False)
    except Exception:
        pass

    patterns = [
        r'"listItems"\s*:\s*(\[[^\]]+\])',
        r'"mods"\s*:\s*({.*?})\s*,\s*"mainInfo"',
        r'"mods"\s*:\s*({.*})\s*,"filters"',
    ]
    all_items: List[Dict[str, Any]] = []

    for pat in patterns:
        m = re.search(pat, txt, flags=re.S)
        if not m:
            m = re.search(pat, html, flags=re.S)
        if not m:
            continue
        blob = m.group(1)
        try:
            data = json.loads(blob)
            if isinstance(data, dict) and "listItems" in data:
                all_items.extend(data["listItems"])
            elif isinstance(data, list):
                all_items.extend(data)
        except Exception:
            continue

    return [productize(x) for x in all_items if isinstance(x, dict)]

def paginated_url(base: str, page: int) -> str:
    if page <= 1:
        return base
    return f"{base}&page={page}" if "?" in base else f"{base}?page={page}"

def fetch_listing_with_network(page, url: str, wait_secs: int = 15) -> List[Dict[str, Any]]:
    """
    Đợi các response JSON phù hợp:
    - Content-Type: application/json
    - URL có dấu hiệu API listing/catalog
    Gom 'mods.listItems' hoặc 'listItems' từ response data.
    """
    def predicate(resp):
        try:
            ctype = resp.headers.get("content-type", "")
            if "application/json" not in ctype:
                return False
            u = resp.url.lower()
            return any(x in u for x in [
                "lazada.", "/catalog", "/search", "/item", "/pdp", "acs-", "mtop"
            ])
        except Exception:
            return False

    start = time.time()
    products: List[Dict[str, Any]] = []
    seen_snippets = 0

    while time.time() - start < wait_secs and len(products) < 120:
        try:
            resp = page.wait_for_response(predicate, timeout=3000)
            if "application/json" not in resp.headers.get("content-type", ""):
                continue
            data = resp.json()
            jtxt = json.dumps(data)
            grabbed = False

            # 1) Thẳng tay vào mods.listItems
            try:
                li = data["mods"]["listItems"]
                if isinstance(li, list):
                    products.extend([productize(x) for x in li if isinstance(x, dict)])
                    grabbed = True
            except Exception:
                pass

            # 2) Deep search bằng regex
            if not grabbed:
                m = re.search(r'"listItems"\s*:\s*(\[[^\]]+\])', jtxt, re.S)
                if m:
                    try:
                        li = json.loads(m.group(1))
                        products.extend([productize(x) for x in li if isinstance(x, dict)])
                        grabbed = True
                    except Exception:
                        pass

            if grabbed:
                seen_snippets += 1
                # dừng sớm nếu đã gom đủ
                if seen_snippets >= 2 and len(products) >= 40:
                    break
        except PWTimeout:
            pass
        except Exception:
            pass

    # lọc trùng theo url
    uniq, seen = [], set()
    for p in products:
        u = p.get("url")
        if u and u not in seen:
            seen.add(u)
            uniq.append(p)
    return uniq

def crawl_category(play, category: str, base_url: str, max_pages: int, sleep_base: float) -> List[Dict[str, Any]]:
    """
    Mỗi trang: goto -> bắt network JSON -> fallback HTML nếu cần.
    Block ảnh/font qua route() để nhẹ và giảm fingerprint.
    """
    ua = os.getenv("LAZADA_UA",
                   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36")
    proxy = os.getenv("LAZADA_HTTP_PROXY")
    headless = os.getenv("LAZADA_HEADLESS", "1") != "0"

    launch_args = ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"]
    browser = play.chromium.launch(headless=headless, args=launch_args)

    context_kwargs = {
        "user_agent": ua,
        "locale": "vi-VN",
        "java_script_enabled": True,
        "bypass_csp": True,
    }
    if proxy:
        context_kwargs["proxy"] = {"server": proxy}

    context = browser.new_context(**context_kwargs)
    context.set_default_navigation_timeout(60000)
    context.set_default_timeout(60000)

    # Chặn ảnh/font/icon
    context.route(
        "**/*",
        lambda r: r.abort()
        if re.search(r"\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf)(\?|$)", r.request.url, re.I)
        else r.continue_()
    )

    page = context.new_page()
    page.set_extra_http_headers({"Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"})

    all_products: List[Dict[str, Any]] = []

    print(f"\n=== Crawling {category.upper()} ===")
    for i in range(1, max_pages + 1):
        url = paginated_url(base_url, i)
        print(f"Trang {i}: {url}")

        try:
            page.goto(url, wait_until="domcontentloaded")
            rand_sleep(sleep_base, sleep_base + 0.8)

            html = page.content()
            if is_blocked_html(html):
                raise RuntimeError("Blocked by anti-bot (HTML)")

            # Ưu tiên JSON từ network
            products = fetch_listing_with_network(page, url, wait_secs=15)

            # Fallback từ HTML nếu network không có
            if not products:
                products = extract_from_html(html)

            # Gắn category + lọc record hợp lệ
            norm: List[Dict[str, Any]] = []
            for p in products:
                if p.get("name") and p.get("url"):
                    p["category"] = category
                    norm.append(p)

            # gộp
            # (xử lý trùng lặp theo url trong toàn danh mục)
            before = len(all_products)
            seen = {x["url"] for x in all_products if x.get("url")}
            for p in norm:
                if p["url"] not in seen:
                    all_products.append(p)
                    seen.add(p["url"])

            print(f"   +{len(all_products) - before} sản phẩm (tổng {len(all_products)})")

            # Giãn nhịp
            if i < max_pages:
                rand_sleep(sleep_base + 0.5, sleep_base + 2.0)

        except Exception as e:
            print(f"   Lỗi trang {i}: {e}")
            # backoff nhẹ
            rand_sleep(2.0, 4.0)
            continue

    context.close()
    browser.close()
    return all_products

def save_outputs(rows: List[Dict[str, Any]], prefix: str):
    outdir = Path("output")
    outdir.mkdir(exist_ok=True)

    # JSON
    jf = outdir / f"{prefix}.json"
    with open(jf, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    # CSV
    cf = outdir / f"{prefix}.csv"
    if rows:
        cols = sorted({k for r in rows for k in r.keys()})
        with open(cf, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)

    print(f"Đã lưu: {jf} / {cf}")

def main():
    max_pages = int(os.getenv("LAZADA_MAX_PAGES", "20"))
    sleep_base = float(os.getenv("LAZADA_SLEEP_BASE", "1.2"))

    results: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        for cat, base_url in CATEGORIES.items():
            try:
                rows = crawl_category(p, cat, base_url, max_pages=max_pages, sleep_base=sleep_base)
                results.extend(rows)
            except Exception as e:
                print(f"[{cat}] lỗi: {e}")
                continue

    # loại trùng toàn cục theo url
    uniq, seen = [], set()
    for r in results:
        u = r.get("url")
        if u and u not in seen:
            seen.add(u)
            uniq.append(r)

    print(f"\nTỔNG UNIQUE: {len(uniq)}")
    if uniq:
        save_outputs(uniq, prefix="lazada_listing_playwright")

if __name__ == "__main__":
    main()
