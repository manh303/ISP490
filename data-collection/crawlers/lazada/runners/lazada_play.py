# lazada_play.py
import os, re, json, time, random
from typing import List, Dict
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

ANTI_PATTERNS = [
    "are you human", "captcha", "security verification",
    "access denied", "forbidden",
    "tạm thời không thể truy cập", "xác minh bảo mật"
]

def _random_sleep(a=0.8, b=2.0):
    time.sleep(random.uniform(a, b))

def _is_blocked(html: str) -> bool:
    low = (html or "").lower()
    return any(p in low for p in ANTI_PATTERNS)

def _extract_from_html(html: str) -> List[Dict]:
    """
    Fallback: một số trang Lazada nhúng JSON lớn (mods/listItems) trong HTML.
    Ta cố gắng bắt các khối JSON có 'listItems' hoặc 'mods'.
    """
    try_patterns = [
        r'"listItems"\s*:\s*(\[[^\]]+\])',
        r'"mods"\s*:\s*({.*?})\s*,\s*"mainInfo"',
        r'"mods"\s*:\s*({.*})\s*,"filters"',   # biến thể khác
    ]
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    txt = soup.get_text(" ", strip=False)
    for pat in try_patterns:
        m = re.search(pat, txt, flags=re.S)
        if not m:
            m = re.search(pat, html, flags=re.S)
        if m:
            blob = m.group(1)
            try:
                data = json.loads(blob)
                if isinstance(data, dict) and "listItems" in data:
                    return data["listItems"]
                if isinstance(data, list):
                    return data
            except Exception:
                pass
    return []

def _productize(obj: Dict) -> Dict:
    """Chuẩn hóa vài field thường gặp từ JSON listing của Lazada."""
    out = {}
    # Tên
    out["name"] = obj.get("name") or obj.get("itemTitle") or obj.get("title")
    # Giá
    price = obj.get("price") or obj.get("itemPrice") or obj.get("priceShow")
    if isinstance(price, str):
        # lấy số
        price_num = re.sub(r"[^\d.]", "", price)
        out["price"] = float(price_num) if price_num else None
    else:
        out["price"] = price
    # Shop / seller
    out["seller"] = obj.get("sellerName") or obj.get("seller") or obj.get("seller_name")
    # Rating
    out["rating"] = obj.get("ratingScore") or obj.get("rating") or obj.get("itemRating")
    # Link & productId
    out["url"] = obj.get("productUrl") or obj.get("itemUrl") or obj.get("productUrlPath")
    if out["url"] and out["url"].startswith("//"):
        out["url"] = "https:" + out["url"]
    out["productId"] = obj.get("itemId") or obj.get("productId") or obj.get("nid")
    return out

def fetch_lazada_listing(url: str, max_tries: int = 5) -> List[Dict]:
    """
    Mở listing Lazada và TRÍCH JSON TỪ NETWORK:
    - Chặn tải ảnh/font để nhẹ & giảm dấu vết
    - Giả lập UA/Accept-Language
    - Đợi response JSON có 'listItems'/'mods'; nếu không có, fallback parse HTML
    """
    ua = os.getenv("LAZADA_UA", 
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36")
    proxy = os.getenv("LAZADA_HTTP_PROXY")  # ví dụ http://user:pass@host:port
    is_headless = os.getenv("LAZADA_HEADLESS", "1") != "0"

    def _predicate(resp):
        # Chỉ lấy JSON từ domain lazada / acs-m / gw api
        try:
            ctype = resp.headers.get("content-type", "")
            url_l = resp.url.lower()
            if "application/json" not in ctype:
                return False
            if not any(d in url_l for d in [
                "lazada.", "acs-", "mtop", "/catalog", "/search", "/item", "/pdp"
            ]):
                return False
            # Thỏa mãn, để phía dưới đọc JSON
            return True
        except Exception:
            return False

    for attempt in range(1, max_tries + 1):
        with sync_playwright() as p:
            launch_args = ["--no-sandbox", "--disable-dev-shm-usage",
                           "--disable-gpu", "--disable-software-rasterizer"]
            browser = p.chromium.launch(headless=is_headless, args=launch_args)
            context_kwargs = {
                "user_agent": ua,
                "locale": "vi-VN",
                "java_script_enabled": True,
                "bypass_csp": True,
            }
            if proxy:
                context_kwargs["proxy"] = {"server": proxy}

            context = browser.new_context(**context_kwargs)
            # Chặn tài nguyên nặng
            context.route(
                "**/*",
                lambda r: r.abort()
                if re.search(r"\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf)(\?|$)", r.request.url, flags=re.I)
                else r.continue_()
            )
            page = context.new_page()
            page.set_default_navigation_timeout(60000)
            page.set_default_timeout(60000)

            # Header thêm
            page.set_extra_http_headers({
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
            })

            try:
                page.goto(url, wait_until="domcontentloaded")
                _random_sleep(1.2, 2.2)

                # chặn bot sơ bộ
                html = page.content()
                if _is_blocked(html):
                    raise RuntimeError("Blocked by anti-bot (HTML)")

                # đợi 1–3 response JSON hợp lệ rồi hợp nhất
                products = []
                got_one = False
                t_end = time.time() + 15  # chờ tối đa 15s
                while time.time() < t_end and len(products) < 50:  # limit để tránh quá nhiều
                    try:
                        resp = page.wait_for_response(_predicate, timeout=3000)
                        ctype = resp.headers.get("content-type", "")
                        if "application/json" in ctype:
                            data = resp.json()
                            blob = json.dumps(data).lower()
                            if "listitems" in blob or '"mods"' in blob:
                                # Thử gom listItems
                                # 1) trực tiếp: data["mods"]["listItems"]
                                try:
                                    li = data["mods"]["listItems"]
                                    products.extend(li)
                                    got_one = True
                                except Exception:
                                    pass
                                # 2) deep search thô nếu cấu trúc khác
                                txt = json.dumps(data)
                                m = re.search(r'"listItems"\s*:\s*(\[[^\]]+\])', txt, re.S)
                                if m:
                                    try:
                                        products.extend(json.loads(m.group(1)))
                                        got_one = True
                                    except Exception:
                                        pass
                    except PWTimeout:
                        # không có thêm response đúng tiêu chí
                        pass

                # nếu chưa bắt được từ network → fallback parse HTML
                if not got_one:
                    html = page.content()
                    if _is_blocked(html):
                        raise RuntimeError("Blocked by anti-bot (HTML fallback)")
                    products = _extract_from_html(html)

                # chuẩn hóa
                norm = [_productize(x) for x in products if isinstance(x, dict)]
                # lọc những bản ghi có tên
                norm = [x for x in norm if x.get("name")]
                context.close()
                browser.close()

                if norm:
                    return norm
                else:
                    # có thể rate-limit → thử lại
                    raise RuntimeError("Empty products, retrying...")
            except Exception as e:
                try:
                    context.close()
                    browser.close()
                except Exception:
                    pass
                # backoff nhẹ + có thể đổi proxy/UA giữa các lần
                time.sleep(2 + attempt * 1.5)
                continue

    # hết lượt thử
    return []
