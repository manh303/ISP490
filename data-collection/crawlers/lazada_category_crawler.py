#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, os, csv, json, time, random, argparse
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------- utils ----------
def text(x):
    return re.sub(r"\s+", " ", x.strip()) if x else ""

def to_int(s):
    if s is None: return None
    m = re.findall(r"\d+", s.replace(".", "").replace(",", ""))
    if not m: return None
    try: return int("".join(m))
    except: return None

def get_discount(p_cur, p_ori):
    if p_cur and p_ori and p_ori > 0 and p_cur <= p_ori:
        return round(100.0 * (p_ori - p_cur) / p_ori, 2)
    return None

def make_driver(headless: bool, user_agent: str | None = None):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,2200")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=vi-VN")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    service = ChromeService()                      # Selenium Manager
    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        })
    except Exception:
        pass
    return driver

def polite_sleep(a=1.0, b=2.3):
    time.sleep(random.uniform(a, b))

def smart_scroll(driver, min_rounds=3, max_rounds=10, pause=0.8):
    last = driver.execute_script("return document.body.scrollHeight")
    rounds = 0
    while rounds < max_rounds:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new = driver.execute_script("return document.body.scrollHeight")
        rounds += 1
        if new <= last and rounds >= min_rounds:
            break
        last = new

def parse_products_from_html(html, inferred_category=None):
    soup = BeautifulSoup(html, "lxml")
    candidates = list(soup.select('[data-qa-locator="product-item"]'))
    for a in soup.select("a[href]"):
        href = a.get("href","")
        if re.search(r"/products?/", href) or re.search(r"/catalog/", href):
            card = a
            for _ in range(4):
                if not card: break
                card = card.parent
            if card: candidates.append(card)
    seen, cards = set(), []
    for c in candidates:
        k = id(c)
        if k not in seen:
            seen.add(k); cards.append(c)

    items = []
    for card in cards:
        a = card.select_one("a[href]")
        url = a["href"] if a and a.has_attr("href") else None

        name = None
        for sel in [".Rf6ng",".buTCk","[data-qa-locator='product-title']","img[alt]","a[title]"]:
            el = card.select_one(sel)
            if el:
                cand = text(el.get("title") or el.get("alt") or el.get_text())
                if cand:
                    name = cand; break

        img = None
        img_el = card.select_one("img[src]") or card.select_one("img[data-src]")
        if img_el:
            img = img_el.get("src") or img_el.get("data-src")

        price_texts = []
        for sel in [".ooOxS",".aBrP0",".c13VH6",".c3gUW0","[data-qa-locator='product-price']",".price",".c1DXz4",".c1Wz2s",".c2prKC"]:
            for el in card.select(sel):
                t = text(el.get_text())
                if t and any(ch.isdigit() for ch in t):
                    price_texts.append(t)
        price_cands = []
        for t in price_texts:
            for n in re.findall(r"(?:[₫VND\\s]*)(\\d[\\d\\.,]*)", t):
                price_cands.append(n)
        price_current = to_int(price_cands[0]) if price_cands else None
        price_original = to_int(price_cands[1]) if len(price_cands) >= 2 else None
        strike = card.select_one("s, del, .c1hkC1")
        if strike:
            val = to_int(text(strike.get_text()))
            if val: price_original = val

        discount_percent = None
        disc_el = None
        for sel in [".ZkIOf",".IcOsH",".discount",".c1hkC1","[class*='discount']"]:
            disc_el = card.select_one(sel)
            if disc_el: break
        if disc_el:
            m = re.search(r"(\\d+(?:[.,]\\d+)?)\\s*%", disc_el.get_text())
            if m:
                try: discount_percent = float(m.group(1).replace(",", "."))
                except: discount_percent = None
        if discount_percent is None:
            discount_percent = get_discount(price_current, price_original)

        rating_avg = None
        aria = card.select_one("[aria-label*='Rated']")
        if aria:
            m = re.search(r"Rated\\s+(\\d+(?:[.,]\\d+)?)", aria.get("aria-label",""))
            if m:
                try: rating_avg = float(m.group(1).replace(",", "."))
                except: rating_avg = None

        rating_count = None
        count_text = ""
        for sel in [".qzqFw",".c3XbGJ",".c3KeDq","[class*='rating']"]:
            for el in card.select(sel):
                count_text += " " + el.get_text(" ")
        m = re.search(r"(\\d[\\d\\.,]*)(?:k)?\\s*(?:ratings|reviews|\\))", count_text, flags=re.I)
        if m:
            val = re.sub(r"[^\\d]", "", m.group(1))
            try: rating_count = int(val)
            except: rating_count = None

        sold_count = None
        txt = " ".join(card.stripped_strings)
        m = re.search(r"(\\d[\\d\\.,]*)\\s*(?:sold|đã\\s*bán)", txt, flags=re.I)
        if m:
            s = re.sub(r"[^\\d]", "", m.group(1))
            try: sold_count = int(s)
            except: sold_count = None

        seller_name = None
        seller_type = None
        if card.select_one("[data-qa-locator='lazmall']") or card.select_one("svg[aria-label*='LazMall']"):
            seller_type = "LazMall/Official"
        for sel in [".c16H9d",".c2uQn2","[data-qa-locator='seller-name']"]:
            el = card.select_one(sel)
            if el:
                seller_name = text(el.get_text()); break

        if (name or url):
            items.append({
                "source": "lazada",
                "product_name": name,
                "product_url": url,
                "image_url": img,
                "price_current": price_current,
                "price_original": price_original,
                "discount_percent": discount_percent,
                "rating_avg": rating_avg,
                "rating_count": rating_count,
                "sold_count": sold_count,
                "seller_name": seller_name,
                "seller_type": seller_type,
                "category": inferred_category,
                "crawl_date": datetime.utcnow().isoformat() + "Z"
            })

    out, seen2 = [], set()
    for it in items:
        k = (it.get("product_url"), it.get("product_name"))
        if k not in seen2:
            out.append(it); seen2.add(k)
    return out

def set_page(url: str, page_no: int) -> str:
    u = urlparse(url)
    qs = dict(parse_qsl(u.query, keep_blank_values=True))
    qs["page"] = str(page_no)
    new_q = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def infer_category_from_seed(seed_url: str) -> str:
    # ví dụ: https://www.lazada.vn/dong-ho-thong-minh/ -> "dong-ho-thong-minh"
    path = urlparse(seed_url).path.strip("/").split("/")
    return path[0] if path else None

def crawl(driver, seeds, out_prefix, max_pages=40):
    out, seen = [], set()
    for seed in seeds:
        if not seed or seed.startswith("#"):  # bỏ dòng comment
            continue
        cat = infer_category_from_seed(seed)
        print("[CAT]", seed, "->", cat)

        empty_streak = 0
        for p in range(1, max_pages + 1):
            page_url = set_page(seed, p)
            try:
                driver.get(page_url)
                WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
                smart_scroll(driver, min_rounds=3, max_rounds=10, pause=0.8)
                html = driver.page_source
                items = parse_products_from_html(html, inferred_category=cat)

                if not items:
                    empty_streak += 1
                    print(f"[PAGE] {page_url} -> 0 items (empty_streak={empty_streak})")
                    if empty_streak >= 2:
                        print("[STOP] 2 trang trống liên tiếp → dừng category")
                        break
                    continue
                else:
                    empty_streak = 0

                newc = 0
                for it in items:
                    k = (it.get("product_url"), it.get("product_name"))
                    if k not in seen:
                        seen.add(k); out.append(it); newc += 1
                print(f"[PAGE] {page_url} -> +{newc} (total {len(out)})")
                polite_sleep(1.2, 2.6)

            except Exception as e:
                print("[WARN] fetch/page failed:", page_url, e)
                break

    # ghi file
    fields = ["source","product_name","product_url","image_url","price_current","price_original","discount_percent",
              "rating_avg","rating_count","sold_count","seller_name","seller_type","category","crawl_date"]
    with open(f"{out_prefix}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for it in out: w.writerow(it)
    with open(f"{out_prefix}.jsonl", "w", encoding="utf-8") as f:
        for it in out: f.write(json.dumps(it, ensure_ascii=False) + "\n")
    print(f"[DONE] {len(out)} products")
    print(" -", f"{out_prefix}.csv")
    print(" -", f"{out_prefix}.jsonl")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", required=True, help="Đường dẫn seeds.txt (mỗi dòng 1 URL; cho phép #comment)")
    ap.add_argument("--out-prefix", default="lazada_all", help="Tiền tố file xuất")
    ap.add_argument("--max-pages", type=int, default=40, help="Số trang tối đa mỗi category")
    ap.add_argument("--headless", action="store_true", help="Chạy headless (không mở cửa sổ)")
    ap.add_argument("--user-agent", default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
    args = ap.parse_args()

    with open(args.seeds, "r", encoding="utf-8") as f:
        seed_urls = [line.strip() for line in f if line.strip()]

    driver = make_driver(headless=args.headless, user_agent=args.user_agent)
    try:
        crawl(driver, seed_urls, args.out_prefix, max_pages=args.max_pages)
    finally:
        try: driver.quit()
        except: pass

if __name__ == "__main__":
    main()
