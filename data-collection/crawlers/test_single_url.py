#!/usr/bin/env python3
"""Test a single Lazada URL to see what's actually on the page"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    return webdriver.Chrome(options=chrome_options)

def test_url(url):
    print(f"Testing URL: {url}")

    driver = setup_driver()

    try:
        driver.get(url)
        time.sleep(5)

        print(f"Page title: {driver.title}")
        print(f"Current URL: {driver.current_url}")

        # Check if redirected to anti-bot page
        if "_____tmd_____" in driver.current_url or "punish" in driver.current_url:
            print("❌ REDIRECTED TO ANTI-BOT PAGE")
            return False

        # Check for product elements
        all_links = driver.find_elements(By.TAG_NAME, 'a')
        product_links = []

        for link in all_links:
            href = link.get_attribute('href')
            if href and ('product' in href.lower() or '-i' in href):
                product_links.append(href)

        print(f"✅ Page loaded successfully")
        print(f"Total links: {len(all_links)}")
        print(f"Product-like links: {len(product_links)}")

        if product_links:
            print("Sample product links:")
            for i, link in enumerate(product_links[:5]):
                print(f"  {i+1}. {link}")

        # Check for common Lazada elements
        elements_found = {}
        test_selectors = [
            '.item',
            '.product',
            '[data-qa-locator*="product"]',
            '.c16H9d',
            '[class*="item"]',
            '[data-spm*="product"]',
            'a[href*="-i"]'
        ]

        for selector in test_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            elements_found[selector] = len(elements)
            if len(elements) > 0:
                print(f"✅ Found {len(elements)} elements with: {selector}")

        # Save page source for inspection
        with open('page_source.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print("Page source saved to page_source.html")

        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        driver.quit()

def main():
    # Test different categories
    test_urls = [
        "https://www.lazada.vn/dien-thoai-may-tinh-bang/",
        "https://www.lazada.vn/may-tinh-laptop/",
        "https://www.lazada.vn/dien-thoai-may-tinh-bang/?page=1"
    ]

    for url in test_urls:
        print(f"\n{'='*80}")
        test_url(url)
        time.sleep(3)

if __name__ == "__main__":
    main()