#!/usr/bin/env python3
"""Debug script to examine Lazada page structure and find correct selectors"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def setup_debug_driver():
    """Setup Chrome driver for debugging"""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    return webdriver.Chrome(options=chrome_options)

def debug_lazada_page(url):
    """Debug a Lazada page to find correct selectors"""
    print(f"=== DEBUGGING LAZADA PAGE ===")
    print(f"URL: {url}")

    driver = setup_debug_driver()

    try:
        print("Loading page...")
        driver.get(url)
        time.sleep(5)

        print(f"Page title: {driver.title}")
        print(f"Current URL: {driver.current_url}")

        # Check page source for clues
        page_source = driver.page_source
        print(f"Page source length: {len(page_source)} characters")

        # Try different selectors for product links
        selectors_to_try = [
            'a[href*="/products/"]',
            'a[href*="lazada.vn/"]',
            '.item',
            '.product',
            '[data-qa-locator="product-item"]',
            '.c16H9d',  # Common Lazada class
            '.card-product',
            'a[href*="-i"]',  # Lazada product URLs contain -i followed by ID
            '.gridItem',
            '.product-card'
        ]

        for selector in selectors_to_try:
            print(f"\nTrying selector: {selector}")
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            print(f"  Found {len(elements)} elements")

            if elements:
                for i, element in enumerate(elements[:3]):  # Check first 3
                    try:
                        href = element.get_attribute('href')
                        text = element.text[:50] if element.text else ""
                        print(f"    {i+1}. href: {href}")
                        print(f"       text: {text}")
                    except:
                        print(f"    {i+1}. Error getting element info")

        # Check for specific Lazada elements
        print(f"\n=== CHECKING LAZADA SPECIFIC ELEMENTS ===")

        # Look for search results
        search_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-locator*="search"]')
        print(f"Search elements found: {len(search_elements)}")

        # Look for product containers
        product_containers = driver.find_elements(By.CSS_SELECTOR, '[class*="product"], [class*="item"]')
        print(f"Product containers found: {len(product_containers)}")

        # Look for links containing product info
        all_links = driver.find_elements(By.TAG_NAME, 'a')
        product_links = [link for link in all_links if link.get_attribute('href') and 'lazada.vn' in link.get_attribute('href')]
        print(f"Total Lazada links found: {len(product_links)}")

        # Check if it's an actual product listing page
        if 'catalog' in driver.current_url or 'search' in driver.current_url:
            print("✅ This appears to be a product listing page")
        else:
            print("❌ This doesn't appear to be a product listing page")

        # Save screenshot for manual inspection
        screenshot_path = "debug_lazada_page.png"
        driver.save_screenshot(screenshot_path)
        print(f"Screenshot saved to: {screenshot_path}")

    except Exception as e:
        print(f"Debug error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        driver.quit()

def main():
    """Test multiple seed URLs"""
    seed_urls = [
        "https://www.lazada.vn/catalog/?from=hp_categories&page=2&q=Mobiles&service=all_channel&spm=a2o4n.searchlist.cate_1.1.46343f1abl2X9U&src=all_channel",
        "https://www.lazada.vn/catalog/?from=hp_categories&page=2&q=Laptops&service=all_channel&spm=a2o4n.searchlist.cate_1.3.69767812j4Hxbo&src=all_channel"
    ]

    for i, url in enumerate(seed_urls, 1):
        print(f"\n{'='*80}")
        print(f"TESTING SEED URL {i}")
        debug_lazada_page(url)
        if i < len(seed_urls):
            print(f"\nWaiting 5 seconds before next test...")
            time.sleep(5)

if __name__ == "__main__":
    main()