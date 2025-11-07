#!/usr/bin/env python3
"""
Debug Current Lazada Structure - Live DOM Analysis
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

def debug_lazada_structure():
    """Debug live Lazada page structure"""

    # Setup driver
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        # Use the exact URL from user
        url = "https://www.lazada.vn/catalog/?spm=a2o4n.searchlistcategory.cate_1.1.631635c0Sd9Eq6&q=Mobiles&from=hp_categories&src=all_channel"
        print(f"Loading: {url}")
        driver.get(url)

        # Wait and scroll
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, 2000);")
        time.sleep(3)

        print("\n=== DEBUGGING CURRENT LAZADA STRUCTURE ===")

        # Check for different product selectors
        selectors_to_test = [
            # From structure.html
            '.Bm3ON[data-qa-locator="product-item"]',
            '.Bm3ON',
            '[data-qa-locator="product-item"]',
            '[data-qa-locator="general-products"]',

            # Common product selectors
            '.product',
            '.product-item',
            '[class*="product"]',
            '[data-qa*="product"]',
            '[data-testid*="product"]',

            # Generic containers
            '[class*="item"]',
            '[class*="card"]',
            'article',
            '[role="article"]'
        ]

        for selector in selectors_to_test:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"Selector '{selector}': {len(elements)} elements found")

                if elements and len(elements) > 5:
                    # Analyze first element
                    first_element = elements[0]
                    print(f"  First element HTML (first 200 chars):")
                    html = first_element.get_attribute('outerHTML')[:200]
                    print(f"  {html}...")

                    # Look for links
                    links = first_element.find_elements(By.TAG_NAME, "a")
                    print(f"  Links found: {len(links)}")
                    if links:
                        href = links[0].get_attribute('href')
                        if href:
                            print(f"  First link: {href[:100]}...")

                    # Look for text content
                    text = first_element.text[:100] if first_element.text else ""
                    print(f"  Text content: {text}...")
                    print()

            except Exception as e:
                print(f"Selector '{selector}': Error - {e}")

        # Check page source for specific patterns
        print("\n=== PAGE SOURCE ANALYSIS ===")
        page_source = driver.page_source

        patterns = [
            'data-qa-locator',
            'product-item',
            'Bm3ON',
            'lazada',
            'item-id',
            'class=".*item.*"'
        ]

        for pattern in patterns:
            count = page_source.count(pattern)
            print(f"Pattern '{pattern}': {count} occurrences")

        print(f"\nPage title: {driver.title}")
        print(f"Current URL: {driver.current_url}")

        # Save page source for analysis
        with open("C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/current_page_source.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        print("Page source saved to: current_page_source.html")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    debug_lazada_structure()