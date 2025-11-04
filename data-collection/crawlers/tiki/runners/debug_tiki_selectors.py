#!/usr/bin/env python3
"""
Debug Tiki Selectors - Analyze Tiki DOM structure
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """Setup Chrome driver with anti-detection"""
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def analyze_tiki_structure():
    """Analyze Tiki page structure"""
    driver = setup_driver()

    try:
        print("Loading Tiki electronics page...")
        url = "https://tiki.vn/dien-thoai-smartphone/c1795"
        driver.get(url)

        # Wait for page load
        time.sleep(5)

        # Scroll to load products
        driver.execute_script("window.scrollTo(0, 2000);")
        time.sleep(3)

        print("\n=== ANALYZING TIKI DOM STRUCTURE ===")

        # Try different product selectors
        selectors_to_test = [
            "[data-qa='product-item']",
            ".product-item",
            "[data-view-id='product_list_item']",
            ".ProductItem",
            ".product-box-list .product-item",
            "[data-view-id]",
            ".style__StyledItem",
            ".product-item-inner",
            ".item"
        ]

        for selector in selectors_to_test:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"Selector '{selector}': {len(elements)} elements found")

                if elements and len(elements) > 5:
                    # Analyze first element structure
                    first_element = elements[0]
                    print(f"  First element HTML (first 200 chars):")
                    html = first_element.get_attribute('outerHTML')[:200]
                    print(f"  {html}...")

                    # Try to find links
                    links = first_element.find_elements(By.TAG_NAME, "a")
                    print(f"  Links found: {len(links)}")
                    if links:
                        href = links[0].get_attribute('href')
                        print(f"  First link: {href}")

                    # Try to find text content
                    text = first_element.text[:100]
                    print(f"  Text content: {text}...")
                    print()

            except Exception as e:
                print(f"Selector '{selector}': Error - {e}")

        # Get page source snippet
        print("\n=== PAGE SOURCE ANALYSIS ===")
        page_source = driver.page_source

        # Look for specific patterns
        patterns = [
            'data-qa=',
            'data-view-id=',
            'product-item',
            'ProductItem',
            'product-box'
        ]

        for pattern in patterns:
            count = page_source.count(pattern)
            print(f"Pattern '{pattern}': {count} occurrences")

        print("\nPage title:", driver.title)
        print("Current URL:", driver.current_url)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    analyze_tiki_structure()