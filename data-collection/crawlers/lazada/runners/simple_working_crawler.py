#!/usr/bin/env python3
"""Simple working crawler that actually gets products from Lazada"""

import time
import json
import random
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class SimpleWorkingCrawler:
    def __init__(self):
        self.base_url = "https://www.lazada.vn"

    def create_stealth_driver(self):
        """Create a stealth Chrome driver to avoid detection"""
        options = Options()

        # Basic stealth options
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Real user agent
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        # Window size
        options.add_argument('--window-size=1366,768')

        driver = webdriver.Chrome(options=options)

        # Remove automation indicators
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def test_simple_search(self, search_term="iphone"):
        """Test simple search to see if we can get products"""
        print(f"Testing simple search for: {search_term}")

        driver = self.create_stealth_driver()
        products = []

        try:
            # Go to search page
            search_url = f"https://www.lazada.vn/catalog/?q={search_term}"
            print(f"Loading: {search_url}")

            driver.get(search_url)
            time.sleep(5)

            print(f"Page title: {driver.title}")
            print(f"Current URL: {driver.current_url}")

            # Check if we got blocked
            if "_____tmd_____" in driver.current_url or "punish" in driver.current_url:
                print("❌ Got blocked by anti-bot")
                return []

            # Try to find products with multiple selectors
            selectors_to_try = [
                '[data-qa-locator="product-item"]',
                '.c16H9d',
                '[class*="item"]',
                '.gridItem',
                '.product-item',
                '[data-spm*="product"]'
            ]

            product_elements = []
            for selector in selectors_to_try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    print(f"✅ Found {len(elements)} products with selector: {selector}")
                    product_elements = elements[:10]  # Take first 10
                    break

            if not product_elements:
                print("❌ No product elements found with any selector")
                return []

            # Extract basic info from found products
            for i, element in enumerate(product_elements):
                try:
                    # Try to get product name
                    name = "N/A"
                    name_selectors = ['[title]', 'img[alt]', 'a[title]']
                    for name_sel in name_selectors:
                        try:
                            name_elem = element.find_element(By.CSS_SELECTOR, name_sel)
                            name = name_elem.get_attribute('title') or name_elem.get_attribute('alt') or name_elem.text
                            if name and len(name) > 5:
                                break
                        except:
                            continue

                    # Try to get price
                    price = "N/A"
                    price_selectors = ['[class*="price"]', 'span[class*="currency"]']
                    for price_sel in price_selectors:
                        try:
                            price_elem = element.find_element(By.CSS_SELECTOR, price_sel)
                            price_text = price_elem.text.strip()
                            if price_text and ('₫' in price_text or 'đ' in price_text):
                                price = price_text
                                break
                        except:
                            continue

                    # Try to get product URL
                    url = "N/A"
                    try:
                        link_elem = element.find_element(By.CSS_SELECTOR, 'a[href]')
                        href = link_elem.get_attribute('href')
                        if href:
                            url = href
                    except:
                        pass

                    if name != "N/A" and name != "":
                        product = {
                            "source": "simple_lazada_test",
                            "product_name": name,
                            "price": price,
                            "url": url,
                            "crawl_date": datetime.now().isoformat()
                        }
                        products.append(product)
                        print(f"  {i+1}. {name[:50]}... | {price}")

                except Exception as e:
                    print(f"  {i+1}. Error extracting product: {e}")
                    continue

        except Exception as e:
            print(f"Error during crawling: {e}")

        finally:
            driver.quit()

        return products

    def test_category_page(self, category_url):
        """Test a category page"""
        print(f"Testing category: {category_url}")

        driver = self.create_stealth_driver()
        products = []

        try:
            driver.get(category_url)
            time.sleep(3)

            print(f"Page title: {driver.title}")
            print(f"Current URL: {driver.current_url}")

            # Check if blocked
            if "_____tmd_____" in driver.current_url or "punish" in driver.current_url:
                print("❌ Category page blocked")
                return []

            # Scroll to load content
            driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(2)

            # Look for product links
            product_links = []
            link_selectors = [
                'a[href*="/products/"]',
                'a[href*="-i"]',
                'a[data-spm*="product"]'
            ]

            for selector in link_selectors:
                links = driver.find_elements(By.CSS_SELECTOR, selector)
                for link in links:
                    href = link.get_attribute('href')
                    if href and href not in product_links:
                        product_links.append(href)
                        if len(product_links) >= 5:  # Just test 5
                            break
                if product_links:
                    break

            print(f"Found {len(product_links)} product links")

            # Test extracting from first few product pages
            for i, product_url in enumerate(product_links[:3]):
                try:
                    print(f"  Testing product {i+1}: {product_url[:60]}...")
                    driver.get(product_url)
                    time.sleep(2)

                    # Extract basic product info
                    name = "N/A"
                    try:
                        name_elem = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.TAG_NAME, "h1"))
                        )
                        name = name_elem.text.strip()
                    except:
                        pass

                    price = "N/A"
                    try:
                        price_elem = driver.find_element(By.CSS_SELECTOR, '[class*="price"]:not([class*="original"])')
                        price = price_elem.text.strip()
                    except:
                        pass

                    if name != "N/A" and len(name) > 5:
                        product = {
                            "source": "category_test",
                            "product_name": name,
                            "price": price,
                            "url": product_url,
                            "crawl_date": datetime.now().isoformat()
                        }
                        products.append(product)
                        print(f"    ✅ {name[:40]}... | {price}")
                    else:
                        print(f"    ❌ Could not extract product info")

                except Exception as e:
                    print(f"    ❌ Error: {e}")
                    continue

        except Exception as e:
            print(f"Category test error: {e}")

        finally:
            driver.quit()

        return products

    def test_seeds_urls(self):
        """Test all URLs from seeds.txt"""
        print("=== TESTING ALL SEED URLs ===")

        # Load seeds
        seeds_file = os.path.join(os.path.dirname(__file__), 'seeds.txt')
        seed_urls = {}

        try:
            with open(seeds_file, 'r', encoding='utf-8') as f:
                current_category = None
                for line in f:
                    line = line.strip()
                    if line.startswith('#'):
                        current_category = line[1:].strip()
                    elif line.startswith('http') and current_category:
                        seed_urls[current_category] = line
        except Exception as e:
            print(f"Error loading seeds: {e}")
            return

        print(f"Loaded {len(seed_urls)} seed URLs")

        all_products = []

        # Test first 3 categories
        for category, url in list(seed_urls.items())[:3]:
            print(f"\n--- Testing: {category} ---")
            products = self.test_category_page(url)
            all_products.extend(products)

            if products:
                print(f"✅ {category}: {len(products)} products extracted")
            else:
                print(f"❌ {category}: No products extracted")

            # Wait between categories
            time.sleep(3)

        # Save results
        if all_products:
            filename = f"simple_test_results_{int(time.time())}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_products, f, ensure_ascii=False, indent=2)
            print(f"\n✅ Saved {len(all_products)} products to {filename}")

        return all_products

def main():
    """Run simple tests"""
    crawler = SimpleWorkingCrawler()

    print("=== SIMPLE LAZADA CRAWLER TEST ===\n")

    # Test 1: Simple search
    print("1. Testing simple search...")
    search_products = crawler.test_simple_search("iphone")
    if search_products:
        print(f"✅ Search test: {len(search_products)} products found")
    else:
        print("❌ Search test: No products found")

    time.sleep(5)

    # Test 2: Test seed URLs
    print("\n2. Testing seed URLs...")
    seed_products = crawler.test_seeds_urls()

    # Summary
    total_products = len(search_products) + len(seed_products)
    print(f"\n=== TEST SUMMARY ===")
    print(f"Search results: {len(search_products)} products")
    print(f"Seed results: {len(seed_products)} products")
    print(f"Total: {total_products} products")

    if total_products > 0:
        print("✅ CRAWLER CAN GET PRODUCTS!")
    else:
        print("❌ No products extracted - need to adjust approach")

if __name__ == "__main__":
    main()