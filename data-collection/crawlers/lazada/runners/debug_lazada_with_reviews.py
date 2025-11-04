#!/usr/bin/env python3
"""
Debug Lazada Crawler with Reviews
Simplified version for testing and debugging
"""

import time
import json
import logging
from datetime import datetime
from pathlib import Path
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DebugLazadaCrawler:
    def __init__(self):
        self.driver = None

    def setup_stealth_driver(self):
        """Setup stealth driver to avoid detection"""
        try:
            logger.info("Setting up stealth driver...")

            # Try undetected chromedriver first
            try:
                self.driver = uc.Chrome(
                    options=uc.ChromeOptions()
                )
                logger.info("SUCCESS: Undetected ChromeDriver initialized")
                return True
            except Exception as e:
                logger.warning(f"Undetected ChromeDriver failed: {e}")

                # Fallback to regular ChromeDriver with stealth options
                options = Options()
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

                self.driver = webdriver.Chrome(options=options)
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                logger.info("SUCCESS: Regular ChromeDriver with stealth options")
                return True

        except Exception as e:
            logger.error(f"Driver setup failed: {e}")
            return False

    def test_page_access(self, url):
        """Test if we can access a Lazada page"""
        try:
            logger.info(f"Testing page access: {url}")
            self.driver.get(url)
            time.sleep(5)

            # Check current URL for redirects
            current_url = self.driver.current_url
            logger.info(f"Current URL: {current_url}")

            # Check for anti-bot detection
            if "punish" in current_url or "captcha" in current_url:
                logger.warning("DETECTED: Anti-bot protection!")
                return False

            # Save page source for analysis
            page_source = self.driver.page_source
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            debug_file = f"debug_page_source_{timestamp}.html"

            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(page_source)
            logger.info(f"Page source saved to: {debug_file}")

            # Try to find product elements
            selectors_to_try = [
                "[data-qa-locator='product-item']",
                ".product-item",
                ".item",
                "a[href*='/products/']",
                "[class*='product']"
            ]

            for selector in selectors_to_try:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    logger.info(f"Selector '{selector}': Found {len(elements)} elements")
                    if elements:
                        # Try to extract basic info from first element
                        elem = elements[0]
                        text = elem.text[:100] if elem.text else "No text"
                        href = elem.get_attribute('href') if elem.tag_name == 'a' else "No href"
                        logger.info(f"Sample element - Text: {text}... | Href: {href}")
                except Exception as e:
                    logger.debug(f"Selector '{selector}' failed: {e}")

            return True

        except Exception as e:
            logger.error(f"Page access test failed: {e}")
            return False

    def test_product_detail_access(self, product_url):
        """Test accessing a specific product detail page"""
        try:
            logger.info(f"Testing product detail access: {product_url}")
            self.driver.get(product_url)
            time.sleep(5)

            current_url = self.driver.current_url
            logger.info(f"Product page URL: {current_url}")

            # Look for review-related elements
            review_selectors = [
                '.reviews-container',
                '.product-reviews',
                '.review-list',
                '[data-qa-locator="reviews"]',
                '.ugc-review-list',
                '[class*="review"]',
                '[class*="rating"]'
            ]

            for selector in review_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    logger.info(f"Review selector '{selector}': Found {len(elements)} elements")
                    if elements:
                        sample_text = elements[0].text[:100] if elements[0].text else "No text"
                        logger.info(f"Sample review element: {sample_text}...")
                except Exception as e:
                    logger.debug(f"Review selector '{selector}' failed: {e}")

            # Try JavaScript to find review elements
            js_script = """
            var reviewElements = document.querySelectorAll('[class*="review"], [class*="ugc"], [data-qa*="review"]');
            return reviewElements.length;
            """
            js_result = self.driver.execute_script(js_script)
            logger.info(f"JavaScript review search found: {js_result} elements")

            return True

        except Exception as e:
            logger.error(f"Product detail test failed: {e}")
            return False

    def run_debug_test(self):
        """Run comprehensive debug test"""
        logger.info("Starting Lazada Debug Test")

        try:
            if not self.setup_stealth_driver():
                logger.error("Driver setup failed")
                return

            # Test 1: Category page access
            category_url = "https://www.lazada.vn/dien-thoai-di-dong/"
            success = self.test_page_access(category_url)

            if success:
                logger.info("✓ Category page access successful")

                # Test 2: Try to find a product URL for detailed testing
                try:
                    product_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/products/']")
                    if product_links:
                        product_url = product_links[0].get_attribute('href')
                        logger.info(f"Found product URL for testing: {product_url}")

                        # Test 3: Product detail page access
                        self.test_product_detail_access(product_url)
                    else:
                        logger.warning("No product links found on category page")

                        # Test with a known product URL
                        test_product_url = "https://www.lazada.vn/products/dien-thoai-iphone-15-pro-max-128gb-chinh-hang-vn-a-i3856789523.html"
                        logger.info("Testing with a known product URL...")
                        self.test_product_detail_access(test_product_url)

                except Exception as e:
                    logger.error(f"Product link extraction failed: {e}")
            else:
                logger.error("✗ Category page access failed")

        except Exception as e:
            logger.error(f"Debug test failed: {e}")

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

if __name__ == "__main__":
    crawler = DebugLazadaCrawler()
    crawler.run_debug_test()