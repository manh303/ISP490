#!/usr/bin/env python3
"""
Test Pagination Crawler - Validate enhanced multi-platform crawler with pagination
"""

import time
import json
import os
import re
import random
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

class TestPaginationCrawler:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Setup test driver"""
        try:
            self.driver = uc.Chrome()
            return True
        except Exception as e:
            print(f"Driver setup failed: {e}")
            return False

    def log(self, message):
        """Simple logging"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

    def build_lazada_page_url(self, base_url, page_number):
        """Test Lazada URL building"""
        try:
            parsed = urlparse(base_url)
            query_params = parse_qs(parsed.query)
            query_params['page'] = [str(page_number)]
            new_query = urlencode(query_params, doseq=True)
            new_parsed = parsed._replace(query=new_query)
            return urlunparse(new_parsed)
        except:
            separator = '&' if '?' in base_url else '?'
            if 'page=' in base_url:
                return re.sub(r'page=\d+', f'page={page_number}', base_url)
            else:
                return f"{base_url}{separator}page={page_number}"

    def test_lazada_pagination(self):
        """Test Lazada pagination URL building"""
        self.log("Testing Lazada pagination URL building...")

        test_cases = [
            {
                'base': 'https://www.lazada.vn/catalog/?from=hp_categories&q=Mobiles&service=all_channel&spm=a2o4n.pdp_revamp.cate_1.1.60f21bf7X5rw1s&src=all_channel',
                'expected_pattern': 'page='
            },
            {
                'base': 'https://www.lazada.vn/dien-thoai-di-dong/',
                'expected_pattern': 'page='
            }
        ]

        for i, case in enumerate(test_cases):
            for page in [1, 2, 3]:
                url = self.build_lazada_page_url(case['base'], page)
                self.log(f"Test {i+1}, Page {page}: {url}")

                # Validate URL format
                if case['expected_pattern'] in url and f'page={page}' in url:
                    self.log(f"SUCCESS: URL format correct for page {page}")
                else:
                    self.log(f"ERROR: URL format incorrect for page {page}")

    def test_actual_lazada_access(self):
        """Test actual access to Lazada pages"""
        self.log("Testing actual Lazada page access...")

        if not self.setup_driver():
            return

        try:
            # Test simple category
            base_url = "https://www.lazada.vn/dien-thoai-di-dong/"

            for page in [1, 2]:
                page_url = self.build_lazada_page_url(base_url, page)
                self.log(f"Testing page {page}: {page_url}")

                self.driver.get(page_url)
                time.sleep(5)

                # Check for anti-bot
                current_url = self.driver.current_url
                if "punish" in current_url or "captcha" in current_url:
                    self.log(f"ERROR: Anti-bot detected on page {page}")
                    continue

                # Find products
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-qa-locator="product-item"]')
                self.log(f"SUCCESS: Found {len(product_elements)} products on page {page}")

                if len(product_elements) > 0:
                    # Test enhanced data extraction
                    sample_product = product_elements[0]

                    # Test price extraction
                    try:
                        price_elem = sample_product.find_element(By.CSS_SELECTOR, '.ooOxS')
                        price_text = price_elem.text
                        self.log(f"SUCCESS: Price found: {price_text}")
                    except:
                        self.log("ERROR: Price extraction failed")

                    # Test discount extraction
                    try:
                        discount_elem = sample_product.find_element(By.CSS_SELECTOR, '.IcOsH')
                        discount_text = discount_elem.text
                        self.log(f"SUCCESS: Discount found: {discount_text}")
                    except:
                        self.log("INFO: No discount found (normal)")

                    # Test rating extraction
                    try:
                        rating_elem = sample_product.find_element(By.CSS_SELECTOR, '.qzqFw')
                        rating_text = rating_elem.text
                        self.log(f"SUCCESS: Rating found: {rating_text}")
                    except:
                        self.log("INFO: No rating found")

                    # Test location extraction
                    try:
                        location_elem = sample_product.find_element(By.CSS_SELECTOR, '.oa6ri')
                        location_text = location_elem.text
                        self.log(f"SUCCESS: Location found: {location_text}")
                    except:
                        self.log("INFO: No location found")

                time.sleep(3)  # Delay between pages

        finally:
            if self.driver:
                self.driver.quit()

    def test_tiki_pagination(self):
        """Test Tiki pagination"""
        self.log("Testing Tiki pagination...")

        base_url = "https://tiki.vn/dien-thoai-smartphone/c1795"

        for page in [1, 2, 3]:
            separator = '&' if '?' in base_url else '?'
            if 'page=' in base_url:
                url = re.sub(r'page=\d+', f'page={page}', base_url)
            else:
                url = f"{base_url}{separator}page={page}"

            self.log(f"Tiki Page {page}: {url}")

    def run_validation_tests(self):
        """Run all validation tests"""
        self.log("Starting Enhanced Multi-Platform Crawler Validation")
        self.log("=" * 60)

        # Test 1: URL Building
        self.test_lazada_pagination()
        self.log("")

        # Test 2: Tiki URL Building
        self.test_tiki_pagination()
        self.log("")

        # Test 3: Actual Access (careful - might trigger anti-bot)
        self.log("WARNING: About to test actual page access - this might trigger anti-bot")
        user_input = input("Continue with actual access test? (y/n): ")
        if user_input.lower() == 'y':
            self.test_actual_lazada_access()

        self.log("Validation tests completed!")

if __name__ == "__main__":
    tester = TestPaginationCrawler()
    tester.run_validation_tests()