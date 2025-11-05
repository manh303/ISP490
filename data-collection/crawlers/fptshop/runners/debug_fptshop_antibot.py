#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import json
import sys

def test_fptshop_antibot():
    url = 'https://fptshop.com.vn/dien-thoai/xiaomi-15t'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print('=== Testing HTTP Request ===')
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f'Status: {response.status_code}')
        print(f'Content length: {len(response.text)}')

        # Check for anti-bot indicators
        content_lower = response.text.lower()
        if 'cloudflare' in content_lower or 'captcha' in content_lower or 'challenge' in content_lower:
            print('ANTI-BOT: Cloudflare/CAPTCHA detected')
        else:
            print('OK: Normal response')

        # Check for price elements in raw HTML
        if 'Price_currentPrice' in response.text or 'price' in content_lower:
            print('OK: Price elements found in HTML')
        else:
            print('ISSUE: Price elements NOT found in HTML')

    except Exception as e:
        print(f'HTTP Error: {e}')

    print('\n=== Testing Selenium ===')
    driver = None
    try:
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        print('Loading page...')
        driver.get(url)
        time.sleep(5)

        # Check page content
        page_source = driver.page_source
        print(f'Page length: {len(page_source)}')

        # Check for anti-bot protection
        page_lower = page_source.lower()
        if 'cloudflare' in page_lower or 'captcha' in page_lower or 'challenge' in page_lower:
            print('ANTI-BOT: Protection detected in Selenium')
        else:
            print('OK: Page loaded normally in Selenium')

        # Check page title
        try:
            title = driver.title
            print(f'Page title: {title}')
        except:
            print('Could not get page title')

        # Test multiple price selectors
        price_selectors = [
            'p.Price_currentPrice__PBYcv',
            '.Price_currentPrice__PBYcv',
            '.fs-dtprice-special',
            '.price-current',
            '[class*="Price"]',
            '[class*="price"]',
            '.ProductCard_price',
            '.ProductPrice',
            '*[data-price]'
        ]

        found_price = False
        for selector in price_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for elem in elements[:3]:
                        text = elem.text.strip()
                        if text and len(text) > 0:
                            print(f'FOUND PRICE with {selector}: {text}')
                            found_price = True
                            break
                    if found_price:
                        break
            except Exception as e:
                continue

        if not found_price:
            print('ISSUE: No price elements found with any selector')

            # Debug: Show all elements with 'price' or 'Price' in class name
            try:
                price_elements = driver.find_elements(By.CSS_SELECTOR, '[class*="price"], [class*="Price"]')
                print(f'Found {len(price_elements)} elements with price in class:')
                for i, elem in enumerate(price_elements[:10]):
                    try:
                        class_name = elem.get_attribute('class')
                        text = elem.text.strip()[:50]
                        print(f'  {i+1}. {elem.tag_name}.{class_name} -> {text}')
                    except:
                        pass
            except Exception as e:
                print(f'Debug search failed: {e}')

        # Check if page is JavaScript-heavy and needs more time
        try:
            scripts = driver.find_elements(By.TAG_NAME, 'script')
            print(f'Found {len(scripts)} script tags - page is JavaScript heavy')
        except:
            pass

    except Exception as e:
        print(f'Selenium error: {e}')
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    test_fptshop_antibot()