#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def debug_fptshop_pagination():
    """Debug FPTShop pagination structure"""

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    session = requests.Session()
    session.headers.update(headers)

    # Test main category page
    url = "https://fptshop.com.vn/dien-thoai"
    print(f"Testing: {url}")

    try:
        response = session.get(url, timeout=10)
        print(f"Status: {response.status_code}")
        print(f"Content length: {len(response.text)}")

        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for different types of product links
        print("\n=== Product Link Analysis ===")

        # Method 1: Direct product links
        product_links = soup.find_all('a', href=True)
        dien_thoai_links = [a for a in product_links if '/dien-thoai/' in a.get('href', '')]
        print(f"Direct /dien-thoai/ links: {len(dien_thoai_links)}")

        # Show first few
        for i, link in enumerate(dien_thoai_links[:10]):
            href = link.get('href')
            text = link.get_text().strip()[:50]
            print(f"  {i+1}. {href} - {text}")

        # Method 2: Look for AJAX/API endpoints
        print(f"\n=== Checking for AJAX/API calls ===")
        script_tags = soup.find_all('script')
        api_patterns = [
            r'api[^"\']*product',
            r'/api/[^"\']*',
            r'ajax[^"\']*',
            r'load[^"\']*product'
        ]

        for script in script_tags:
            if script.string:
                for pattern in api_patterns:
                    matches = re.findall(pattern, script.string, re.IGNORECASE)
                    if matches:
                        print(f"Found API pattern: {matches}")

        # Method 3: Look for pagination elements
        print(f"\n=== Pagination Analysis ===")

        # Common pagination selectors
        pagination_selectors = [
            '.pagination',
            '.page-nav',
            '.fs-pagination',
            'nav[aria-label*="page"]',
            '[class*="page"]',
            '[class*="Pagination"]'
        ]

        for selector in pagination_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"Found pagination with {selector}: {len(elements)} elements")
                for elem in elements[:3]:
                    print(f"  Content: {elem.get_text().strip()[:100]}")

        # Method 4: Look for "Load More" or infinite scroll
        print(f"\n=== Load More / Infinite Scroll ===")
        load_more_patterns = [
            'load.*more',
            'view.*more',
            'show.*more',
            'xem.*them',
            'tai.*them'
        ]

        for pattern in load_more_patterns:
            elements = soup.find_all(text=re.compile(pattern, re.IGNORECASE))
            if elements:
                print(f"Found '{pattern}' text: {len(elements)} occurrences")

        # Method 5: Check for category sub-pages or filters
        print(f"\n=== Category Structure ===")

        # Look for brand filters or subcategories
        brand_links = soup.find_all('a', href=True)
        brand_patterns = [
            'iphone', 'samsung', 'xiaomi', 'oppo', 'vivo'
        ]

        for pattern in brand_patterns:
            matching_links = [a for a in brand_links if pattern in a.get('href', '').lower()]
            if matching_links:
                print(f"{pattern.capitalize()} links: {len(matching_links)}")
                for link in matching_links[:3]:
                    print(f"  {link.get('href')}")

        # Method 6: Check actual HTML structure
        print(f"\n=== HTML Structure Sample ===")

        # Look for product containers
        container_selectors = [
            '.ProductCard',
            '.product-item',
            '.fs-pitem',
            '[class*="Product"]',
            '[class*="item"]'
        ]

        for selector in container_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"Product containers with {selector}: {len(elements)}")
                if elements:
                    first_elem = elements[0]
                    print(f"  Sample classes: {first_elem.get('class')}")
                    links = first_elem.find_all('a', href=True)
                    for link in links[:2]:
                        print(f"    Link: {link.get('href')}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_fptshop_pagination()