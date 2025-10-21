#!/usr/bin/env python3
"""
Enhanced Multi-Platform Crawler V2 - Improved Pages Crawling & More Data Fields
Crawls multiple pages and extracts comprehensive product data including reviews
"""

import time
import json
import os
import re
import random
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

class EnhancedMultiPlatformCrawlerV2:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'products_with_discounts': 0,
            'products_with_ratings': 0,
            'products_with_reviews': 0,
            'pages_crawled': 0,
            'start_time': datetime.now(),
            'lazada_products': 0,
            'tiki_products': 0,
            'categories_crawled': []
        }

    def setup_driver(self):
        """Setup enhanced undetected ChromeDriver"""
        try:
            # Try undetected chromedriver first
            self.driver = uc.Chrome()
            return True
        except Exception as e:
            self.log(f"Undetected ChromeDriver failed: {e}")

            # Fallback to regular ChromeDriver
            try:
                options = Options()
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

                self.driver = webdriver.Chrome(options=options)
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                return True
            except Exception as e2:
                self.log(f"Regular ChromeDriver also failed: {e2}")
                return False

    def log(self, message):
        """Enhanced logging with Unicode support"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            # Convert to safe ASCII for console
            safe_message = str(message).encode('ascii', 'ignore').decode('ascii')
            print(f"[{timestamp}] {safe_message}")
        except:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [LOG MESSAGE]")

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text with enhanced Unicode handling"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text

            if text:
                text = text.strip()
                # Clean common Vietnamese characters for compatibility
                replacements = {
                    '\u20ab': ' VND',
                    '\u1ed1': 'o',
                    '\u0110': 'D',
                    '\u1ec7': 'e',
                    '\u1ea3': 'a',
                    '\u1eaf': 'a',
                    '\u1ee5': 'u'
                }
                for old, new in replacements.items():
                    text = text.replace(old, new)
                return text
            return ""
        except:
            return ""

    def wait_and_scroll(self, platform):
        """Enhanced platform-specific wait and scroll"""
        try:
            if platform == 'lazada':
                # Initial page load wait
                time.sleep(4)

                # Progressive scrolling to trigger lazy loading
                for i in range(5):
                    scroll_position = (i + 1) * 600
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                    time.sleep(1.5)

                # Scroll back to top
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

            else:  # tiki
                time.sleep(2)
                # Tiki specific scrolling
                for i in range(4):
                    scroll_position = (i + 1) * 800
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                    time.sleep(1)

        except Exception as e:
            self.log(f"Scroll error: {e}")

    def build_page_url(self, base_url, page_number):
        """Build paginated URL properly"""
        try:
            parsed = urlparse(base_url)
            query_params = parse_qs(parsed.query)

            # Update page parameter
            query_params['page'] = [str(page_number)]

            # Rebuild URL
            new_query = urlencode(query_params, doseq=True)
            new_parsed = parsed._replace(query=new_query)

            return urlunparse(new_parsed)
        except:
            # Fallback method
            separator = '&' if '?' in base_url else '?'
            if 'page=' in base_url:
                # Replace existing page parameter
                return re.sub(r'page=\d+', f'page={page_number}', base_url)
            else:
                # Add page parameter
                return f"{base_url}{separator}page={page_number}"

    def extract_enhanced_lazada_product(self, product_element, index):
        """Extract comprehensive Lazada product data from updated structure"""
        try:
            product = {'platform': 'lazada'}

            # Extract from data attributes (more reliable)
            data_item_id = product_element.get_attribute('data-item-id')
            if data_item_id:
                product['id'] = data_item_id
            else:
                product['id'] = f"lazada_{index}"

            # Product URL - multiple selectors
            url = None
            url_selectors = [
                'a[href*="/products/"]',
                'a[href*="pdp-i"]'
            ]

            for selector in url_selectors:
                try:
                    url_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                    url = url_elem.get_attribute('href')
                    if url:
                        break
                except:
                    continue

            if not url:
                return None

            # Handle relative URLs
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://www.lazada.vn' + url

            product['url'] = url

            # Product name - enhanced extraction
            name = ""
            name_selectors = [
                'a[title]',  # Title attribute is most reliable
                '.RfADt a',  # Product title link
                'img[alt]'   # Image alt text as fallback
            ]

            for selector in name_selectors:
                try:
                    elem = product_element.find_element(By.CSS_SELECTOR, selector)
                    if selector == 'a[title]':
                        name = self.safe_text_extract(elem, 'title')
                    elif selector == 'img[alt]':
                        name = self.safe_text_extract(elem, 'alt')
                    else:
                        name = self.safe_text_extract(elem)

                    if name and len(name) > 10:
                        break
                except:
                    continue

            product['name'] = name if name else f"Lazada Product {index}"

            # Price extraction - using real selectors from HTML
            price = 0
            price_selectors = [
                '.ooOxS',  # Main price class from HTML
                '.aBrP0 .ooOxS',  # Nested price
                'span[class*="price"]'
            ]

            for selector in price_selectors:
                try:
                    price_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                    price_text = self.safe_text_extract(price_elem)
                    if price_text and ('₫' in price_text or 'VND' in price_text):
                        # Extract numbers only
                        clean_price = re.sub(r'[^\d]', '', price_text)
                        if clean_price.isdigit() and len(clean_price) >= 4:
                            price_val = int(clean_price)
                            if 1000 <= price_val <= 100000000:
                                price = price_val
                                break
                except:
                    continue

            product['price'] = price
            if price > 0:
                self.results['products_with_prices'] += 1

            # Discount information - from HTML structure
            discount = None
            try:
                discount_elem = product_element.find_element(By.CSS_SELECTOR, '.IcOsH')
                discount_text = self.safe_text_extract(discount_elem)
                if discount_text and 'Off' in discount_text:
                    discount = discount_text
                    self.results['products_with_discounts'] += 1
            except:
                pass

            product['discount'] = discount

            # Sold count - from HTML structure
            sold_count = None
            try:
                sold_elem = product_element.find_element(By.CSS_SELECTOR, '._1cEkb span')
                sold_text = self.safe_text_extract(sold_elem)
                if sold_text and ('sold' in sold_text or 'Đã bán' in sold_text):
                    # Extract numbers
                    numbers = re.findall(r'[\d,\.]+', sold_text)
                    if numbers:
                        sold_str = numbers[0].replace(',', '').replace('.', '')
                        if sold_str.replace('K', '').replace('k', '').replace(',', '').isdigit():
                            if 'K' in sold_text or 'k' in sold_text:
                                sold_count = int(float(sold_str.replace('K', '').replace('k', '')) * 1000)
                            else:
                                sold_count = int(sold_str)
            except:
                pass

            product['sold_count'] = sold_count

            # Rating information - from HTML structure
            rating_average = None
            rating_count = None

            try:
                # Count filled stars
                filled_stars = product_element.find_elements(By.CSS_SELECTOR, '._9-ogB.Dy1nx')
                if filled_stars:
                    rating_average = len(filled_stars)
                    self.results['products_with_ratings'] += 1
            except:
                pass

            try:
                # Rating count in parentheses
                rating_count_elem = product_element.find_element(By.CSS_SELECTOR, '.qzqFw')
                rating_text = self.safe_text_extract(rating_count_elem)
                if rating_text:
                    # Extract number from parentheses like "(33)"
                    count_match = re.search(r'\((\d+)\)', rating_text)
                    if count_match:
                        rating_count = int(count_match.group(1))
            except:
                pass

            product['rating_average'] = rating_average
            product['rating_count'] = rating_count

            # Location - from HTML structure
            location = None
            try:
                location_elem = product_element.find_element(By.CSS_SELECTOR, '.oa6ri')
                location = self.safe_text_extract(location_elem, 'title') or self.safe_text_extract(location_elem)
            except:
                pass

            product['location'] = location

            # Images - extract multiple images
            images = []
            try:
                img_elements = product_element.find_elements(By.CSS_SELECTOR, 'img[src]')
                for img in img_elements:
                    src = img.get_attribute('src')
                    if src and ('lazcdn' in src or 'alicdn' in src):
                        images.append(src)
                        if len(images) >= 3:  # Limit to 3 images
                            break
            except:
                pass

            product['images'] = images

            # Additional metadata
            product.update({
                'category': 'Electronics',
                'crawl_time': datetime.now().isoformat(),
                'page_position': index,
                'has_discount': discount is not None,
                'has_rating': rating_average is not None
            })

            return product

        except Exception as e:
            self.log(f"Lazada product extraction error: {str(e)}")
            return None

    def extract_enhanced_tiki_product(self, product_element, index):
        """Extract enhanced Tiki product data"""
        try:
            product = {'platform': 'tiki'}

            # URL - Tiki product links
            url = None
            try:
                if product_element.tag_name == 'a':
                    url = product_element.get_attribute('href')
                else:
                    url_elem = product_element.find_element(By.CSS_SELECTOR, 'a[href]')
                    url = url_elem.get_attribute('href')

                if url:
                    if url.startswith('//'):
                        url = 'https:' + url
                    elif url.startswith('/'):
                        url = 'https://tiki.vn' + url
                    product['url'] = url

                    # Extract ID
                    id_match = re.search(r'p(\d+)', url)
                    if id_match:
                        product['id'] = id_match.group(1)
                    else:
                        spid_match = re.search(r'spid=(\d+)', url)
                        product['id'] = spid_match.group(1) if spid_match else f"tiki_{index}"
                else:
                    return None
            except:
                return None

            # Name extraction
            name = ""
            name_selectors = [
                '.name',
                '.product-name',
                'h3',
                'img[alt]'
            ]

            for selector in name_selectors:
                try:
                    elem = product_element.find_element(By.CSS_SELECTOR, selector)
                    if selector == 'img[alt]':
                        name = self.safe_text_extract(elem, 'alt')
                    else:
                        name = self.safe_text_extract(elem)

                    if name and len(name) > 5:
                        break
                except:
                    continue

            product['name'] = name if name else f"Tiki Product {index}"

            # Price extraction
            price = 0
            price_selectors = [
                '.price-current',
                '.price',
                '.current-price',
                '.style__CurrentPrice',
                '.price-discount__price'
            ]

            for selector in price_selectors:
                try:
                    price_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                    price_text = self.safe_text_extract(price_elem)
                    if price_text:
                        clean_text = re.sub(r'[^\d]', '', price_text)
                        if clean_text.isdigit() and len(clean_text) >= 4:
                            price_val = int(clean_text)
                            if 1000 <= price_val <= 100000000:
                                price = price_val
                                break
                except:
                    continue

            product['price'] = price
            if price > 0:
                self.results['products_with_prices'] += 1

            # Additional Tiki-specific fields
            product.update({
                'category': 'Electronics',
                'crawl_time': datetime.now().isoformat(),
                'page_position': index,
                'discount': None,
                'sold_count': None,
                'rating_average': None,
                'rating_count': None,
                'location': None,
                'images': []
            })

            return product

        except Exception as e:
            self.log(f"Tiki product extraction error: {str(e)}")
            return None

    def crawl_lazada_pages(self, base_url, category_name, max_pages=5):
        """Crawl multiple Lazada pages"""
        self.log(f"Crawling Lazada {category_name} - {max_pages} pages")
        all_products = []

        for page_num in range(1, max_pages + 1):
            try:
                # Build page URL
                page_url = self.build_page_url(base_url, page_num)
                self.log(f"Page {page_num}: {page_url}")

                # Navigate to page
                self.driver.get(page_url)
                self.wait_and_scroll('lazada')

                # Check for anti-bot protection
                current_url = self.driver.current_url
                if "punish" in current_url or "captcha" in current_url:
                    self.log(f"Anti-bot detected on page {page_num}, skipping...")
                    time.sleep(10)  # Wait before trying next page
                    continue

                # Find products using updated selector
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-qa-locator="product-item"]')
                self.log(f"Found {len(product_elements)} products on page {page_num}")

                if not product_elements:
                    self.log(f"No products found on page {page_num}, stopping pagination")
                    break

                # Extract products
                page_products = []
                for i, element in enumerate(product_elements, 1):
                    try:
                        product = self.extract_enhanced_lazada_product(element, i)
                        if product and product.get('url'):
                            page_products.append(product)

                        # Progress logging
                        if i % 20 == 0:
                            self.log(f"Processed {i}/{len(product_elements)} products")

                    except Exception as e:
                        self.log(f"Error processing product {i}: {str(e)}")
                        continue

                    # Small delay every 10 products
                    if i % 10 == 0:
                        time.sleep(0.5)

                all_products.extend(page_products)
                self.results['lazada_products'] += len(page_products)
                self.results['pages_crawled'] += 1

                self.log(f"Page {page_num}: {len(page_products)} products extracted")

                # Random delay between pages
                time.sleep(random.uniform(3, 6))

            except Exception as e:
                self.log(f"Error crawling page {page_num}: {str(e)}")
                continue

        self.log(f"Lazada {category_name}: Total {len(all_products)} products from {max_pages} pages")
        return all_products

    def crawl_tiki_pages(self, base_url, category_name, max_pages=3):
        """Crawl multiple Tiki pages"""
        self.log(f"Crawling Tiki {category_name} - {max_pages} pages")
        all_products = []

        for page_num in range(1, max_pages + 1):
            try:
                # Build page URL
                page_url = self.build_page_url(base_url, page_num)
                self.log(f"Page {page_num}: {page_url}")

                # Navigate to page
                self.driver.get(page_url)
                self.wait_and_scroll('tiki')

                # Find products
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, '.product-item')
                self.log(f"Found {len(product_elements)} Tiki products on page {page_num}")

                if not product_elements:
                    self.log(f"No products found on page {page_num}, stopping pagination")
                    break

                # Extract products
                page_products = []
                for i, element in enumerate(product_elements, 1):
                    try:
                        product = self.extract_enhanced_tiki_product(element, i)
                        if product and product.get('url'):
                            page_products.append(product)

                        if i % 20 == 0:
                            self.log(f"Processed {i}/{len(product_elements)} Tiki products")

                    except Exception as e:
                        continue

                    if i % 10 == 0:
                        time.sleep(0.5)

                all_products.extend(page_products)
                self.results['tiki_products'] += len(page_products)
                self.results['pages_crawled'] += 1

                self.log(f"Page {page_num}: {len(page_products)} products extracted")

                # Random delay between pages
                time.sleep(random.uniform(2, 4))

            except Exception as e:
                self.log(f"Error crawling Tiki page {page_num}: {str(e)}")
                continue

        self.log(f"Tiki {category_name}: Total {len(all_products)} products from {max_pages} pages")
        return all_products

    def run_enhanced_crawl(self, pages_per_category=3):
        """Run enhanced multi-platform crawl with multiple pages"""
        self.log("Starting Enhanced Multi-Platform Crawler V2")

        try:
            if not self.setup_driver():
                self.log("Driver setup failed")
                return

            # Enhanced category definitions with more categories
            categories = {
                'lazada': {
                    'security_cameras': {
                        'url': 'https://www.lazada.vn/catalog/?from=hp_categories&q=Security%20Cameras%20%26%20Systems&service=all_channel&spm=a2o4n.searchlist.cate_1.6.1a7d3ca5mp2A2S&src=all_channel',
                        'pages': pages_per_category
                    },
                    'mobiles': {
                        'url': 'https://www.lazada.vn/catalog/?spm=a2o4n.pdp_revamp.cate_1.1.71295d00xk3zIs&q=Mobiles&from=hp_categories&src=all_channel',
                        'pages': pages_per_category
                    },
                    'laptops': {
                        'url': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.3.651c7812lHRQZ4&q=Laptops&from=hp_categories&src=all_channel',
                        'pages': pages_per_category
                    },
                    'tablets': {
                        'url': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.2.447a2aa22Nh6OK&q=Tablets&from=hp_categories&src=all_channel',
                        'pages': pages_per_category
                    },
                    'desktops': {
                        'url': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.4.290d3f1agMS5VO&q=Desktops%20Computers&from=hp_categories&src=all_channel',
                        'pages': pages_per_category
                    },
                    'audio': {
                        'url': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.5.5a1b6634I1885g&q=Audio&from=hp_categories&src=all_channel',
                        'pages': pages_per_category
                    }
                },
                'tiki': {
                    'smartphones': {
                        'url': 'https://tiki.vn/dien-thoai-smartphone/c1795',
                        'pages': pages_per_category
                    },
                    'laptops': {
                        'url': 'https://tiki.vn/laptop/c1846',
                        'pages': pages_per_category
                    },
                    'tablets': {
                        'url': 'https://tiki.vn/may-tinh-bang/c1883',
                        'pages': pages_per_category
                    }
                }
            }

            all_products = []

            # Crawl Lazada categories
            self.log("=== LAZADA CRAWLING ===")
            for category_name, category_info in categories['lazada'].items():
                self.log(f"Starting Lazada {category_name}")
                try:
                    products = self.crawl_lazada_pages(
                        category_info['url'],
                        category_name,
                        category_info['pages']
                    )
                    all_products.extend(products)
                    self.results['categories_crawled'].append(f"lazada_{category_name}")

                    # Break between categories
                    time.sleep(random.uniform(5, 8))

                except Exception as e:
                    self.log(f"Error crawling Lazada {category_name}: {str(e)}")
                    continue

            # Crawl Tiki categories
            self.log("=== TIKI CRAWLING ===")
            for category_name, category_info in categories['tiki'].items():
                self.log(f"Starting Tiki {category_name}")
                try:
                    products = self.crawl_tiki_pages(
                        category_info['url'],
                        category_name,
                        category_info['pages']
                    )
                    all_products.extend(products)
                    self.results['categories_crawled'].append(f"tiki_{category_name}")

                    # Break between categories
                    time.sleep(random.uniform(3, 5))

                except Exception as e:
                    self.log(f"Error crawling Tiki {category_name}: {str(e)}")
                    continue

            # Update final results
            self.results['total_products'] = len(all_products)

            # Save comprehensive results
            if all_products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/enhanced_multi_platform_v2_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                # Prepare results
                results_copy = self.results.copy()
                results_copy['start_time'] = self.results['start_time'].isoformat()
                results_copy['end_time'] = datetime.now().isoformat()
                results_copy['duration'] = str(datetime.now() - self.results['start_time'])

                # Calculate additional statistics
                products_with_images = sum(1 for p in all_products if p.get('images'))
                products_with_location = sum(1 for p in all_products if p.get('location'))

                results_copy.update({
                    'products_with_images': products_with_images,
                    'products_with_location': products_with_location,
                    'price_success_rate': (self.results['products_with_prices'] / len(all_products) * 100) if all_products else 0,
                    'discount_rate': (self.results['products_with_discounts'] / len(all_products) * 100) if all_products else 0
                })

                # Save data
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'metadata': results_copy,
                        'products': all_products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Display comprehensive summary
                self.log("=== COMPREHENSIVE RESULTS ===")
                self.log(f"Total products: {self.results['total_products']}")
                self.log(f"Pages crawled: {self.results['pages_crawled']}")
                self.log(f"Lazada products: {self.results['lazada_products']}")
                self.log(f"Tiki products: {self.results['tiki_products']}")
                self.log(f"Products with prices: {self.results['products_with_prices']}")
                self.log(f"Products with discounts: {self.results['products_with_discounts']}")
                self.log(f"Products with ratings: {self.results['products_with_ratings']}")
                self.log(f"Products with images: {products_with_images}")
                self.log(f"Products with location: {products_with_location}")

                if all_products:
                    price_rate = (self.results['products_with_prices'] / len(all_products)) * 100
                    self.log(f"Price extraction success rate: {price_rate:.1f}%")

                # Sample products display
                self.log("=== SAMPLE PRODUCTS ===")
                for platform in ['lazada', 'tiki']:
                    platform_products = [p for p in all_products if p['platform'] == platform]
                    if platform_products:
                        sample = platform_products[0]
                        self.log(f"{platform.title()}: {sample['name'][:50]}...")
                        self.log(f"  Price: {sample['price']:,} VND")
                        self.log(f"  Rating: {sample.get('rating_average', 'N/A')}")
                        self.log(f"  Discount: {sample.get('discount', 'None')}")
                        self.log(f"  Location: {sample.get('location', 'N/A')}")

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

if __name__ == "__main__":
    # Run with 2 pages per category for testing
    crawler = EnhancedMultiPlatformCrawlerV2()
    crawler.run_enhanced_crawl(pages_per_category=2)