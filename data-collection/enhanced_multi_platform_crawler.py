#!/usr/bin/env python3
"""
Enhanced Multi-Platform Crawler - Production Ready
Combines working Lazada and Tiki crawlers with accurate selectors
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
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

class EnhancedMultiPlatformCrawler:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'products_with_discounts': 0,
            'products_with_ratings': 0,
            'pages_crawled': 0,
            'start_time': datetime.now(),
            'lazada_products': 0,
            'tiki_products': 0,
            'categories_crawled': []
        }

    def setup_driver(self):
        """Setup undetected ChromeDriver"""
        try:
            self.driver = uc.Chrome()
            return True
        except Exception as e:
            print(f"Undetected ChromeDriver failed: {e}")

            # Fallback to regular ChromeDriver
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return True

    def log(self, message):
        """Unicode-safe logging"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            safe_message = message.encode('ascii', 'ignore').decode('ascii')
            print(f"[{timestamp}] {safe_message}")
        except:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [LOG MESSAGE]")

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text with Unicode handling"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text

            if text:
                text = text.strip()
                # Clean Vietnamese characters for console compatibility
                text = text.replace('\u20ab', ' VND')
                text = text.replace('\u1ed1', 'o')
                text = text.replace('\u0110', 'D')
                text = text.replace('\u1ec7', 'e')
                return text
            return ""
        except:
            return ""

    def wait_and_scroll(self, platform):
        """Platform-specific wait and scroll"""
        try:
            if platform == 'lazada':
                time.sleep(5)
                for i in range(4):
                    scroll_position = (i + 1) * 800
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                    time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
            else:  # tiki
                time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, 1000);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, 2000);")
                time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, 3000);")
                time.sleep(1)
        except:
            pass

    def build_lazada_page_url(self, base_url, page_number):
        """Build Lazada paginated URL with proper format"""
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

    def build_tiki_page_url(self, base_url, page_number):
        """Build Tiki paginated URL"""
        try:
            # Tiki uses simple page parameter
            separator = '&' if '?' in base_url else '?'
            if 'page=' in base_url:
                return re.sub(r'page=\d+', f'page={page_number}', base_url)
            else:
                return f"{base_url}{separator}page={page_number}"
        except:
            return base_url

    def extract_lazada_price(self, product_element):
        """Extract Lazada price"""
        try:
            price_selectors = [
                'span[class*="price"]',
                'div[class*="price"]',
                '[class*="ooOxS"]',
                '[class*="aBrP0"]'
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = self.safe_text_extract(elem)
                        if price_text and ('VND' in price_text or '₫' in price_text or price_text.replace(',', '').isdigit()):
                            clean_text = re.sub(r'[^\d]', '', price_text)
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 1000 <= price <= 100000000:
                                    return price
                except:
                    continue
            return 0
        except:
            return 0

    def extract_tiki_price(self, product_element):
        """Extract Tiki price"""
        try:
            price_selectors = [
                ".price-current",
                ".price",
                ".current-price",
                ".style__CurrentPrice",
                ".price-discount__price"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = self.safe_text_extract(elem)
                        if price_text:
                            clean_text = re.sub(r'[^\d]', '', price_text)
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 1000 <= price <= 100000000:
                                    return price
                except:
                    continue
            return 0
        except:
            return 0

    def extract_lazada_product(self, product_link, index):
        """Extract Lazada product from link"""
        try:
            product = {'platform': 'lazada'}

            # URL
            url = product_link.get_attribute('href')
            if not url or '/products/' not in url:
                return None

            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://www.lazada.vn' + url

            product['url'] = url

            # ID
            id_match = re.search(r'pdp-i(\d+)', url)
            if id_match:
                product['id'] = id_match.group(1)
            else:
                id_match = re.search(r'i(\d+)', url)
                product['id'] = id_match.group(1) if id_match else f"lazada_{index}"

            # Name
            title = product_link.get_attribute('title')
            if title and len(title) > 10:
                product['name'] = self.safe_text_extract(product_link, 'title')
            else:
                text = self.safe_text_extract(product_link)
                if text and len(text) > 10:
                    product['name'] = text
                else:
                    try:
                        img = product_link.find_element(By.TAG_NAME, 'img')
                        alt_text = img.get_attribute('alt')
                        if alt_text and len(alt_text) > 10:
                            product['name'] = self.safe_text_extract(img, 'alt')
                        else:
                            product['name'] = f"Lazada Product {index}"
                    except:
                        product['name'] = f"Lazada Product {index}"

            # Price
            try:
                parent = product_link.find_element(By.XPATH, '..')
                for _ in range(3):
                    price = self.extract_lazada_price(parent)
                    if price > 0:
                        product['price'] = price
                        break
                    parent = parent.find_element(By.XPATH, '..')
                else:
                    product['price'] = 0
            except:
                product['price'] = 0

            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Lazada product extraction error: {str(e)}")
            return None

    def extract_enhanced_lazada_product(self, product_element, index):
        """Extract enhanced Lazada product data with new fields"""
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

            # Price extraction - using real selectors
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

            # Discount information
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

            # Sold count
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

            # Rating information
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

            # Location
            location = None
            try:
                location_elem = product_element.find_element(By.CSS_SELECTOR, '.oa6ri')
                location = self.safe_text_extract(location_elem, 'title') or self.safe_text_extract(location_elem)
            except:
                pass

            product['location'] = location

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
            self.log(f"Enhanced Lazada product extraction error: {str(e)}")
            return None

    def extract_tiki_product(self, product_element, index):
        """Extract Tiki product (FIXED version)"""
        try:
            product = {'platform': 'tiki'}

            # URL - element is the <a> tag itself
            url = product_element.get_attribute("href")
            if url:
                if url.startswith('//'):
                    url = 'https:' + url
                elif url.startswith('/'):
                    url = 'https://tiki.vn' + url
                product['url'] = url

                # ID
                id_match = re.search(r'p(\d+)', url)
                if id_match:
                    product['id'] = id_match.group(1)
                else:
                    spid_match = re.search(r'spid=(\d+)', url)
                    product['id'] = spid_match.group(1) if spid_match else f"tiki_{index}"
            else:
                return None

            # Name
            name_selectors = [".name", ".product-name", "img[alt]"]
            product_name = ""

            for selector in name_selectors:
                try:
                    name_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                    if selector == "img[alt]":
                        product_name = self.safe_text_extract(name_elem, 'alt')
                    else:
                        product_name = self.safe_text_extract(name_elem)

                    if product_name and len(product_name) > 5:
                        break
                except:
                    continue

            product['name'] = product_name if product_name else f"Tiki Product {index}"

            # Price
            product['price'] = self.extract_tiki_price(product_element)

            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Tiki product extraction error: {str(e)}")
            return None

    def crawl_lazada_pages(self, base_url, category_name, max_pages=3):
        """Crawl multiple Lazada pages"""
        self.log(f"Crawling Lazada {category_name} - {max_pages} pages")
        all_products = []

        for page_num in range(1, max_pages + 1):
            try:
                # Build page URL
                page_url = self.build_lazada_page_url(base_url, page_num)
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

                # Random delay between pages to avoid detection
                time.sleep(random.uniform(3, 6))

            except Exception as e:
                self.log(f"Error crawling page {page_num}: {str(e)}")
                continue

        self.log(f"Lazada {category_name}: Total {len(all_products)} products from {max_pages} pages")
        return all_products

    def crawl_tiki_pages(self, base_url, category_name, max_pages=3):
        """Crawl multiple Tiki pages with pagination and load more handling"""
        self.log(f"Crawling Tiki {category_name} - {max_pages} pages")
        all_products = []

        for page_num in range(1, max_pages + 1):
            try:
                # Build page URL
                page_url = self.build_tiki_page_url(base_url, page_num)
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
                        product = self.extract_tiki_product(element, i)
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

                # Try to handle "Load More" button if on first page
                if page_num == 1:
                    try:
                        # Look for load more button
                        load_more_selectors = [
                            'button:contains("Xem thêm")',
                            'button[class*="show-more"]',
                            'button[class*="load-more"]',
                            '.show-more-button'
                        ]

                        for selector in load_more_selectors:
                            try:
                                if ':contains(' in selector:
                                    # Skip JavaScript selectors for now
                                    continue
                                load_more_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                                if load_more_btn.is_displayed():
                                    self.log("Found load more button, clicking...")
                                    load_more_btn.click()
                                    time.sleep(3)

                                    # Get additional products after load more
                                    new_elements = self.driver.find_elements(By.CSS_SELECTOR, '.product-item')
                                    additional_products = []

                                    # Extract only new products (skip already processed ones)
                                    for i, element in enumerate(new_elements[len(product_elements):], len(product_elements) + 1):
                                        try:
                                            product = self.extract_tiki_product(element, i)
                                            if product and product.get('url'):
                                                additional_products.append(product)
                                        except:
                                            continue

                                    all_products.extend(additional_products)
                                    self.results['tiki_products'] += len(additional_products)
                                    self.log(f"Load more: {len(additional_products)} additional products")
                                    break
                            except:
                                continue
                    except Exception as e:
                        self.log(f"Load more handling failed: {str(e)}")

                # Random delay between pages
                time.sleep(random.uniform(2, 4))

            except Exception as e:
                self.log(f"Error crawling Tiki page {page_num}: {str(e)}")
                continue

        self.log(f"Tiki {category_name}: Total {len(all_products)} products from {max_pages} pages")
        return all_products

    def run_comprehensive_crawl(self):
        """Run comprehensive multi-platform crawl"""
        self.log("Starting Enhanced Multi-Platform Crawler")

        try:
            if not self.setup_driver():
                self.log("Driver setup failed")
                return

            # Define working URLs
            categories = {
                'lazada': {
                    'mobiles': 'https://www.lazada.vn/catalog/?spm=a2o4n.pdp_revamp.cate_1.1.71295d00xk3zIs&q=Mobiles&from=hp_categories&src=all_channel',
                    'laptops': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.3.651c7812lHRQZ4&q=Laptops&from=hp_categories&src=all_channel',
                    'tablets': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.2.447a2aa22Nh6OK&q=Tablets&from=hp_categories&src=all_channel',
                    'destops_computers':'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.4.290d3f1agMS5VO&q=Desktops%20Computers&from=hp_categories&src=all_channel',
                    'audio': 'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.5.5a1b6634I1885g&q=Audio&from=hp_categories&src=all_channel',
                    'cameras':'https://www.lazada.vn/catalog/?spm=a2o4n.searchlist.cate_1.6.1a7d3ca5mp2A2S&q=Security%20Cameras%20%26%20Systems&from=hp_categories&src=all_channel'

                },
                'tiki': {
                    'smartphones': 'https://tiki.vn/dien-thoai-smartphone/c1795',
                    'laptops': 'https://tiki.vn/laptop/c1846',
                    'tablets': 'https://tiki.vn/may-tinh-bang/c1883',
                    'may_doc_sach': 'https://tiki.vn/may-doc-sach/c28856',
                    'dien_thoai_pho thong': 'https://tiki.vn/dien-thoai-pho-thong/c1796',
                    'headphones': 'https://tiki.vn/tai-nghe-co-day/c1804'
                }
            }

            all_products = []

            # Crawl Lazada categories with pagination
            self.log("=== LAZADA CRAWLING WITH PAGINATION ===")
            for category_name, url in categories['lazada'].items():
                self.log(f"Starting Lazada {category_name}")
                try:
                    products = self.crawl_lazada_pages(url, category_name, max_pages=3)
                    all_products.extend(products)
                    self.results['categories_crawled'].append(f"lazada_{category_name}")

                    # Break between categories
                    time.sleep(random.uniform(5, 8))

                except Exception as e:
                    self.log(f"Error crawling Lazada {category_name}: {str(e)}")
                    continue

            # Crawl Tiki categories with pagination
            self.log("=== TIKI CRAWLING WITH PAGINATION ===")
            for category_name, url in categories['tiki'].items():
                self.log(f"Starting Tiki {category_name}")
                try:
                    products = self.crawl_tiki_pages(url, category_name, max_pages=3)
                    all_products.extend(products)
                    self.results['categories_crawled'].append(f"tiki_{category_name}")

                    # Break between categories
                    time.sleep(random.uniform(3, 5))

                except Exception as e:
                    self.log(f"Error crawling Tiki {category_name}: {str(e)}")
                    continue

            self.results['total_products'] = len(all_products)

            # Save results
            if all_products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/enhanced_multi_platform_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

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
                    'discount_rate': (self.results['products_with_discounts'] / len(all_products) * 100) if all_products else 0,
                    'rating_rate': (self.results['products_with_ratings'] / len(all_products) * 100) if all_products else 0
                })

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'metadata': results_copy,
                        'products': all_products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Sample results
                self.log("=== SAMPLE PRODUCTS ===")
                lazada_products = [p for p in all_products if p['platform'] == 'lazada']
                tiki_products = [p for p in all_products if p['platform'] == 'tiki']

                if lazada_products:
                    sample_lazada = lazada_products[0]
                    self.log(f"Lazada: {sample_lazada['name'][:50]}... - Price: {sample_lazada['price']}")

                if tiki_products:
                    sample_tiki = tiki_products[0]
                    self.log(f"Tiki: {sample_tiki['name'][:50]}... - Price: {sample_tiki['price']}")

            # Enhanced Final statistics
            self.log("=== ENHANCED FINAL RESULTS ===")
            self.log(f"Total products: {self.results['total_products']}")
            self.log(f"Pages crawled: {self.results['pages_crawled']}")
            self.log(f"Categories crawled: {len(self.results['categories_crawled'])}")
            self.log(f"Lazada products: {self.results['lazada_products']}")
            self.log(f"Tiki products: {self.results['tiki_products']}")
            self.log(f"Products with prices: {self.results['products_with_prices']}")
            self.log(f"Products with discounts: {self.results['products_with_discounts']}")
            self.log(f"Products with ratings: {self.results['products_with_ratings']}")

            if all_products:
                products_with_images = sum(1 for p in all_products if p.get('images'))
                products_with_location = sum(1 for p in all_products if p.get('location'))

                self.log(f"Products with images: {products_with_images}")
                self.log(f"Products with location: {products_with_location}")

                price_percentage = (self.results['products_with_prices'] / len(all_products)) * 100
                discount_percentage = (self.results['products_with_discounts'] / len(all_products)) * 100
                rating_percentage = (self.results['products_with_ratings'] / len(all_products)) * 100

                self.log(f"Price extraction success rate: {price_percentage:.1f}%")
                self.log(f"Discount extraction rate: {discount_percentage:.1f}%")
                self.log(f"Rating extraction rate: {rating_percentage:.1f}%")

                # Duration
                duration = datetime.now() - self.results['start_time']
                self.log(f"Total crawl duration: {duration}")

                # Show sample enhanced product
                enhanced_products = [p for p in all_products if p.get('discount') or p.get('rating_average')]
                if enhanced_products:
                    sample = enhanced_products[0]
                    self.log("=== SAMPLE ENHANCED PRODUCT ===")
                    self.log(f"Name: {sample['name'][:50]}...")
                    self.log(f"Price: {sample['price']:,} VND")
                    self.log(f"Discount: {sample.get('discount', 'None')}")
                    self.log(f"Rating: {sample.get('rating_average', 'N/A')} ({sample.get('rating_count', 'N/A')} reviews)")
                    self.log(f"Location: {sample.get('location', 'N/A')}")
                    self.log(f"Sold: {sample.get('sold_count', 'N/A')}")

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

if __name__ == "__main__":
    crawler = EnhancedMultiPlatformCrawler()
    crawler.run_comprehensive_crawl()