#!/usr/bin/env python3
import time
import json
import logging
import random
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.robotparser import RobotFileParser

class BaseCrawler:
    def __init__(self, source_name: str, base_url: str, delay_range: tuple = (2, 5)):
        self.source_name = source_name
        self.base_url = base_url
        self.delay_range = delay_range
        self.session = requests.Session()
        self.driver = None
        self.logger = self._setup_logger()
        self.robots_parser = self._load_robots_txt()

        # Set up user agent
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]

    def _setup_logger(self) -> logging.Logger:
        import os
        logger = logging.getLogger(f"{self.source_name}_crawler")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Create logs directory if it doesn't exist
            log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
            os.makedirs(log_dir, exist_ok=True)

            log_file = os.path.join(log_dir, f'{self.source_name}_crawler.log')
            handler = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            # Also log to console
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def _load_robots_txt(self) -> Optional[RobotFileParser]:
        try:
            robots_url = urljoin(self.base_url, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp
        except Exception as e:
            self.logger.warning(f"Could not load robots.txt for {self.base_url}: {e}")
            return None

    def can_crawl(self, url: str) -> bool:
        if self.robots_parser:
            return self.robots_parser.can_fetch('*', url)
        return True

    def get_random_user_agent(self) -> str:
        return random.choice(self.user_agents)

#https://cellphones.com.vn/mobile.html
#https://cellphones.com.vn/tablet.html
#https://cellphones.com.vn/laptop.html
#https://cellphones.com.vn/thiet-bi-am-thanh.html
#https://cellphones.com.vn/thiet-bi-am-thanh/micro-thu-am.html
#https://cellphones.com.vn/do-choi-cong-nghe.html
#https://cellphones.com.vn/phu-kien/camera.html
#https://cellphones.com.vn/do-gia-dung.html
#https://cellphones.com.vn/nha-thong-minh/suc-khoe-lam-dep.html
#https://cellphones.com.vn/phu-kien.html
#https://cellphones.com.vn/may-tinh-de-ban.html
#https://cellphones.com.vn/man-hinh.html
#https://cellphones.com.vn/may-in.html
#https://cellphones.com.vn/tivi.html
#https://cellphones.com.vn/dien-may.html

    def setup_session(self):
        self.session.headers.update({
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def setup_driver(self, headless: bool = True) -> webdriver.Chrome:
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')

        # Stability options
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')  # Speed up loading
        chrome_options.add_argument('--disable-javascript')  # Disable JS to prevent crashes
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=4096')
        chrome_options.add_argument(f'--user-agent={self.get_random_user_agent()}')
        chrome_options.add_argument('--window-size=1280,720')  # Smaller window

        # Prefs to disable images and CSS
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(5)  # Reduced wait time
        self.driver.set_page_load_timeout(30)
        return self.driver

    def random_delay(self):
        delay = random.uniform(*self.delay_range)
        self.logger.info(f"Sleeping for {delay:.2f} seconds")
        time.sleep(delay)

    def safe_find_element(self, driver, by, value, timeout=3):
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            return None

    def safe_find_elements(self, driver, by, value, timeout=10):
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return driver.find_elements(by, value)
        except TimeoutException:
            self.logger.warning(f"Elements not found: {by}={value}")
            return []

    def extract_text_safe(self, element, default="N/A"):
        try:
            return element.text.strip() if element and element.text else default
        except:
            return default

    def extract_attribute_safe(self, element, attribute, default="N/A"):
        try:
            return element.get_attribute(attribute) if element else default
        except:
            return default

    def clean_price(self, price_text: str) -> Optional[int]:
        if not price_text or price_text == "N/A":
            return None
        try:
            # Remove all non-digit characters except dots and commas
            price_clean = ''.join(c for c in price_text if c.isdigit() or c in '.,')
            # Remove dots used as thousand separators, keep commas as decimal
            price_clean = price_clean.replace('.', '').replace(',', '.')
            return int(float(price_clean))
        except:
            return None

    def extract_rating(self, rating_text: str) -> Optional[float]:
        if not rating_text or rating_text == "N/A":
            return None
        try:
            # Extract numeric value from rating
            import re
            match = re.search(r'(\d+\.?\d*)', rating_text)
            if match:
                return float(match.group(1))
        except:
            return None
        return None

    def extract_count(self, count_text: str) -> Optional[int]:
        if not count_text or count_text == "N/A":
            return None
        try:
            # Extract numbers, handle k/K for thousands
            import re
            count_text = count_text.lower().replace(',', '').replace('.', '')
            if 'k' in count_text:
                match = re.search(r'(\d+\.?\d*)k', count_text)
                if match:
                    return int(float(match.group(1)) * 1000)
            else:
                match = re.search(r'(\d+)', count_text)
                if match:
                    return int(match.group(1))
        except:
            return None
        return None

    def create_product_data(self, **kwargs) -> Dict[str, Any]:
        return {
            "source": self.source_name,
            "product_id": kwargs.get('product_id', ''),
            "url": kwargs.get('url', ''),
            "crawl_date": datetime.now().isoformat(),
            "product_name": kwargs.get('product_name', ''),
            "brand": kwargs.get('brand', ''),
            "category": kwargs.get('category', ''),
            "description": kwargs.get('description', ''),
            "image_urls": kwargs.get('image_urls', []),
            "price_current": kwargs.get('price_current'),
            "price_original": kwargs.get('price_original'),
            "discount_percent": kwargs.get('discount_percent'),
            "rating_avg": kwargs.get('rating_avg'),
            "rating_count": kwargs.get('rating_count'),
            "sold_count": kwargs.get('sold_count'),
            "favorite_count": kwargs.get('favorite_count'),
            "seller_name": kwargs.get('seller_name', ''),
            "seller_type": kwargs.get('seller_type', '')
        }

    def create_review_data(self, **kwargs) -> Dict[str, Any]:
        return {
            "source": self.source_name,
            "product_id": kwargs.get('product_id', ''),
            "review_id": kwargs.get('review_id', ''),
            "crawl_date": datetime.now().isoformat(),
            "rating": kwargs.get('rating'),
            "title": kwargs.get('title', ''),
            "content": kwargs.get('content', ''),
            "review_date": kwargs.get('review_date', ''),
            "reviewer_name": kwargs.get('reviewer_name', ''),
            "verified_purchase": kwargs.get('verified_purchase', False),
            "like_count": kwargs.get('like_count'),
            "image_urls": kwargs.get('image_urls', []),
            "seller_response": kwargs.get('seller_response', '')
        }

    def save_data(self, data: List[Dict], filename: str):
        try:
            # Ensure data directory exists
            import os
            data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
            os.makedirs(data_dir, exist_ok=True)

            filepath = os.path.join(data_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                for item in data:
                    json.dump(item, f, ensure_ascii=False)
                    f.write('\n')
            self.logger.info(f"Saved {len(data)} items to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

    def cleanup(self):
        if self.driver:
            self.driver.quit()
        if self.session:
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()