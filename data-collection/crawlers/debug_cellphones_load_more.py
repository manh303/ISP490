#!/usr/bin/env python3
"""Debug script to understand CellphoneS load more button behavior"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

def debug_cellphones_load_more():
    # Set up Chrome options
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(options=options)

    try:
        print('Visiting CellphoneS mobile page...')
        driver.get('https://cellphones.com.vn/mobile.html')
        time.sleep(5)

        title = driver.title
        print(f'Page title length: {len(title) if title else 0}')

        # Count initial products
        initial_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*=".html"]')
        initial_mobile_links = [link.get_attribute('href') for link in initial_links
                              if link.get_attribute('href') and 'mobile' in link.get_attribute('href')]
        print(f'Initial mobile product links: {len(initial_mobile_links)}')

        # Look for load more button
        load_more_selectors = [
            '.button.btn-show-more.button__show-more-product',
            '.btn-show-more',
            '.button__show-more-product'
        ]

        for i in range(5):  # Try up to 5 times
            print(f'\n--- Load attempt {i+1} ---')

            # Scroll to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Look for load more button
            load_more_found = False
            for selector in load_more_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    element = elements[0]
                    print(f'Found load more button: {selector}')
                    button_text = element.text[:100] if element.text else 'No text'
                    print(f'Button text length: {len(button_text)}')
                    print(f'Button visible: {element.is_displayed()}')
                    print(f'Button enabled: {element.is_enabled()}')

                    if element.is_displayed() and element.is_enabled():
                        # Scroll to button
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                        time.sleep(1)

                        # Click button
                        driver.execute_script("arguments[0].click();", element)
                        print('Clicked load more button')
                        load_more_found = True
                        time.sleep(5)  # Wait for new products to load
                        break
                    else:
                        print('Button found but not clickable')

            if not load_more_found:
                print('No load more button found')

            # Count products after clicking
            current_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*=".html"]')
            current_mobile_links = [link.get_attribute('href') for link in current_links
                                  if link.get_attribute('href') and 'mobile' in link.get_attribute('href')]
            print(f'Current mobile product links: {len(current_mobile_links)}')

            # Check if number increased
            if len(current_mobile_links) == len(initial_mobile_links):
                print(f'No new products loaded. Still at {len(current_mobile_links)} products')
                if not load_more_found:
                    print('Stopping - no load more button and no new products')
                    break
            else:
                print(f'Products increased from {len(initial_mobile_links)} to {len(current_mobile_links)}')
                initial_mobile_links = current_mobile_links

            # Check page source for any remaining "Xem thêm" text
            page_source = driver.page_source
            if 'Xem thêm' in page_source:
                print('Still has "Xem thêm" text in page')
            else:
                print('No more "Xem thêm" text found')

        print(f'\nFinal count: {len(current_mobile_links)} mobile product links')

        # Show some sample URLs
        print('\nSample URLs:')
        for i, url in enumerate(current_mobile_links[:5]):
            print(f'  {i+1}. {url}')

    except Exception as e:
        print(f'Error: {e}')
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_cellphones_load_more()