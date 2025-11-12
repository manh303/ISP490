#!/usr/bin/env python3
"""Debug script to check Lazada page structure"""
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

print("=" * 60)
print("LAZADA DEBUG SCRIPT")
print("=" * 60)

# Setup driver
options = uc.ChromeOptions()
options.add_argument("--window-size=1920,1080")
driver = uc.Chrome(options=options)

try:
    url = "https://www.lazada.vn/dien-thoai-di-dong/"
    print(f"\n1. Loading: {url}")
    driver.get(url)
    
    print("2. Waiting 10 seconds for JavaScript to render...")
    time.sleep(10)
    
    print(f"3. Current URL: {driver.current_url}")
    print(f"4. Page title: {driver.title}")
    
    # Scroll
    print("5. Scrolling to trigger lazy loading...")
    for i in range(5):
        driver.execute_script(f'window.scrollTo(0, {(i+1)*300})')
        time.sleep(1.5)
    
    print("6. Waiting another 3 seconds...")
    time.sleep(3)
    
    # Try different selectors
    print("\n7. Testing selectors:")
    selectors = [
        '[data-qa-locator="product-item"]',
        'div.Bm3ON',
        '[class*="product"]',
        'a[href*="/products/"]',
        'div[class*="item"]'
    ]
    
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            print(f"   {selector}: {len(elements)} elements")
            if elements and len(elements) > 0:
                print(f"      First element: {elements[0].tag_name}")
                try:
                    print(f"      First element text: {elements[0].text[:50]}...")
                except:
                    pass
        except Exception as e:
            print(f"   {selector}: ERROR - {e}")
    
    # Try JavaScript
    print("\n8. Testing JavaScript extraction:")
    js_script = """
    var links = document.querySelectorAll('a[href*="/products/"]');
    console.log('Found links:', links.length);
    return links.length;
    """
    count = driver.execute_script(js_script)
    print(f"   Found {count} product links via JavaScript")
    
    # Get all links
    js_script2 = """
    var links = document.querySelectorAll('a[href*="/products/"]');
    var urls = [];
    for (var i = 0; i < Math.min(5, links.length); i++) {
        urls.push(links[i].href);
    }
    return urls;
    """
    urls = driver.execute_script(js_script2)
    print(f"\n9. Sample product URLs:")
    for i, url in enumerate(urls, 1):
        print(f"   {i}. {url}")
    
    # Save page source
    print("\n10. Saving page source...")
    with open("debug_lazada_page.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print("   Saved to: debug_lazada_page.html")
    
    print("\n" + "=" * 60)
    print("DEBUG COMPLETED!")
    print("Check debug_lazada_page.html to see page structure")
    print("=" * 60)
    
    input("\nPress Enter to close browser...")
    
except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    try:
        driver.quit()
    except:
        pass
