#!/usr/bin/env python3
"""
Lazada Cookie Generator - Chạy trên máy local để lấy cookies
"""
import json
from playwright.sync_api import sync_playwright

def generate_cookies():
    print("=" * 60)
    print("LAZADA COOKIE GENERATOR")
    print("=" * 60)
    print("\n1. Browser sẽ mở trang Lazada")
    print("2. Đăng nhập vào tài khoản Lazada của bạn")
    print("3. Sau khi đăng nhập xong, nhấn Enter trong terminal này")
    print("4. Cookies sẽ được lưu vào file cookies.json")
    print("\n" + "=" * 60)
    input("\nNhấn Enter để bắt đầu...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        # Mở trang Lazada
        print("\n✓ Đang mở Lazada...")
        page.goto("https://www.lazada.vn")
        
        print("\n→ Vui lòng đăng nhập vào Lazada trong browser")
        print("→ Sau khi đăng nhập xong, quay lại terminal và nhấn Enter")
        input("\nNhấn Enter sau khi đã đăng nhập...")
        
        # Lấy cookies
        cookies = context.cookies()
        
        # Lưu cookies
        with open('lazada_cookies.json', 'w') as f:
            json.dump(cookies, f, indent=2)
        
        print("\n✓ Cookies đã được lưu vào: lazada_cookies.json")
        print("\n" + "=" * 60)
        print("HƯỚNG DẪN SỬ DỤNG COOKIES:")
        print("=" * 60)
        print("\n1. Copy file lazada_cookies.json vào server:")
        print("   docker cp lazada_cookies.json airflow-webserver:/app/data/.profiles/lazada/lazada_cookies.json")
        print("\n2. Hoặc tạo thư mục và copy:")
        print("   docker exec airflow-webserver mkdir -p /app/data/.profiles/lazada")
        print("   docker cp lazada_cookies.json airflow-webserver:/app/data/.profiles/lazada/lazada_cookies.json")
        print("\n3. Chạy crawler với cookies:")
        print("   docker exec airflow-webserver python /app/crawlers/lazada/runners/lazada_with_cookies.py")
        print("\n" + "=" * 60)
        
        browser.close()

if __name__ == "__main__":
    try:
        generate_cookies()
    except KeyboardInterrupt:
        print("\n\n✗ Đã hủy")
    except Exception as e:
        print(f"\n✗ Lỗi: {e}")