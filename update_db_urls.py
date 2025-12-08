# update_db_urls.py
# Batch replace old Render database URLs with local Docker URLs

import os
import re
from pathlib import Path

# Database URLs
OLD_URL_PATTERN = r"postgresql://dss_user:dss_password_123@dpg-d4j17gn5r7bs73bsoqm0-a\.singapore-postgres\.render\.com[:/]\d*/ecommerce_dss"
NEW_URL = "postgresql://dss_user:dss_password_123@localhost:5433/ecommerce_dss"

# Old patterns
OLD_HOST = "localhost"
NEW_HOST = "localhost"

OLD_DB_NAME = "ecommerce_dss"
NEW_DB_NAME = "ecommerce_dss"

# Files to update
EXTENSIONS = [".py", ".yml", ".yaml", ".sh", ".ps1", ".env"]
SKIP_DIRS = ["venv", "node_modules", ".git", "__pycache__", "backups", "data"]

def should_skip(path):
    """Check if path should be skipped"""
    return any(skip in str(path) for skip in SKIP_DIRS)

def update_file(file_path):
    """Update database URLs in a single file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Replace full DATABASE_URL
        content = re.sub(OLD_URL_PATTERN, NEW_URL, content)
        
        # Replace host
        content = content.replace(OLD_HOST, NEW_HOST)
        
        # Replace database name (only in specific patterns)
        content = re.sub(
            r"(['\"]database['\"]:\s*['\"])ecommerce_dss(['\"])",
            r"\1ecommerce_dss\2",
            content
        )
        content = re.sub(
            r"(database\s*=\s*['\"])ecommerce_dss(['\"])",
            r"\1ecommerce_dss\2",
            content
        )
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
    except Exception as e:
        print(f"  [ERROR] {file_path}: {e}")
    return False

def main():
    project_root = Path(__file__).parent
    updated_count = 0
    
    print(f"Updating database URLs to local Docker...")
    print(f"Old URL pattern: {OLD_URL_PATTERN}")
    print(f"New URL: {NEW_URL}")
    print("=" * 60)
    
    for ext in EXTENSIONS:
        for file_path in project_root.rglob(f"*{ext}"):
            if should_skip(file_path):
                continue
            
            if update_file(file_path):
                print(f"✅ Updated: {file_path.relative_to(project_root)}")
                updated_count += 1
    
    print("=" * 60)
    print(f"\n✅ Updated {updated_count} files")
    print("\nNext steps:")
    print("1. Update .env file manually (if needed)")
    print("2. Restart backend: uvicorn backend.main:app --reload")
    print("3. Test connection with local database")

if __name__ == "__main__":
    main()
