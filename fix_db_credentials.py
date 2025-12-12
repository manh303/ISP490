#!/usr/bin/env python3
"""
Script to fix database credentials from Render to local Docker
"""
import re
import os
from pathlib import Path

# Old and new patterns
OLD_PASSWORD = "dss_password_123"
NEW_PASSWORD = "dss_password_123"

OLD_HOST_PATTERN = r"dpg-[a-z0-9-]+\.singapore-postgres\.render\.com"
NEW_HOST = "localhost"

OLD_PORT = "5432"
NEW_PORT = "5433"

OLD_DB = "ecommerce_dss"
NEW_DB = "ecommerce_dss"

# Files to update
file_patterns = [
    "**/*.py",
    "**/*.yml",
    "**/*.yaml",
]

exclude_dirs = {
    ".git", "node_modules", "__pycache__", 
    ".venv", "venv", "dist", "build",
    ".pytest_cache", ".mypy_cache"
}

def should_exclude(path):
    """Check if path should be excluded"""
    parts = Path(path).parts
    return any(excluded in parts for excluded in exclude_dirs)

def update_file(file_path):
    """Update database credentials in a file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Replace password
        content = content.replace(OLD_PASSWORD, NEW_PASSWORD)
        
        # Replace host pattern
        content = re.sub(OLD_HOST_PATTERN, NEW_HOST, content)
        
        # Replace database name
        content = content.replace(OLD_DB, NEW_DB)
        
        # Replace port in connection strings (only when with old host)
        # This is more conservative to avoid breaking other configs
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
    except Exception as e:
        print(f"  ⚠️  Error processing {file_path}: {e}")
    return False

def main():
    print("🔄 Fixing database credentials from Render to Local Docker...")
    print(f"   Old password: {OLD_PASSWORD}")
    print(f"   New password: {NEW_PASSWORD}")
    print(f"   Old DB: {OLD_DB}")
    print(f"   New DB: {NEW_DB}")
    print()
    
    updated_files = []
    project_root = Path(".")
    
    for pattern in file_patterns:
        for file_path in project_root.glob(pattern):
            if file_path.is_file() and not should_exclude(file_path):
                if update_file(file_path):
                    updated_files.append(str(file_path))
                    print(f"  ✅ {file_path}")
    
    print()
    print("=" * 60)
    print(f"✅ Updated {len(updated_files)} files")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Check .env file manually (if needed)")
    print("2. Backend should auto-reload")
    print("3. Test database connection")

if __name__ == "__main__":
    main()
