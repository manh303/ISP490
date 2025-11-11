#!/usr/bin/env python3
"""
Test script to check directory creation permissions
"""
import os
import tempfile
from pathlib import Path

def test_directory_creation():
    """Test creating directories in various locations"""
    test_dirs = [
        "/app/.profiles",
        "/app/data/outputs", 
        "/tmp/profiles/lazada",
        "/tmp/data/outputs",
        "/tmp/crawler_logs"
    ]
    
    results = {}
    
    for test_dir in test_dirs:
        try:
            Path(test_dir).mkdir(parents=True, exist_ok=True)
            # Test write permission
            test_file = os.path.join(test_dir, "test_write.txt")
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
            results[test_dir] = "✅ OK"
        except PermissionError as e:
            results[test_dir] = f"❌ Permission denied: {e}"
        except Exception as e:
            results[test_dir] = f"❌ Error: {e}"
    
    print("Directory Creation Test Results:")
    print("=" * 50)
    for dir_path, result in results.items():
        print(f"{dir_path:<25} {result}")
    
    # Test temp directory as fallback
    try:
        temp_dir = tempfile.mkdtemp(prefix="airflow-test-")
        print(f"\n✅ Temp directory created: {temp_dir}")
        os.rmdir(temp_dir)
    except Exception as e:
        print(f"\n❌ Temp directory failed: {e}")

if __name__ == "__main__":
    test_directory_creation()