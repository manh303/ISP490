#!/usr/bin/env python3

import os
import json
import shutil
from pathlib import Path

def setup_kaggle_credentials():
    """Setup Kaggle credentials from local kaggle.json"""
    source_path = Path("C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-pipeline/kaggle.json")
    if not source_path.exists():
        print(f"❌ kaggle.json not found at {source_path}")
        return False

    # Create .kaggle directory if it doesn't exist
    kaggle_dir = Path.home() / '.kaggle'
    kaggle_dir.mkdir(exist_ok=True)
    
    # Copy kaggle.json to the correct location
    dest_path = kaggle_dir / 'kaggle.json'
    shutil.copy(source_path, dest_path)
    
    # Set correct permissions
    os.chmod(dest_path, 0o600)
    print(f"✅ Copied kaggle.json to {dest_path} and set permissions")
    
    # Load credentials into environment variables
    with open(source_path) as f:
        creds = json.load(f)
    os.environ['KAGGLE_USERNAME'] = creds['username']
    os.environ['KAGGLE_KEY'] = creds['key']
    
    return True

def test_kaggle_credentials():
    """Test Kaggle API credentials"""
    try:
        # Import kaggle after credentials are set up
        import kaggle
        
        # Test API connection
        print("Testing Kaggle API...")
        
        # List available datasets (first 5)
        datasets = kaggle.api.dataset_list(page=1)
        
        print("✅ Kaggle API working!")
        print("Sample datasets:")
        for dataset in datasets:
            print(f"  - {dataset.ref}")
        
        return True
        
    except Exception as e:
        print(f"❌ Kaggle API Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check kaggle.json exists in ~/.kaggle/")
        print("2. Verify file permissions: chmod 600 ~/.kaggle/kaggle.json")
        print("3. Check JSON format in kaggle.json")
        return False

def get_kaggle_info_for_env():
    """Extract info để tạo .env file"""
    kaggle_path = Path("C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-pipeline/kaggle.json")
    
    if kaggle_path.exists():
        import json
        with open(kaggle_path, 'r') as f:
            creds = json.load(f)
        
        print("\n📝 Add these to your .env file:")
        print(f"KAGGLE_USERNAME={creds['username']}")
        print(f"KAGGLE_KEY={creds['key']}")
        
        # Copy file to .kaggle directory
        kaggle_dir = Path.home() / '.kaggle'
        kaggle_dir.mkdir(exist_ok=True)
        import shutil
        shutil.copy(kaggle_path, kaggle_dir / 'kaggle.json')
        
        # Set correct permissions
        os.chmod(kaggle_dir / 'kaggle.json', 0o600)
        print("\n✅ Copied kaggle.json to ~/.kaggle/ directory and set permissions")

if __name__ == "__main__":
    if setup_kaggle_credentials():
        if test_kaggle_credentials():
            get_kaggle_info_for_env()