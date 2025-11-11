#!/usr/bin/env python3
"""
Tiki Complete Pipeline - Products + Reviews + Load to DB
"""
import subprocess
import sys
import os
from pathlib import Path

LOG_PREFIX = "[Tiki-Pipeline]"

def run_script(script_path):
    """Run a Python script"""
    script_name = Path(script_path).name
    print(f"\n{'='*60}")
    print(f"{LOG_PREFIX} Running: {script_name}")
    print('='*60)
    
    result = subprocess.run([sys.executable, script_path])
    
    if result.returncode != 0:
        print(f"{LOG_PREFIX} ❌ {script_name} failed")
        return False
    
    print(f"{LOG_PREFIX} ✅ {script_name} completed")
    return True

def main():
    print(f"{LOG_PREFIX} Starting Tiki Complete Pipeline")
    print("="*60)
    
    current_dir = Path(__file__).parent
    
    steps = [
        ("1. Crawl Products", current_dir / "tiki_crawler.py"),
        ("2. Crawl Reviews", current_dir / "tiki_review_crawler.py"),
        ("3. Load to Database", current_dir / "load_reviews_to_db.py")
    ]
    
    for step_name, script_path in steps:
        print(f"\n{LOG_PREFIX} Step: {step_name}")
        
        if not script_path.exists():
            print(f"{LOG_PREFIX} ❌ Script not found: {script_path}")
            return
        
        if not run_script(script_path):
            print(f"\n{LOG_PREFIX} ❌ Pipeline stopped at {step_name}")
            return
    
    print(f"\n{'='*60}")
    print(f"{LOG_PREFIX} ✅ All steps completed successfully!")
    print("="*60)

if __name__ == "__main__":
    main()
