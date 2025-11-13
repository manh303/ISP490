#!/usr/bin/env python3
"""
Run All Standardization Steps
"""
import subprocess
import sys
from pathlib import Path

def run_script(script_name):
    """Run a Python script"""
    script_path = Path(__file__).parent / script_name
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print('='*60)
    
    result = subprocess.run([sys.executable, str(script_path)])
    
    if result.returncode != 0:
        print(f"❌ {script_name} failed")
        return False
    
    print(f"✅ {script_name} completed")
    return True

def main():
    print("STANDARDIZATION PIPELINE")
    print("="*60)
    
    steps = [
        "data_cleaning.py",
        "data_quality.py",
        "identifier_sync.py",
        "category_mapping.py",
        "technical_metadata.py"
    ]
    
    for step in steps:
        if not run_script(step):
            print(f"\n❌ Pipeline stopped at {step}")
            return
    
    print("\n✅ All standardization steps completed!")

if __name__ == "__main__":
    main()
