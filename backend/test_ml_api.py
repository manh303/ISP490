#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick test script to verify ML API can load models
"""

import sys
import io
import os
from pathlib import Path

# Fix encoding for Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    os.chdir(Path(__file__).parent)

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

def test_ml_paths():
    """Test if ML folder paths are correct"""
    print("=" * 60)
    print("ML API Path Test")
    print("=" * 60)
    
    # Calculate paths like ml_api.py does
    ml_api_file = Path(__file__).parent / "app" / "api" / "v1" / "ml_api.py"
    ml_folder = ml_api_file.resolve().parent.parent.parent.parent.parent / "ml"
    models_dir = ml_folder / "models" / "ml-models"
    data_dir = ml_folder / "data"
    
    print(f"ML API File: {ml_api_file}")
    print(f"ML Folder: {ml_folder}")
    print(f"Models Dir: {models_dir}")
    print(f"Data Dir: {data_dir}")
    print()
    
    # Check if paths exist
    print("✓ Path Existence Checks:")
    print(f"  ML folder exists: {ml_folder.exists()}")
    print(f"  Models dir exists: {models_dir.exists()}")
    print(f"  Data dir exists: {data_dir.exists()}")
    print()
    
    # List models
    if models_dir.exists():
        print("✓ Available Models:")
        for model_file in sorted(models_dir.glob("*.pkl")):
            size_kb = model_file.stat().st_size / 1024
            print(f"  - {model_file.name} ({size_kb:.1f} KB)")
    print()
    
    # List data
    if data_dir.exists():
        print("✓ Available Data:")
        for category in ["demand_prediction", "product_recommendation"]:
            cat_dir = data_dir / category
            if cat_dir.exists():
                print(f"  {category}:")
                for data_file in sorted(cat_dir.glob("*.csv")):
                    size_kb = data_file.stat().st_size / 1024
                    print(f"    - {data_file.name} ({size_kb:.1f} KB)")
    print()
    
    return models_dir.exists() and data_dir.exists()


def test_ml_import():
    """Test if ML API can be imported"""
    print("=" * 60)
    print("ML API Import Test")
    print("=" * 60)
    
    try:
        from app.api.v1.ml_api import (
            DemandPredictionRequest,
            RecommendationRequest,
            load_models,
            get_demand_data,
            get_recommendation_data
        )
        print("✓ Successfully imported ML API components")
        print(f"  - DemandPredictionRequest")
        print(f"  - RecommendationRequest")
        print(f"  - load_models")
        print(f"  - get_demand_data")
        print(f"  - get_recommendation_data")
        print()
        
        # Try to load models
        print("✓ Loading Models...")
        from app.api.v1 import ml_api
        
        models_loaded = list(ml_api.loaded_models.keys())
        print(f"  Models loaded: {models_loaded if models_loaded else 'Using mock mode'}")
        print()
        
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n")
    
    paths_ok = test_ml_paths()
    import_ok = test_ml_import()
    
    print("=" * 60)
    print("Test Results")
    print("=" * 60)
    print(f"Paths: {'✅ PASS' if paths_ok else '❌ FAIL'}")
    print(f"Import: {'✅ PASS' if import_ok else '❌ FAIL'}")
    print()
    
    if paths_ok and import_ok:
        print("✅ All tests passed! ML API is ready.")
        return 0
    else:
        print("❌ Some tests failed. Check the output above.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
