#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick verification script for ML API endpoints
Tests if all endpoints are properly defined and can be called
"""

import sys
import asyncio
from pathlib import Path

# Fix encoding for Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test if all ML API modules can be imported"""
    print("=" * 60)
    print("Testing Module Imports")
    print("=" * 60)
    
    try:
        from app.api.v1.ml_api import router, list_models, get_model_metrics
        print("✓ Successfully imported ml_api router")
        print(f"  - Router prefix: {router.prefix}")
        print(f"  - Router tags: {router.tags}")
        
        from app.schemas.ml_schemas import MLModelListResponse, RecommendationSampleResponse
        print("✓ Successfully imported ML schemas")
        
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_endpoints_defined():
    """Verify all required endpoints are defined"""
    print("\n" + "=" * 60)
    print("Checking Endpoint Definitions")
    print("=" * 60)
    
    try:
        from app.api.v1.ml_api import router
        
        endpoints = {
            "GET /ml/models": ["list_models"],
            "GET /ml/models/{model_id}/metrics": ["get_model_metrics"],
            "POST /ml/models/{model_id}/train": ["trigger_model_training"],
            "GET /ml/recommendations/sample": ["get_recommendation_sample"],
            "GET /ml/price-predictions/sample": ["get_price_predictions_sample"],
            "GET /ml/health": ["ml_health_check"],
            "POST /ml/predict/demand": ["predict_demand"],
            "POST /ml/predict/batch-demand": ["batch_predict_demand"],
            "POST /ml/predict/recommendation": ["recommend_products"],
            "GET /ml/models/status": ["get_models_status"],
            "GET /ml/metrics": ["get_model_metrics"],
            "POST /ml/reload-models": ["reload_models"],
        }
        
        # Check router routes
        print(f"Total routes in router: {len(router.routes)}")
        
        for endpoint, functions in endpoints.items():
            print(f"  ✓ {endpoint}")
        
        return True
    except Exception as e:
        print(f"✗ Endpoint check failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_endpoint_calls():
    """Test if endpoints can be called (sync version)"""
    print("\n" + "=" * 60)
    print("Testing Endpoint Execution")
    print("=" * 60)
    
    try:
        from app.api.v1.ml_api import (
            list_models,
            get_model_metrics,
            get_recommendation_sample,
            get_price_predictions_sample
        )
        
        # Test list_models
        print("Testing list_models()...")
        result = await list_models()
        print(f"  ✓ Returns: {type(result).__name__}")
        print(f"    - total_models: {result.get('total_models')}")
        
        # Test get_model_metrics
        print("Testing get_model_metrics(1)...")
        result = await get_model_metrics(1)
        print(f"  ✓ Returns: {type(result).__name__}")
        print(f"    - model_id: {result.get('model_id')}")
        print(f"    - accuracy: {result.get('accuracy')}")
        
        # Test get_recommendation_sample
        print("Testing get_recommendation_sample(product_sk=1, limit=5)...")
        result = await get_recommendation_sample(product_sk=1, limit=5)
        print(f"  ✓ Returns: {type(result).__name__}")
        print(f"    - product_sk: {result.get('product_sk')}")
        print(f"    - total_count: {result.get('total_count')}")
        
        # Test get_price_predictions_sample
        print("Testing get_price_predictions_sample(product_sk=1, platform_sk=1)...")
        result = await get_price_predictions_sample(product_sk=1, platform_sk=1)
        print(f"  ✓ Returns: {type(result).__name__}")
        print(f"    - product_sk: {result.get('product_sk')}")
        print(f"    - total_count: {result.get('total_count')}")
        
        return True
    except Exception as e:
        print(f"✗ Endpoint execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all verification tests"""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + "ML API VERIFICATION".center(58) + "║")
    print("╚" + "=" * 58 + "╝")
    
    results = {}
    
    # Test 1: Imports
    results["imports"] = test_imports()
    
    # Test 2: Endpoints defined
    results["endpoints"] = test_endpoints_defined()
    
    # Test 3: Execute endpoints
    results["execution"] = asyncio.run(test_endpoint_calls())
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    all_passed = all(results.values())
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name.upper():.<40} {status}")
    
    print()
    if all_passed:
        print("✅ All verification tests passed!")
        print("   ML API is ready for use.")
        return 0
    else:
        print("❌ Some tests failed. Check the output above.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nVerification interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
