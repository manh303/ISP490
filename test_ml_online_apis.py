"""
Test script for ML Online APIs

Tests both /ml/price-predictions/online and /ml/sentiment/online
to verify they are no longer returning hardcoded values.
"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"

# You'll need a valid JWT token for authentication
# Replace with actual token from login
TOKEN = "YOUR_JWT_TOKEN_HERE"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def test_price_prediction():
    """Test price prediction API"""
    print("\n" + "="*80)
    print("Testing /ml/price-predictions/online")
    print("="*80)
    
    payload = {
        "platform_code": "tiki",
        "product_key": "tiki_123456",
        "model_name": "price_forecast_rf",
        "model_version": "v1.0",
        # Optional: provide features directly
        "current_price": 150000,
        "avg_rating": 4.5,
        "review_count": 100
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/ml/price-predictions/online",
            headers=headers,
            json=payload,
            timeout=10
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("\n✓ SUCCESS! Response:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            
            # Verify it's not hardcoded 1000000
            if data['predicted_price'] == 1000000.0:
                print("\n⚠️  WARNING: Price is still 1,000,000 - may be hardcoded or coincidence")
            else:
                print(f"\n✓ Price prediction: {data['predicted_price']:,.0f} VND (not hardcoded!)")
                
            print(f"✓ Model: {data['model_name']} v{data['model_version']}")
            print(f"✓ Latency: {data['latency_ms']}ms")
            
        else:
            print(f"\n✗ FAILED!")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"\n✗ EXCEPTION: {str(e)}")


def test_sentiment_analysis():
    """Test sentiment analysis API"""
    print("\n" + "="*80)
    print("Testing /ml/sentiment/online")
    print("="*80)
    
    test_cases = [
        {
            "text": "Sản phẩm rất tốt, chất lượng cao, giao hàng nhanh",
            "expected": "positive"
        },
        {
            "text": "Tệ quá, không như mô tả, rất thất vọng",
            "expected": "negative"
        },
        {
            "text": "Bình thường, không có gì đặc biệt",
            "expected": "neutral"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- Test Case {i} ---")
        print(f"Text: {test_case['text']}")
        
        payload = {
            "platform_code": "tiki",
            "review_text": test_case['text'],
            "model_name": "sentiment_tfidf_logreg",
            "model_version": "v1.0"
        }
        
        try:
            response = requests.post(
                f"{BASE_URL}/ml/sentiment/online",
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Label: {data['label']} (score: {data['score']:.3f})")
                print(f"  Model: {data['model_name']} v{data['model_version']}")
                print(f"  Latency: {data['latency_ms']}ms")
                
                # Check if it's using real ML (not simple keyword matching)
                if data['score'] in [0.9, 0.6]:
                    print(f"  ⚠️  Score {data['score']} might indicate hardcoded logic")
                else:
                    print(f"  ✓ Score looks realistic (not 0.9 or 0.6)")
                    
            else:
                print(f"✗ FAILED! Status: {response.status_code}")
                print(f"Error: {response.text}")
                
        except Exception as e:
            print(f"✗ EXCEPTION: {str(e)}")


def check_model_files():
    """Check if model files exist"""
    print("\n" + "="*80)
    print("Checking Model Files")
    print("="*80)
    
    import os
    from pathlib import Path
    
    model_dir = Path("ml/models")
    
    if not model_dir.exists():
        print(f"✗ Model directory not found: {model_dir}")
        return
        
    print(f"✓ Model directory: {model_dir.absolute()}")
    
    pkl_files = list(model_dir.glob("*.pkl"))
    if pkl_files:
        print(f"\n✓ Found {len(pkl_files)} model files:")
        for f in pkl_files:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  - {f.name} ({size_mb:.2f} MB)")
    else:
        print("\n✗ No .pkl files found!")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("ML ONLINE API TEST SUITE")
    print("="*80)
    print("\nNOTE: You need to set a valid JWT TOKEN in this script first!")
    print("      Get it by logging in through /api/v1/iam/login\n")
    
    # First check model files
    check_model_files()
    
    # Then test APIs (if you have token)
    if TOKEN != "YOUR_JWT_TOKEN_HERE":
        test_price_prediction()
        test_sentiment_analysis()
    else:
        print("\n⚠️  Skipping API tests - no JWT token provided")
        print("   To test the APIs:")
        print("   1. Login and get JWT token")
        print("   2. Update TOKEN variable in this script")
        print("   3. Run again")
    
    print("\n" + "="*80)
    print("Test suite completed!")
    print("="*80)
