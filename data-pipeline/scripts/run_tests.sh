#!/bin/bash

# Data Pipeline Testing Script
echo "🧪 Running Data Pipeline Tests..."

# Set colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test functions
test_step() {
    echo -e "${YELLOW}Testing: $1${NC}"
    if python scripts/$2; then
        echo -e "${GREEN}✅ $1 - PASSED${NC}"
        return 0
    else
        echo -e "${RED}❌ $1 - FAILED${NC}"
        return 1
    fi
}

# Run tests
cd /app

echo "📊 Testing Data Pipeline Components..."
echo "======================================"

test_step "Data Download" "step1_download_data.py"
if [ $? -eq 0 ]; then
    test_step "Data Cleaning" "step2_data_cleaning.py"
    if [ $? -eq 0 ]; then
        test_step "Feature Engineering" "step3_feature_engineering.py"
        if [ $? -eq 0 ]; then
            test_step "ML Modeling" "step4_ml_modeling.py"
            if [ $? -eq 0 ]; then
                test_step "Model Validation" "step5_model_validation.py"
            fi
        fi
    fi
fi

echo ""
echo "🏁 Testing completed!"
echo "Check logs above for any issues."