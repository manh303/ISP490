import unittest
from backend.app.services.ai_summarizer import AISummarizer

class TestAISummarizer(unittest.TestCase):
    def setUp(self):
        self.summarizer = AISummarizer()

    def test_price_prompt_generation_with_data(self):
        dss_result = {
            "kpi_summary": {
                "num_products": 10,
                "num_with_recommendation": 5,
                "current_revenue": 1000000,
                "projected_revenue": 1200000,
                "expected_revenue_uplift_pct": 0.2,
                "avg_confidence": 0.85
            },
            "table_data": [
                {
                    "product_name": "Test Product 1",
                    "current_price": 100000,
                    "predicted_price": 110000,
                    "price_change_pct": 0.1,
                    "confidence": 0.9,
                    "expected_revenue_change_pct": 0.15
                }
            ],
            "filters": {}
        }
        prompt = self.summarizer._build_price_prompt(dss_result)
        print("\n[Prompt with Data]:\n", prompt)
        self.assertIn("1,000,000 VND", prompt)
        self.assertIn("Test Product 1", prompt)

    def test_price_prompt_generation_no_data(self):
        dss_result = {
            "kpi_summary": {
                "num_products": 0,
                "num_with_recommendation": 0,
                "current_revenue": 0,
                "projected_revenue": 0
            },
            "table_data": [],
            "filters": {}
        }
        prompt = self.summarizer._build_price_prompt(dss_result)
        print("\n[Prompt No Data]:\n", prompt)
        self.assertIn("không có dữ liệu", prompt)

if __name__ == '__main__':
    unittest.main()
