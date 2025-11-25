"""
AI Summarizer Service for DSS
Generates insights and recommended actions from DSS results using LLM
"""

import os
import json
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Check if OpenAI is available
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("OpenAI not available. Install with: pip install openai")


class AISummarizer:
    """
    Generate AI-powered insights and action recommendations from DSS results.
    
    Usage:
        summarizer = AISummarizer()
        result = summarizer.summarize_with_ai("price_prediction", dss_result_raw)
    """
    
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # Cheaper model for summaries
        
        if OPENAI_AVAILABLE and self.api_key:
            openai.api_key = self.api_key
            self.available = True
            logger.info(f"AI Summarizer initialized with model: {self.model}")
        else:
            self.available = False
            logger.warning("AI Summarizer not available - missing OpenAI API key or library")
    
    def summarize_with_ai(self, scenario: str, dss_result_raw: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Generate insights and actions for DSS result
        
        Args:
            scenario: One of "price_prediction", "product_recommendation", "review_sentiment"
            dss_result_raw: Raw DSS result with kpi_summary and table_data
        
        Returns:
            {
                "summary_insights": ["insight 1", "insight 2", ...],
                "recommended_actions": ["action 1", "action 2", ...]
            }
        """
        
        if not self.available:
            return self._get_fallback_response(scenario, dss_result_raw)
        
        try:
            # Get prompt for scenario
            system_prompt = self._get_system_prompt()
            user_prompt = self._build_user_prompt(scenario, dss_result_raw)
            
            # Call OpenAI (v1.0+ API)
            from openai import OpenAI
            client = OpenAI(api_key=self.api_key)
            
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=1000,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            content = response.choices[0].message.content
            result = json.loads(content)
            
            # Validate structure
            if "summary_insights" not in result or "recommended_actions" not in result:
                logger.error("Invalid AI response structure")
                return self._get_fallback_response(scenario, dss_result_raw)
            
            logger.info(f"AI summarization successful for scenario: {scenario}")
            return result
            
        except Exception as e:
            logger.error(f"AI summarization error: {e}")
            return self._get_fallback_response(scenario, dss_result_raw)
    
    def _get_system_prompt(self) -> str:
        """System prompt for AI assistant"""
        return """Bạn là trợ lý phân tích dữ liệu e-commerce chuyên nghiệp.

Nhiệm vụ của bạn:
1. Phân tích dữ liệu DSS (Decision Support System) được cung cấp
2. Tóm tắt 3-5 insights quan trọng nhất (tiếng Việt, ngắn gọn, dễ hiểu)
3. Đề xuất 3-7 hành động cụ thể, có thể thực hiện ngay

Yêu cầu:
- Insights phải dựa trên SỐ LIỆU thực tế trong data
- Actions phải CỤ THỂ, HÀNH ĐỘNG ĐƯỢC (không chung chung)
- Sử dụng số liệu để minh họa (VD: "tăng 15%", "40/120 sản phẩm")
- Ưu tiên insights có tác động lớn nhất đến doanh thu/lợi nhuận
- Viết bằng tiếng Việt chuyên nghiệp

Trả về ĐÚNG format JSON:
{
  "summary_insights": ["insight 1", "insight 2", "insight 3"],
  "recommended_actions": ["action 1", "action 2", "action 3"]
}"""
    
    def _build_user_prompt(self, scenario: str, dss_result_raw: Dict[str, Any]) -> str:
        """Build user prompt based on scenario"""
        
        prompts = {
            "price_prediction": self._build_price_prompt,
            "product_recommendation": self._build_reco_prompt,
            "review_sentiment": self._build_sentiment_prompt,
        }
        
        builder = prompts.get(scenario)
        if not builder:
            raise ValueError(f"Unknown scenario: {scenario}")
        
        return builder(dss_result_raw)
    
    def _build_price_prompt(self, dss_result: Dict[str, Any]) -> str:
        """Build prompt for price prediction scenario"""
        
        kpi = dss_result.get("kpi_summary", {})
        table = dss_result.get("table_data", [])[:10]  # Top 10 products
        filters = dss_result.get("filters", {})
        
        prompt = f"""Scenario: PRICE PREDICTION & OPTIMIZATION

Bối cảnh:
- Period: {filters.get('from_date')} đến {filters.get('to_date')}
- Platform: {', '.join(filters.get('platforms', ['All']))}
- Category: {', '.join(filters.get('categories', ['All']))}

KPI Summary:
- Tổng số sản phẩm phân tích: {kpi.get('num_products', 0)}
- Số sản phẩm có đề xuất giá mới: {kpi.get('num_with_recommendation', 0)}
- Doanh thu hiện tại: {kpi.get('current_total_revenue', 0):,.0f} ₫
- Doanh thu dự kiến (nếu áp giá mới): {kpi.get('projected_total_revenue', 0):,.0f} ₫

Top sản phẩm có cơ hội tăng doanh thu:
"""
        
        for i, item in enumerate(table[:5], 1):
            prompt += f"""
{i}. {item.get('product_name', 'N/A')}
   - Giá hiện tại: {item.get('current_price', 0):,.0f} ₫
   - Giá đề xuất: {item.get('recommended_price', 0):,.0f} ₫
   - Doanh thu hiện tại: {item.get('current_revenue', 0):,.0f} ₫
   - Doanh thu dự kiến: {item.get('projected_revenue', 0):,.0f} ₫
   - Thay đổi doanh thu: {item.get('expected_revenue_change_pct', 0)*100:.1f}%
   - Độ tin cậy: {item.get('confidence', 0)*100:.0f}%
"""
        
        prompt += """
Hãy phân tích và đưa ra:
1. Summary Insights: 3-5 insight quan trọng về cơ hội tối ưu giá
2. Recommended Actions: 3-7 hành động cụ thể cho team Pricing/Product

Trả về JSON format đúng như system prompt."""
        
        return prompt
    
    def _build_reco_prompt(self, dss_result: Dict[str, Any]) -> str:
        """Build prompt for product recommendation scenario"""
        
        kpi = dss_result.get("kpi_summary", {})
        table = dss_result.get("table_data", [])[:15]
        filters = dss_result.get("filters", {})
        
        prompt = f"""Scenario: PRODUCT RECOMMENDATION & CROSS-SELL

Bối cảnh:
- Mode: {filters.get('scope_mode', 'N/A')}
- Source Product: {kpi.get('source_product', 'N/A')}
- Platform: {', '.join(filters.get('platforms', ['All']))}

KPI Summary:
- Số gợi ý sản phẩm: {kpi.get('num_recommendations', 0)}
- Độ tương đồng trung bình: {kpi.get('avg_similarity', 0)*100:.1f}%

Top gợi ý cross-sell/upsell:
"""
        
        for i, item in enumerate(table[:10], 1):
            prompt += f"""
{i}. {item.get('source_product_name', 'N/A')} → {item.get('recommended_product_name', 'N/A')}
   - Platform: {item.get('platform', 'N/A')}
   - Similarity: {item.get('similarity_score', 0)*100:.0f}%
   - Tỷ lệ mua cùng: {item.get('co_purchase_rate', 0)*100:.1f}%
   - Doanh thu bundle trung bình: {item.get('avg_bundle_revenue', 0):,.0f} ₫
"""
        
        prompt += """
Hãy phân tích và đưa ra:
1. Summary Insights: 3-5 insight về cơ hội cross-sell/upsell
2. Recommended Actions: 3-7 hành động cụ thể cho team Marketing/Product

Ví dụ actions: tạo bundle, hiển thị "Frequently bought together", campaign combo, điều chỉnh layout...

Trả về JSON format đúng như system prompt."""
        
        return prompt
    
    def _build_sentiment_prompt(self, dss_result: Dict[str, Any]) -> str:
        """Build prompt for review sentiment scenario"""
        
        kpi = dss_result.get("kpi_summary", {})
        table = dss_result.get("table_data", [])[:15]
        filters = dss_result.get("filters", {})
        
        prompt = f"""Scenario: REVIEW SENTIMENT ANALYSIS

Bối cảnh:
- Period: {filters.get('from_date')} đến {filters.get('to_date')}
- Platform: {', '.join(filters.get('platforms', ['All']))}
- Category: {', '.join(filters.get('categories', ['All']))}

KPI Summary:
- Tổng số sản phẩm phân tích: {kpi.get('num_products', 0)}
- Tỷ lệ review tích cực trung bình: {kpi.get('avg_positive_pct', 0)*100:.1f}%
- Số sản phẩm có vấn đề nghiêm trọng: {kpi.get('num_products_with_critical_negative', 0)}

Sản phẩm cần chú ý:
"""
        
        for i, item in enumerate(table[:8], 1):
            reasons = item.get('top_negative_reasons', [])
            reasons_str = ', '.join(reasons[:3]) if reasons else 'N/A'
            
            prompt += f"""
{i}. {item.get('product_name', 'N/A')} ({item.get('platform', 'N/A')})
   - Tổng review: {item.get('total_reviews', 0)}
   - Tích cực: {item.get('positive_pct', 0)*100:.1f}%
   - Tiêu cực: {item.get('negative_pct', 0)*100:.1f}%
   - Lý do tiêu cực chính: {reasons_str}
"""
        
        prompt += """
Hãy phân tích và đưa ra:
1. Summary Insights: 3-5 vấn đề chính khách hàng đang không hài lòng
2. Recommended Actions: 3-7 hành động cụ thể cho team CS/Quality/Content

Ví dụ actions: cải thiện mô tả sản phẩm, xử lý giao hàng, đổi supplier, training CS, update FAQ...

Trả về JSON format đúng như system prompt."""
        
        return prompt
    
    def _get_fallback_response(self, scenario: str, dss_result: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Fallback response when AI is not available
        Generate rule-based insights
        """
        
        kpi = dss_result.get("kpi_summary", {})
        
        fallbacks = {
            "price_prediction": {
                "summary_insights": [
                    f"Phân tích {kpi.get('num_products', 0)} sản phẩm, phát hiện {kpi.get('num_with_recommendation', 0)} sản phẩm có cơ hội tối ưu giá.",
                    f"Doanh thu dự kiến có thể tăng từ {kpi.get('current_total_revenue', 0):,.0f} ₫ lên {kpi.get('projected_total_revenue', 0):,.0f} ₫.",
                    "Cần xem xét điều chỉnh giá cho các sản phẩm có độ tin cậy cao (>80%)."
                ],
                "recommended_actions": [
                    "Review chi tiết top 10 sản phẩm có expected_revenue_change cao nhất.",
                    "Thiết lập A/B test giá cho nhóm sản phẩm ưu tiên trong 7-14 ngày.",
                    "Monitor competitor pricing và điều chỉnh theo thị trường.",
                    "Cập nhật pricing strategy document với insights mới."
                ]
            },
            "product_recommendation": {
                "summary_insights": [
                    f"Tìm thấy {kpi.get('num_recommendations', 0)} cặp sản phẩm có cơ hội cross-sell/upsell.",
                    f"Độ tương đồng trung bình: {kpi.get('avg_similarity', 0)*100:.1f}%",
                    "Có cơ hội tăng doanh thu thông qua product bundling và recommendations."
                ],
                "recommended_actions": [
                    "Implement 'Frequently Bought Together' section cho các cặp sản phẩm có co-purchase rate cao.",
                    "Tạo bundle promotions cho top 5 cặp sản phẩm.",
                    "Update recommendation algorithm với data mới.",
                    "A/B test hiển thị recommendations ở vị trí khác nhau trên product page."
                ]
            },
            "review_sentiment": {
                "summary_insights": [
                    f"Phân tích {kpi.get('num_products', 0)} sản phẩm, {kpi.get('num_products_with_critical_negative', 0)} sản phẩm có vấn đề nghiêm trọng.",
                    f"Tỷ lệ review tích cực trung bình: {kpi.get('avg_positive_pct', 0)*100:.1f}%",
                    "Cần tập trung xử lý các sản phẩm có tỷ lệ tiêu cực cao."
                ],
                "recommended_actions": [
                    "Priority 1: Xử lý ngay các sản phẩm có negative_pct > 30%.",
                    "Phân tích chi tiết top negative reasons và đưa ra action plan cụ thể.",
                    "Cải thiện product description và images để giảm expectation mismatch.",
                    "Training CS team để handle complaints hiệu quả hơn.",
                    "Setup monitoring alert cho sản phẩm có negative trend đột ngột."
                ]
            }
        }
        
        return fallbacks.get(scenario, {
            "summary_insights": ["Không thể tạo insights tự động. Vui lòng xem chi tiết data bên dưới."],
            "recommended_actions": ["Review data và tạo action plan thủ công."]
        })


# Singleton instance
_summarizer_instance = None

def get_ai_summarizer() -> AISummarizer:
    """Get singleton AI summarizer instance"""
    global _summarizer_instance
    if _summarizer_instance is None:
        _summarizer_instance = AISummarizer()
    return _summarizer_instance

