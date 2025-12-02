"""
AI Summarizer Service for DSS
Generates insights and recommended actions from DSS results using LLM
Multi-provider fallback: OpenAI → Google Gemini → Rule-based
"""

import os
import json
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Check available AI libraries
try:
    from openai import OpenAI, RateLimitError as OpenAIRateLimitError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAIRateLimitError = Exception
    logger.warning("OpenAI not available. Install with: pip install openai")

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logger.warning("Google Gemini not available. Install with: pip install google-generativeai")



class AIProvider(ABC):
    """Abstract base class for AI providers"""
    
    def __init__(self, name: str):
        self.name = name
        self.available = False
    
    @abstractmethod
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        """Generate AI response from prompts"""
        pass
    
    def is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is a rate limit error"""
        return False


class OpenAIProvider(AIProvider):
    """OpenAI GPT provider"""
    
    def __init__(self):
        super().__init__("OpenAI")
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        if OPENAI_AVAILABLE and self.api_key:
            try:
                self.client = OpenAI(api_key=self.api_key)
                self.available = True
                logger.info(f"OpenAI provider initialized with model: {self.model}")
            except Exception as e:
                logger.warning(f"Failed to initialize OpenAI: {e}")
        else:
            logger.warning("OpenAI provider not available")
    
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        """Generate response using OpenAI"""
        if not self.available:
            raise RuntimeError("OpenAI provider not available")
        
        messages = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.7,
            max_tokens=1000,
            response_format={"type": "json_object"}
        )
        
        return response.choices[0].message.content
    
    def is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is OpenAI rate limit"""
        return isinstance(error, OpenAIRateLimitError) or "429" in str(error)


class GeminiProvider(AIProvider):
    """Google Gemini provider"""
    
    def __init__(self):
        super().__init__("Google Gemini")
        self.api_key = os.getenv("GOOGLE_GEMINI_API_KEY")

        # Try multiple model names in order of preference (without 'models/' prefix)
        model_options = [
            os.getenv("GEMINI_MODEL", "gemini-2.0-flash").replace("models/", ""),
            "gemini-2.0-flash",
            "gemini-2.0-flash-exp",
            "gemini-1.5-flash",
            "gemini-1.5-pro",
            "gemini-2.5-flash",
            "gemini-flash-latest",
            "gemini-pro-latest",
        ]

        if GEMINI_AVAILABLE and self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                # Try to get available models
                available_models = []
                try:
                    models = genai.list_models()
                    available_models = [model.name for model in models if 'generateContent' in model.supported_generation_methods]
                    logger.info(f"Available Gemini models: {available_models}")
                except Exception as e:
                    logger.warning(f"Could not list Gemini models: {e}")

                # Try models in order of preference
                for model_name in model_options:
                    full_model_name = f"models/{model_name}"
                    if available_models and full_model_name not in available_models:
                        logger.warning(f"Model {full_model_name} not in available models list, trying anyway...")

                    try:
                        # Use only standard, universally-supported safety categories
                        # These are the core categories that work across all Gemini models
                        safety_settings = [
                            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                        ]
                        
                        self.model = genai.GenerativeModel(
                            model_name=model_name,
                            generation_config={
                                "temperature": 0.5,  # Lower temp for more consistent JSON
                                "max_output_tokens": 2000,  # Increased to avoid MAX_TOKENS cutoff
                                "top_p": 0.9,
                                "top_k": 20,
                            },
                            safety_settings=safety_settings
                        )
                        self.model_name = model_name
                        self.available = True
                        logger.info(f"✅ Gemini provider initialized with model: {model_name}")
                        break
                    except Exception as e:
                        logger.warning(f"Failed to initialize Gemini with model {model_name}: {e}")
                        continue
                else:
                    logger.warning("❌ Gemini provider not available - all model options failed")
            except Exception as e:
                logger.warning(f"❌ Gemini provider initialization failed: {e}")
        else:
            logger.warning("❌ Gemini provider not available - missing API key or library")
    
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        """Generate response using Gemini"""
        if not self.available:
            raise RuntimeError("Gemini provider not available")

        # Gemini doesn't have separate system/user roles, combine prompts
        # Keep it simple and direct - overly complex prompts can trigger safety filters
        combined_prompt = f"""Analyze this business data and provide JSON insights:

Data: {user_prompt}

Return ONLY this JSON format (no markdown, no code blocks, just raw JSON):
{{"summary_insights": ["insight 1", "insight 2", "insight 3"], "recommended_actions": ["action 1", "action 2", "action 3"]}}"""

        try:
            response = self.model.generate_content(combined_prompt)
        except Exception as e:
            error_msg = str(e).lower()
            if "safety" in error_msg or "blocked" in error_msg:
                raise RuntimeError(f"Gemini response blocked by safety filters: {e}")
            else:
                raise RuntimeError(f"Gemini API error: {e}")

        # Check if response has any candidates
        if not response.candidates or len(response.candidates) == 0:
            feedback = getattr(response, 'prompt_feedback', {})
            block_reason = getattr(feedback, 'block_reason', 'UNKNOWN')
            logger.error(f"Gemini blocked response. Block reason: {block_reason}")
            raise RuntimeError(f"Gemini returned no candidates - blocked by safety filters (reason: {block_reason})")
        
        candidate = response.candidates[0]
        
        # Check finish reason: 0=UNSPECIFIED, 1=STOP, 2=MAX_TOKENS, 3=SAFETY, 4=RECITATION, 5=UNKNOWN
        finish_reasons = {
            0: "UNSPECIFIED",
            1: "STOP",
            2: "MAX_TOKENS", 
            3: "SAFETY",
            4: "RECITATION",
            5: "BLOCKED_PROACTIVE"
        }
        
        finish_reason_num = getattr(candidate, 'finish_reason', 0)
        finish_reason_str = finish_reasons.get(finish_reason_num, f"UNKNOWN({finish_reason_num})")
        
        # Log the finish reason for debugging
        if finish_reason_num != 1:  # Not STOP
            logger.warning(f"Gemini finished with reason: {finish_reason_str}")
        
        if finish_reason_num == 3:  # SAFETY
            safety_ratings = getattr(candidate, 'safety_ratings', [])
            logger.error(f"Gemini safety ratings: {safety_ratings}")
            raise RuntimeError(f"Gemini blocked by safety filters (finish_reason=SAFETY)")
        elif finish_reason_num == 4:  # RECITATION
            raise RuntimeError("Gemini blocked due to recitation policy")
        elif finish_reason_num == 5:  # BLOCKED_PROACTIVE
            raise RuntimeError("Gemini blocked response proactively")
        
        # Try to extract text from content parts
        if hasattr(candidate, 'content') and candidate.content:
            if hasattr(candidate.content, 'parts') and candidate.content.parts and len(candidate.content.parts) > 0:
                try:
                    text = candidate.content.parts[0].text
                    if text and text.strip():
                        return text
                except (AttributeError, IndexError, TypeError) as e:
                    logger.warning(f"Failed to extract text from candidate.content.parts: {e}")
        
        # Check if response has direct text property
        if hasattr(response, 'text'):
            try:
                if response.text and response.text.strip():
                    return response.text
            except Exception as e:
                logger.warning(f"Failed to access response.text: {e}")
        
        # Response is completely empty - this is a safety filter block
        logger.error(f"Gemini returned empty response (finish_reason={finish_reason_str})")
        raise RuntimeError(f"Gemini response is empty - blocked by safety filters")
    
    def is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is Gemini rate limit"""
        error_str = str(error).lower()
        return "429" in error_str or "quota" in error_str or "rate limit" in error_str


class AISummarizer:
    """
    Generate AI-powered insights and action recommendations from DSS results.
    Multi-provider fallback: OpenAI → Google Gemini → Rule-based
    
    Usage:
        summarizer = AISummarizer()
        result = summarizer.summarize_with_ai("price_prediction", dss_result_raw)
    """
    
    def __init__(self):
        # Initialize all available providers
        self.providers: List[AIProvider] = []
        self.provider_stats: Dict[str, Dict[str, int]] = {}  # Track success/failure per provider
        
        # Try OpenAI first
        openai_provider = OpenAIProvider()
        if openai_provider.available:
            self.providers.append(openai_provider)
            self.provider_stats[openai_provider.name] = {"success": 0, "failure": 0, "rate_limit": 0}
        
        # Try Gemini as fallback
        gemini_provider = GeminiProvider()
        if gemini_provider.available:
            self.providers.append(gemini_provider)
            self.provider_stats[gemini_provider.name] = {"success": 0, "failure": 0, "rate_limit": 0}
        
        # Log available providers
        if self.providers:
            provider_names = [p.name for p in self.providers]
            logger.info(f"AI Summarizer initialized with providers: {', '.join(provider_names)}")
            self.model = self.providers[0].model if hasattr(self.providers[0], 'model') else "multi-provider"
            self.available = True
        else:
            logger.warning("No AI providers available - will use rule-based fallback only")
            self.available = False
            self.model = None
    
    def _reorder_providers_by_success(self):
        """Reorder providers by success rate to prioritize working providers"""
        def provider_score(provider):
            stats = self.provider_stats.get(provider.name, {})
            total = stats.get("success", 0) + stats.get("failure", 0) + stats.get("rate_limit", 0)
            if total == 0:
                return 1  # New provider, put at end
            success_rate = stats.get("success", 0) / total
            has_rate_limit = stats.get("rate_limit", 0) > 0
            # Deprioritize providers with rate limits
            if has_rate_limit:
                success_rate *= 0.5
            return -success_rate  # Negative so higher success = earlier in sort
        
        self.providers.sort(key=provider_score)
    
    def summarize_with_ai(self, scenario: str, dss_result_raw: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Generate insights and actions for DSS result with multi-provider fallback and retry logic
        
        Args:
            scenario: One of "price_prediction", "product_recommendation", "review_sentiment"
            dss_result_raw: Raw DSS result with kpi_summary and table_data
        
        Returns:
            {
                "summary_insights": ["insight 1", "insight 2", ...],
                "recommended_actions": ["action 1", "action 2", ...]
            }
        """
        
        if not self.providers:
            logger.info("No AI providers available, using rule-based fallback")
            return self._get_fallback_response(scenario, dss_result_raw)
        
        # Reorder providers based on recent success/failure rates
        self._reorder_providers_by_success()
        
        # Get prompts for scenario
        system_prompt = self._get_system_prompt()
        user_prompt = self._build_user_prompt(scenario, dss_result_raw)
        
        # Try each provider in order with retry logic
        last_error = None
        for i, provider in enumerate(self.providers):
            max_retries = 2 if provider.name == "OpenAI" else 1
            retry_delay = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    if attempt == 0:
                        logger.info(f"Trying {provider.name} for AI summarization...")
                    else:
                        logger.info(f"Retrying {provider.name} (attempt {attempt + 1}/{max_retries}) after {retry_delay}s delay...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    
                    # For OpenAI, use both prompts; for Gemini, use combined
                    if provider.name == "OpenAI":
                        content = provider.generate(system_prompt, user_prompt)
                    else:
                        # For other providers like Gemini, only use user prompt to avoid complexity
                        content = provider.generate("", user_prompt)
                    
                    # Parse JSON response
                    # Handle potential markdown code blocks from Gemini
                    content = content.strip()
                    if content.startswith("```json"):
                        content = content[7:]
                    if content.startswith("```"):
                        content = content[3:]
                    if content.endswith("```"):
                        content = content[:-3]
                    content = content.strip()
                    
                    result = json.loads(content)
                    
                    # Validate structure
                    if "summary_insights" not in result or "recommended_actions" not in result:
                        logger.error(f"{provider.name} returned invalid structure: {result}")
                        last_error = f"Invalid response structure from {provider.name}"
                        self.provider_stats[provider.name]["failure"] += 1
                        break  # Don't retry invalid structures
                    
                    logger.info(f"✅ AI summarization successful using {provider.name}")
                    self.provider_stats[provider.name]["success"] += 1
                    return result
                    
                except json.JSONDecodeError as e:
                    logger.error(f"{provider.name} returned invalid JSON: {e}")
                    last_error = f"Invalid JSON from {provider.name}: {str(e)}"
                    self.provider_stats[provider.name]["failure"] += 1
                    break  # Don't retry JSON parse errors
                    
                except Exception as e:
                    error_msg = str(e)
                    is_rate_limit = provider.is_rate_limit_error(e)
                    is_safety_blocked = "safety" in error_msg.lower() or "blocked" in error_msg.lower() or "empty" in error_msg.lower()
                    
                    if is_rate_limit:
                        logger.warning(f"⚠️  {provider.name} rate limit (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")
                        last_error = f"{provider.name} rate limited"
                        self.provider_stats[provider.name]["rate_limit"] += 1
                        # Retry on rate limit with exponential backoff
                        if attempt < max_retries - 1:
                            continue
                        else:
                            break  # Move to next provider
                    elif is_safety_blocked:
                        logger.warning(f"⚠️  {provider.name} blocked by safety filters: {error_msg[:150]}")
                        last_error = f"{provider.name} safety filter block"
                        self.provider_stats[provider.name]["failure"] += 1
                        break  # Don't retry safety blocks
                    else:
                        logger.error(f"❌ {provider.name} error: {error_msg[:200]}")
                        last_error = f"{provider.name} error: {error_msg[:100]}"
                        self.provider_stats[provider.name]["failure"] += 1
                        break  # Don't retry other errors
            
            # If this is the last provider and we've exhausted retries, fall back to rule-based
            if i == len(self.providers) - 1:
                logger.warning(f"❌ All AI providers failed (last error: {last_error}), using rule-based fallback")
                logger.info(f"Provider statistics: {self.provider_stats}")
                return self._get_fallback_response(scenario, dss_result_raw)
        
        # Should not reach here, but just in case
        logger.warning("Unexpected fallback to rule-based analysis")
        return self._get_fallback_response(scenario, dss_result_raw)
    
    def _get_system_prompt(self) -> str:
        """System prompt for AI assistant"""
        return """Bạn là một chuyên gia phân tích dữ liệu kinh doanh.

Nhiệm vụ của bạn là phân tích dữ liệu DSS (Decision Support System) được cung cấp và:
1. Xác định các mô hình, xu hướng và bất thường trong dữ liệu
2. Cung cấp các hiểu biết kinh doanh có cấu trúc dựa trên các chỉ số
3. Tạo các khuyến nghị chiến lược dựa trên phân tích

Yêu cầu về phản hồi:
- Hiểu biết phải dựa trên các dữ liệu số cụ thể từ phân tích
- Khuyến nghị nên thực tế và hành động được
- Sử dụng ngôn ngữ rõ ràng, chuyên nghiệp và tiếng Việt
- Cung cấp các quan điểm đa dạng thay vì những gợi ý chung chung

Chỉ trả lại JSON hợp lệ ở định dạng sau:
{"summary_insights": ["insight 1", "insight 2", "insight 3"], "recommended_actions": ["action 1", "action 2", "action 3"]}

Đảm bảo JSON hợp lệ và chứa chính xác các khóa được chỉ định."""
    
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
        table = dss_result.get("table_data", [])[:3]
        filters = dss_result.get("filters", {})

        prompt = f"""Phân tích dự báo giá:
Số sản phẩm: {kpi.get('num_products', 0)}
Có cơ hội tối ưu: {kpi.get('num_with_recommendation', 0)}
Doanh thu hiện tại: {kpi.get('current_total_revenue', 0):,.0f} VND
Doanh thu dự kiến: {kpi.get('projected_total_revenue', 0):,.0f} VND

Top sản phẩm:
"""

        for i, item in enumerate(table, 1):
            prompt += f"{i}. {item.get('product_name', 'N/A')[:30]} - Giá hiện tại: {item.get('current_price', 0):.0f} VND, Giá đề xuất: {item.get('recommended_price', 0):.0f} VND, Độ tin cậy: {item.get('confidence', 0)*100:.0f}%\n"

        prompt += """
Cung cấp thông tin chi tiết bằng tiếng Việt về chiến lược tối ưu hóa giá:
{"summary_insights": ["insight 1", "insight 2", "insight 3"], "recommended_actions": ["action 1", "action 2", "action 3"]}"""

        return prompt
    
    def _build_reco_prompt(self, dss_result: Dict[str, Any]) -> str:
        """Build prompt for product recommendation scenario"""

        kpi = dss_result.get("kpi_summary", {})
        table = dss_result.get("table_data", [])[:3]
        filters = dss_result.get("filters", {})

        prompt = f"""Phân tích khuyến nghị sản phẩm:
Số lượng khuyến nghị: {kpi.get('num_recommendations', 0)}
Độ tương đồng trung bình: {kpi.get('avg_similarity', 0)*100:.0f}%
Chế độ: {filters.get('scope_mode', 'N/A')}

Top cặp sản phẩm:
"""

        for i, item in enumerate(table, 1):
            prompt += f"{i}. {item.get('source_product_name', 'N/A')[:20]} → {item.get('recommended_product_name', 'N/A')[:20]} | Tương đồng: {item.get('similarity_score', 0)*100:.0f}% | Co-mua: {item.get('co_purchase_rate', 0)*100:.0f}%\n"

        prompt += """
Cung cấp thông tin chi tiết bằng tiếng Việt về chiến lược cross-sell:
{"summary_insights": ["insight 1", "insight 2", "insight 3"], "recommended_actions": ["action 1", "action 2", "action 3"]}"""

        return prompt
    
    def _build_sentiment_prompt(self, dss_result: Dict[str, Any]) -> str:
        """Build prompt for review sentiment scenario"""

        kpi = dss_result.get("kpi_summary", {})
        table = dss_result.get("table_data", [])[:3]
        filters = dss_result.get("filters", {})

        prompt = f"""Phân tích cảm xúc đánh giá:
Số sản phẩm: {kpi.get('num_products', 0)}
Tỷ lệ đánh giá tích cực: {kpi.get('avg_positive_pct', 0)*100:.0f}%
Số sản phẩm có vấn đề: {kpi.get('num_products_with_critical_negative', 0)}

Sản phẩm cần chú ý:
"""

        for i, item in enumerate(table, 1):
            reasons = item.get('top_negative_reasons', [])
            reasons_str = ', '.join(reasons[:2]) if reasons else 'N/A'
            prompt += f"{i}. {item.get('product_name', 'N/A')[:25]} - Tích cực: {item.get('positive_pct', 0)*100:.0f}%, Tiêu cực: {item.get('negative_pct', 0)*100:.0f}%, Vấn đề: {reasons_str}\n"

        prompt += """
Cung cấp thông tin chi tiết bằng tiếng Việt về cải thiện sự hài lòng khách hàng:
{"summary_insights": ["insight 1", "insight 2", "insight 3"], "recommended_actions": ["action 1", "action 2", "action 3"]}"""

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

