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
        
        # Primary model from env, default gpt-4o-mini
        primary_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        # Optional fallback models from env, comma-separated
        fallback_models = [
            m.strip()
            for m in os.getenv("OPENAI_MODEL_FALLBACKS", "").split(",")
            if m.strip()
        ]
        
        # Models to try in order
        self.models: List[str] = [primary_model] + [
            m for m in fallback_models if m != primary_model
        ]
        self.model = primary_model  # Keep for logging / backward compat
        
        if OPENAI_AVAILABLE and self.api_key:
            try:
                self.client = OpenAI(
                    api_key=self.api_key,
                    # Disable client-side automatic retries to avoid long backoffs
                    max_retries=int(os.getenv("OPENAI_MAX_RETRIES", "0")),
                    timeout=float(os.getenv("OPENAI_TIMEOUT", "15")),
                )
                self.available = True
                logger.info(
                    "OpenAI provider initialized with model: %s, fallbacks=%s",
                    self.model,
                    self.models[1:],
                )
            except Exception as e:
                logger.warning(f"Failed to initialize OpenAI: {e}")
        else:
            logger.warning("OpenAI provider not available")
    
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        """Generate response using OpenAI (supports multiple models)."""
        if not self.available:
            raise RuntimeError("OpenAI provider not available")
        
        messages: List[Dict[str, str]] = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        
        last_error: Optional[Exception] = None
        
        # Try configured models in order
        for model_name in self.models:
            try:
                logger.info("OpenAI generate with model=%s", model_name)
                response = self.client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=1000,
                    response_format={"type": "json_object"},
                )
                # If successful, remember the working model
                self.model = model_name
                return response.choices[0].message.content
            
            except OpenAIRateLimitError as e:
                # Rate limit is usually per-organization → let caller switch provider
                logger.warning(
                    "OpenAI rate limit on model=%s: %s", model_name, str(e)[:200]
                )
                last_error = e
                # Để AISummarizer fallback sang Gemini thay vì retry lâu
                break
            
            except Exception as e:
                # Other errors (5xx, network, invalid model...) → try next model if any
                logger.warning(
                    "OpenAI error with model=%s: %s", model_name, str(e)[:200]
                )
                last_error = e
                continue
        
        if last_error:
            raise last_error
        
        raise RuntimeError("OpenAI generation failed for all models")
    
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
                    available_models = [
                        model.name for model in models
                        if 'generateContent' in getattr(model, "supported_generation_methods", [])
                    ]
                except Exception as e:
                    logger.warning(f"Could not list Gemini models: {e}")
                
                # Select the first model that exists in available_models
                selected_model = None
                for model_name in model_options:
                    full_name = f"models/{model_name}"
                    if not available_models or full_name in available_models:
                        selected_model = full_name
                        break
                
                if selected_model is None:
                    # Fallback to a default if nothing matched
                    selected_model = "models/gemini-2.0-flash"
                
                self.model = selected_model
                self.client = genai.GenerativeModel(self.model)
                self.available = True
                logger.info(f"✅ Gemini provider initialized with model: {self.model}")
            except Exception as e:
                logger.warning(f"Failed to initialize Google Gemini: {e}")
        else:
            logger.warning("Google Gemini provider not available")
    
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        """Generate response using Google Gemini"""
        if not self.available:
            raise RuntimeError("Google Gemini provider not available")
        
        # Combine system and user prompts for Gemini
        prompt = system_prompt.strip() + "\n\n" + user_prompt if system_prompt else user_prompt
        
        generation_config = {
            "temperature": 0.7,
            "max_output_tokens": 1000,
            "response_mime_type": "application/json",
        }
        
        response = self.client.generate_content(
            [{"text": prompt}],
            generation_config=generation_config,
        )
        
        # Prefer candidates; avoid response.text quick accessor (can throw when empty)
        if getattr(response, "candidates", None):
            for candidate in response.candidates:
                finish = getattr(candidate, "finish_reason", None)
                finish_str = str(finish).upper() if finish else ""
                if finish_str in {"MAX_TOKENS", "SAFETY", "RECITATION", "BLOCKED", "BLOCKED_PROACTIVE"}:
                    # Return minimal JSON to allow downstream parsing/backfill instead of hard fail
                    return json.dumps({
                        "summary_insights": [f"Gemini finish_reason={finish_str.lower()} - output truncated/blocked."],
                        "recommended_actions": []
                    })
                if hasattr(candidate, "content") and getattr(candidate.content, "parts", None):
                    texts = [
                        part.text for part in candidate.content.parts
                        if hasattr(part, "text") and part.text
                    ]
                    if texts:
                        return "\n".join(texts)
        
        # No usable text: return minimal JSON to avoid hard failure
        return json.dumps({
            "summary_insights": ["Gemini response contained no text candidates."],
            "recommended_actions": []
        })
    
    def is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is a Gemini rate limit error"""
        msg = str(error).lower()
        return "rate limit" in msg or "quota" in msg or "429" in msg


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
        self.available: bool = False  # Expose availability to callers
        self.model: str = "rule-based-fallback"  # Last model/provider used
        
        # Prefer Gemini first (to avoid OpenAI rate-limit surfaces), then OpenAI
        if GEMINI_AVAILABLE and os.getenv("GOOGLE_GEMINI_API_KEY"):
            gemini_provider = GeminiProvider()
            if gemini_provider.available:
                self.providers.append(gemini_provider)
        
        if OPENAI_AVAILABLE and os.getenv("OPENAI_API_KEY"):
            openai_provider = OpenAIProvider()
            if openai_provider.available:
                self.providers.append(openai_provider)
        
        # If no AI providers are available, we will use rule-based fallback only
        if not self.providers:
            logger.warning("No AI providers available. Using rule-based summarization only.")
        
        # Initialize provider stats
        for provider in self.providers:
            self.provider_stats[provider.name] = {
                "success": 0,
                "failure": 0,
                "rate_limit": 0
            }
        
        # Order providers based on historical performance
        self._sort_providers_by_performance()
        
        # Set availability/model for downstream consumers (backward compatibility)
        self.available = len(self.providers) > 0
        if self.available:
            primary = self.providers[0]
            self.model = getattr(primary, "model", primary.name)
    
    def _sort_providers_by_performance(self):
        """Sort providers based on success / failure stats when available"""
        if not self.provider_stats:
            return
        
        def provider_score(provider: AIProvider) -> float:
            stats = self.provider_stats.get(provider.name, {})
            success = stats.get("success", 0)
            failure = stats.get("failure", 0)
            total = success + failure
            if total == 0:
                return 0.0
            success_rate = success / total
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

        # Debug AI input when needed
        if os.getenv("AI_DEBUG", "0") == "1":
            try:
                kpi_summary = dss_result_raw.get("kpi_summary", {})
                table_data = dss_result_raw.get("table_data", [])
                logger.info(
                    "[AI_DEBUG] scenario=%s, kpi_keys=%s, table_rows=%d",
                    scenario,
                    list(kpi_summary.keys()),
                    len(table_data),
                )
                if table_data:
                    logger.info(
                        "[AI_DEBUG] first_row=%s",
                        json.dumps(table_data[0], ensure_ascii=False)[:1000],
                    )
            except Exception as e:
                logger.warning(f"[AI_DEBUG] Failed to log AI input: {e}")

        if not self.providers:
            # No AI providers available, fallback to rule-based
            logger.info("No AI providers available. Using rule-based summarization.")
            self.available = False
            self.model = "rule-based-fallback"
            return self._get_fallback_response(scenario, dss_result_raw)
        
        # Build prompts
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
                        logger.info(
                            f"Retrying {provider.name} (attempt {attempt + 1}/{max_retries}) "
                            f"after {retry_delay}s delay..."
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    
                    # For OpenAI, use both prompts; for Gemini, use combined
                    if provider.name == "OpenAI":
                        content = provider.generate(system_prompt, user_prompt)
                    else:
                        # For other providers like Gemini, only use user prompt to avoid complexity
                        content = provider.generate("", user_prompt)
                    
                    # Parse JSON response (be lenient with Gemini/plain-text)
                    # Handle potential markdown code blocks from Gemini
                    if content.startswith("```json"):
                        content = content[7:]
                    if content.endswith("```"):
                        content = content[:-3]
                    if content.startswith("```"):
                        content = content[3:]
                    if content.endswith("```"):
                        content = content[:-3]
                    content = content.strip()
                    
                    try:
                        result = json.loads(content)
                    except json.JSONDecodeError:
                        # If provider returned plain text, wrap it into expected schema
                        logger.warning(f"{provider.name} returned non-JSON content, wrapping as summary_insights")
                        result = {
                            "summary_insights": [content] if content else [],
                            "recommended_actions": [],
                        }
                    
                    # Validate structure and coerce missing keys
                    summary = result.get("summary_insights") or []
                    actions = result.get("recommended_actions") or []

                    # Ép về list[str]
                    if not isinstance(summary, list):
                        summary = [summary]
                    summary = [str(s) for s in summary]

                    if not isinstance(actions, list):
                        actions = [actions]
                    actions = [str(a) for a in actions]

                    # Lọc những dòng rõ ràng là log / debug từ Gemini
                    placeholder_markers = [
                        "Gemini response contained no text candidates",
                        "GenerateContentResponse(",
                        "prompt_token_count",
                        "usage_metadata",
                        "model_version",
                    ]
                    summary = [
                        s for s in summary
                        if not any(marker in s for marker in placeholder_markers)
                    ]

                    # Nếu AI nói quá ít → merge thêm fallback rule-based
                    MIN_INSIGHTS = 2
                    MIN_ACTIONS = 2
                    if len(summary) < MIN_INSIGHTS or len(actions) < MIN_ACTIONS:
                        fb = self._get_fallback_response(scenario, dss_result_raw)
                        fb_ins = fb.get("summary_insights", [])
                        fb_act = fb.get("recommended_actions", [])

                        for s in fb_ins:
                            if s not in summary:
                                summary.append(s)

                        for a in fb_act:
                            if a not in actions:
                                actions.append(a)

                    # Giới hạn tối đa cho gọn
                    result = {
                        "summary_insights": summary[:6],
                        "recommended_actions": actions[:6],
                    }

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
                    is_safety_blocked = (
                        "safety" in error_msg.lower()
                        or "blocked" in error_msg.lower()
                        or "empty" in error_msg.lower()
                    )
                    
                    if is_rate_limit:
                        logger.warning(
                            f"⚠️  {provider.name} rate limit (attempt {attempt + 1}/{max_retries}): "
                            f"{error_msg[:200]}"
                        )
                        last_error = f"{provider.name} rate limited"
                        self.provider_stats[provider.name]["rate_limit"] += 1
                        # Không retry thêm khi bị rate limit, chuyển sang provider tiếp theo
                        break  # Move to next provider
                    elif is_safety_blocked:
                        logger.warning(
                            f"⚠️  {provider.name} blocked by safety filters: {error_msg[:150]}"
                        )
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
                logger.warning(
                    f"All AI providers failed. Last error: {last_error}. "
                    "Falling back to rule-based summarization."
                )
                self.model = "rule-based-fallback"
                self.available = False
                return self._get_fallback_response(scenario, dss_result_raw)
        
        # Should not reach here, but just in case
        logger.warning("Unexpected fallback to rule-based analysis")
        self.model = "rule-based-fallback"
        self.available = False
        return self._get_fallback_response(scenario, dss_result_raw)
    
    def _get_system_prompt(self) -> str:
        """System prompt for AI assistant"""
        return """Bạn là một chuyên gia phân tích dữ liệu kinh doanh.

Nhiệm vụ của bạn là phân tích dữ liệu DSS (Decision Support System) được cung cấp và:
1. Xác định các mô hình, xu hướng và bất thường trong dữ liệu
2. Cung cấp các hiểu biết (insights) hữu ích cho business
3. Đề xuất các hành động cụ thể, rõ ràng, có thể thực thi được

Bạn phải trả về kết quả dưới dạng JSON với cấu trúc:
{
  "summary_insights": ["...", "..."],
  "recommended_actions": ["...", "..."]
}

Trong đó:
- summary_insights: là các nhận định, phân tích, điểm nổi bật từ dữ liệu
- recommended_actions: là các hành động cụ thể, ưu tiên, dễ hiểu cho team business

Ngôn ngữ trả về: tiếng Việt, ngắn gọn, dễ hiểu, tránh thuật ngữ quá hàn lâm.
"""
    
    def _build_user_prompt(self, scenario: str, dss_result_raw: Dict[str, Any]) -> str:
        """
        Build user prompt content based on scenario and DSS raw result
        
        dss_result_raw expected structure:
        {
            "input": {...},             # Input parameters
            "kpi_summary": {...},       # Aggregated metrics
            "table_data": [...],        # Detailed records
            "scenario": "price_prediction" | "product_recommendation" | "review_sentiment"
        }
        """
        
        kpi_summary = dss_result_raw.get("kpi_summary", {})
        table_data = dss_result_raw.get("table_data", [])
        scenario = scenario or dss_result_raw.get("scenario", "unknown")
        
        # Limit table data rows for prompt size
        max_rows = 20
        limited_table_data = table_data[:max_rows]
        truncated = len(table_data) > max_rows
        
        base_prompt = f"Scenario phân tích: {scenario}.\n\n"
        
        base_prompt += "I. Thông tin tổng quan KPI:\n"
        base_prompt += json.dumps(kpi_summary, ensure_ascii=False, separators=(",", ":"))
        base_prompt += "\n\nII. Dữ liệu chi tiết (giới hạn tối đa 50 dòng cho tóm tắt):\n"
        base_prompt += json.dumps(limited_table_data, ensure_ascii=False, separators=(",", ":"))
        
        if truncated:
            base_prompt += "\n\n(Lưu ý: Dữ liệu đã được cắt bớt để phù hợp context của mô hình. Hãy tập trung vào xu hướng tổng quan.)"
        
        # Add scenario-specific instructions
        if scenario == "price_prediction":
            base_prompt += """
            
III. Yêu cầu phân tích cho kịch bản DỰ BÁO GIÁ & TỐI ƯU GIÁ:
- Nhận diện các sản phẩm có giá hiện tại đang cao/thấp bất thường so với mức giá dự báo.
- Xem xét biên lợi nhuận (margin), mức độ giảm giá tối đa (max_discount_pct) và độ tin cậy của mô hình.
- Gợi ý các nhóm sản phẩm nên:
  + Giảm giá để kích cầu nhưng vẫn đảm bảo lợi nhuận.
  + Giữ nguyên giá vì đã tối ưu.
  + Có thể tăng giá nhẹ dựa trên tín hiệu cầu cao / cạnh tranh thấp.
- Ưu tiên gợi ý mang tính hành động rõ ràng (ví dụ: 'Giảm ~5-10% cho nhóm sản phẩm A trong 7 ngày tới').
"""
        elif scenario == "product_recommendation":
            base_prompt += """

III. Yêu cầu phân tích cho kịch bản GỢI Ý SẢN PHẨM LIÊN QUAN:

DỮ LIỆU ĐƯỢC CUNG CẤP:
- KPI tổng quan: số lượng sản phẩm nguồn, số lượng gợi ý, độ tương đồng trung bình, số đơn hàng trung bình.
- Danh sách chi tiết các sản phẩm gợi ý với thông tin: sản phẩm nguồn, sản phẩm được gợi ý, platform, category, giá trung bình, số đơn hàng, rating, độ tương đồng.

HÃY PHÂN TÍCH DỮ LIỆU CỤ THỂ ĐƯỢC CUNG CẤP ĐỂ ĐƯA RA:
1. **Insights cụ thể về xu hướng gợi ý**:
   - Nhóm sản phẩm nào được gợi ý nhiều nhất?
   - Độ tương đồng cao nhất ở category/platform nào?
   - Các cặp sản phẩm thường được gợi ý cùng nhau (combo tiềm năng).

2. **Đánh giá chất lượng gợi ý**:
   - Sản phẩm được gợi ý có rating cao (>4.0) và nhiều đơn hàng?
   - Giá cả hợp lý so với sản phẩm nguồn?
   - Platform nào có gợi ý chất lượng tốt nhất?

3. **Đề xuất tối ưu hóa hệ thống gợi ý**:
   - Tăng trọng số cho sản phẩm có rating cao/đơn hàng nhiều.
   - Điều chỉnh ngưỡng độ tương đồng cho từng category.
   - Thêm yếu tố thời vụ hoặc xu hướng thị trường.

4. **Hành động marketing cụ thể**:
   - Tạo combo/bundle cho các sản phẩm thường đi đôi.
   - Chiến dịch cross-sell cho category có tiềm năng.
   - Cách hiển thị gợi ý trên trang chi tiết sản phẩm.

QUAN TRỌNG:
- Phải dựa trên dữ liệu JSON được cung cấp, KHÔNG dùng thông tin chung chung.
- Trả về ít nhất 4 "summary_insights" và 4 "recommended_actions" cụ thể.
- Nêu rõ platform, category, điểm tương đồng, rating, số đơn hàng trong phân tích.
- Nếu dữ liệu ít, vẫn đề xuất hành động dựa trên xu hướng chung.
"""
        elif scenario == "review_sentiment":
            base_prompt += """
            
III. Yêu cầu phân tích cho kịch bản PHÂN TÍCH REVIEW & SENTIMENT:
- Tóm tắt cảm xúc chung của khách hàng (tích cực, tiêu cực, trung tính) cho từng nhóm sản phẩm / brand / platform (nếu thấy được).
- Chỉ ra các vấn đề nổi bật mà khách hàng hay phàn nàn (chất lượng, giao hàng, đóng gói, bảo hành, v.v.).
- Đề xuất các hành động cải thiện ưu tiên (ví dụ: 'Cải thiện thời gian giao hàng cho nhóm sản phẩm A trên sàn Tiki').
"""
        else:
            base_prompt += """
            
III. Yêu cầu phân tích:
- Dựa trên dữ liệu trên, hãy đưa ra các insight chính và hành động đề xuất cho team kinh doanh.
"""
        
        base_prompt += """
        
IV. Output mong muốn:
- Trả về JSON với 2 trường:
  - "summary_insights": danh sách 3-7 bullet points tóm tắt các điểm đáng chú ý
  - "recommended_actions": danh sách 3-7 hành động cụ thể, có thể thực thi
        
Hãy viết ngắn gọn, rõ ràng, ưu tiên ngôn ngữ dễ hiểu cho người không chuyên dữ liệu.
"""
        
        return base_prompt
    
    def _get_fallback_response(self, scenario: str, dss_result: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Fallback rule-based khi AI không trả ra được nội dung hữu ích.
        Luôn trả về:
          - summary_insights: List[str]
          - recommended_actions: List[str]
        """
        kpi = dss_result.get("kpi_summary", {}) or {}
        table_data = dss_result.get("table_data", []) or []

        summary_insights: List[str] = []
        recommended_actions: List[str] = []

        # ===== 1. Insight chung từ KPI cơ bản =====
        total_products = (
            kpi.get("total_products")
            or kpi.get("total_items")
            or kpi.get("num_products")
        )
        if total_products:
            summary_insights.append(
                f"Hệ thống đang phân tích khoảng {int(total_products)} sản phẩm trong phạm vi lựa chọn."
            )

        avg_price = kpi.get("avg_price")
        if avg_price:
            summary_insights.append(
                f"Mức giá trung bình hiện tại của nhóm sản phẩm này khoảng {avg_price:,.0f} VND."
            )

        avg_rating = kpi.get("avg_rating")
        total_reviews = kpi.get("total_reviews") or kpi.get("review_count")
        if avg_rating and total_reviews:
            summary_insights.append(
                f"Điểm rating trung bình khoảng {avg_rating:.2f} với tổng số ~{int(total_reviews)} đánh giá."
            )
        elif avg_rating:
            summary_insights.append(
                f"Điểm rating trung bình khoảng {avg_rating:.2f}."
            )

        # ===== 2. Logic theo từng scenario =====
        if scenario == "price_prediction":
            # Dùng KPI: num_products, num_with_recommendation, current_revenue, projected_revenue, expected_revenue_uplift_pct, avg_confidence
            num_products = kpi.get("num_products", 0)
            num_with_reco = kpi.get("num_with_recommendation", 0)
            current_rev = kpi.get("current_revenue")
            projected_rev = kpi.get("projected_revenue")
            uplift_pct = (kpi.get("expected_revenue_uplift_pct") or 0.0) * 100
            avg_conf_pct = (kpi.get("avg_confidence") or 0.0) * 100

            if num_products:
                summary_insights.append(
                    f"Hệ thống đang phân tích {num_products} sản phẩm, trong đó {num_with_reco} sản phẩm có khuyến nghị điều chỉnh giá."
                )

            if current_rev is not None and projected_rev is not None:
                diff = projected_rev - current_rev
                summary_insights.append(
                    f"Nếu áp dụng mức giá đề xuất, doanh thu dự kiến có thể thay đổi từ khoảng {current_rev:,.0f} VND "
                    f"lên {projected_rev:,.0f} VND (chênh lệch ~{diff:,.0f} VND)."
                )

            summary_insights.append(
                f"Độ tin cậy trung bình của mô hình dự báo giá khoảng {avg_conf_pct:.1f}%, "
                f"với kỳ vọng biến động doanh thu khoảng {uplift_pct:.1f}% so với hiện tại."
            )

            # Heuristic nhỏ từ table_data: sản phẩm nên giảm giá
            high_discount_candidates = [
                row for row in table_data
                if isinstance(row, dict)
                and row.get("current_price") not in (None, 0)
                and row.get("recommended_price") is not None
                and (row["current_price"] - row["recommended_price"]) / row["current_price"] >= 0.1
            ]
            if high_discount_candidates:
                summary_insights.append(
                    f"Có khoảng {len(high_discount_candidates)} sản phẩm đang có giá hiện tại cao hơn "
                    "ít nhất 10% so với giá mô hình đề xuất."
                )

            recommended_actions.append(
                "Ưu tiên áp dụng khuyến nghị giá cho nhóm sản phẩm có doanh thu hiện tại cao "
                "và mức độ tin cậy của mô hình > 70% để tối ưu tác động."
            )
            recommended_actions.append(
                "Thử nghiệm điều chỉnh giá theo từng nhóm (giảm 5–10%) và theo dõi doanh thu thực tế theo ngày/tuần "
                "để hiệu chỉnh lại mô hình nếu cần."
            )

        elif scenario == "product_recommendation":
            # KPI: num_source_products, num_recommendations, avg_similarity, avg_orders_for_recommended
            num_src = kpi.get("num_source_products", 0)
            num_reco = kpi.get("num_recommendations", 0)
            avg_sim = (kpi.get("avg_similarity") or 0.0) * 100
            avg_orders = kpi.get("avg_orders_for_recommended") or 0.0

            # Đếm số gợi ý / source
            source_counts: Dict[str, int] = {}
            for row in table_data:
                if isinstance(row, dict):
                    src = row.get("source_product_key") or row.get("source_product_id") or "unknown"
                    source_counts[src] = source_counts.get(src, 0) + 1

            if source_counts:
                avg_reco_per_src = sum(source_counts.values()) / max(len(source_counts), 1)
                summary_insights.append(
                    f"Hệ thống đang gợi ý trung bình khoảng {avg_reco_per_src:.1f} sản phẩm liên quan cho mỗi sản phẩm gốc."
                )

            if num_reco == 0:
                summary_insights.append(
                    "Không tìm thấy gợi ý sản phẩm nào với bộ lọc hiện tại (platform, category, min_similarity...)."
                )
                summary_insights.append(
                    "Điều này thường xảy ra khi ngưỡng độ tương đồng đặt quá cao hoặc dữ liệu gợi ý trong khoảng thời gian này chưa đủ."
                )
            else:
                summary_insights.append(
                    f"Tổng cộng có {num_reco} gợi ý cho {num_src} sản phẩm nguồn, với độ tương đồng trung bình khoảng {avg_sim:.1f}%."
                )
                summary_insights.append(
                    f"Các sản phẩm được gợi ý có trung bình ~{avg_orders:.0f} đơn hàng, phù hợp để làm ứng viên cross-sell/upsell."
                )

            recommended_actions.append(
                "Ưu tiên hiển thị 3–5 gợi ý có similarity cao nhất ngay dưới trang chi tiết sản phẩm để tăng tỉ lệ cross-sell."
            )
            recommended_actions.append(
                "Tạo combo/bundle cho những cặp sản phẩm thường xuyên được gợi ý cùng nhau, đặc biệt là các mẫu Oppo A3x/A5i/A58 trong cùng phân khúc."
            )
            recommended_actions.append(
                "Theo dõi tỉ lệ click và add-to-cart trên từng gợi ý để tinh chỉnh thêm mô hình recommendation."
            )

        elif scenario == "review_sentiment":
            # KPI: num_products, total_reviews, avg_positive_pct, avg_negative_pct, num_products_with_critical_negative, avg_rating
            total_reviews = kpi.get("total_reviews", 0)
            avg_pos = (kpi.get("avg_positive_pct") or 0.0) * 100
            avg_neg = (kpi.get("avg_negative_pct") or 0.0) * 100
            num_critical = kpi.get("num_products_with_critical_negative", 0)
            avg_rating = kpi.get("avg_rating")

            if total_reviews:
                summary_insights.append(
                    f"Hệ thống đang phân tích {int(total_reviews)} review, "
                    f"với tỷ lệ tích cực trung bình khoảng {avg_pos:.1f}% và tiêu cực khoảng {avg_neg:.1f}%."
                )
            if avg_rating:
                summary_insights.append(
                    f"Điểm rating trung bình toàn bộ nhóm sản phẩm khoảng {avg_rating:.2f}."
                )
            if num_critical:
                summary_insights.append(
                    f"Có {num_critical} sản phẩm đang có tỷ lệ review tiêu cực ở mức đáng lo (critical negative)."
                )

            if avg_neg >= 30:
                recommended_actions.append(
                    "Tập trung xử lý ngay các sản phẩm có tỷ lệ review tiêu cực cao (>= 30%), xem chi tiết nội dung review 1–2 sao."
                )
            else:
                recommended_actions.append(
                    "Tỷ lệ review tiêu cực trung bình đang ở mức chấp nhận được, nhưng vẫn nên theo dõi nhóm sản phẩm có nhiều phản hồi xấu."
                )
            recommended_actions.append(
                "Phân nhóm các vấn đề chính trong review (chất lượng, giao hàng, bảo hành, đóng gói...) để xây dựng action plan cụ thể."
            )

        # ===== 3. Fallback chung nếu vẫn trống =====
        if not summary_insights:
            summary_insights.append(
                "Hệ thống chưa đủ thông tin nổi bật để rút ra insight chi tiết, nhưng dữ liệu vẫn có thể dùng cho các phân tích chi tiết hơn."
            )

        if not recommended_actions:
            recommended_actions.append(
                "Tiếp tục theo dõi dữ liệu trong các ngày tới và kết hợp với các báo cáo chi tiết để đưa ra quyết định kinh doanh phù hợp."
            )

        return {
            "summary_insights": summary_insights,
            "recommended_actions": recommended_actions,
        }


_ai_summarizer_instance: Optional[AISummarizer] = None


def get_ai_summarizer() -> AISummarizer:
    """
    Trả về 1 instance AISummarizer dùng chung (singleton).
    DSSService, các router khác sẽ gọi hàm này.
    """
    global _ai_summarizer_instance

    if _ai_summarizer_instance is None:
        _ai_summarizer_instance = AISummarizer()
        provider_names = (
            ", ".join(p.name for p in _ai_summarizer_instance.providers)
            or "none"
        )
        logger.info(
            "AI Summarizer initialized with providers: %s",
            provider_names,
        )

    return _ai_summarizer_instance
