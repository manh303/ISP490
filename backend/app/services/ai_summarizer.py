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
                    temperature=0.3,  # Lower for more deterministic outputs
                    max_tokens=2000,  # Increased for detailed product-level actions
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
            os.getenv("GEMINI_MODEL", "gemini-2.5-pro").replace("models/", ""),
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
                    selected_model = "models/gemini-2.5-pro"
                
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
            "temperature": 0.3,  # Lower for more deterministic outputs
            "max_output_tokens": 2000,  # Increased for detailed product-level actions
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
                    # If AI output is too short/incomplete, use fallback entirely instead of merging
                    # to avoid mixed language/quality issues
                    MIN_INSIGHTS = 3
                    MIN_ACTIONS = 3
                    if len(summary) < MIN_INSIGHTS or len(actions) < MIN_ACTIONS:
                        logger.warning(
                            f"{provider.name} returned incomplete response "
                            f"({len(summary)} insights, {len(actions)} actions). "
                            "Using rule-based fallback instead."
                        )
                        self.provider_stats[provider.name]["failure"] += 1
                        # Don't merge - use full fallback to maintain consistency
                        return self._get_fallback_response(scenario, dss_result_raw)

                    # Limit output size
                    result = {
                        "summary_insights": summary[:7],
                        "recommended_actions": actions[:7],
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
        return """You are a business data analysis expert.

Your task is to analyze DSS (Decision Support System) data provided and:
1. Identify patterns, trends, and anomalies in the data
2. Provide useful business insights
3. Recommend specific, clear, actionable steps

You must return results in JSON format with the structure:
{
  "summary_insights": ["...", "..."],
  "recommended_actions": ["...", "..."]
}

Where:
- summary_insights: key observations, analysis, highlights from the data
- recommended_actions: specific, prioritized, easy-to-understand actions for business teams

Output language: **English only**. Be concise, clear, avoid overly academic terminology.
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
        
        base_prompt = f"Analysis scenario: {scenario}.\n\n"
        
        base_prompt += "I. Overall KPI Summary:\n"
        base_prompt += json.dumps(kpi_summary, ensure_ascii=False, separators=(",", ":"))
        base_prompt += "\n\nII. Detailed Data (limited to top 50 rows for summary):\n"
        base_prompt += json.dumps(limited_table_data, ensure_ascii=False, separators=(",", ":"))
        
        if truncated:
            base_prompt += "\n\n(Note: Data has been truncated to fit model context. Focus on overall trends.)"
        
        # Add scenario-specific instructions
        if scenario == "price_prediction":
            # Sort products by business impact for focused analysis
            sorted_products = sorted(
                limited_table_data,
                key=lambda x: abs(x.get("expected_revenue_change_pct", 0) or 0) * (x.get("current_revenue", 0) or 1),
                reverse=True
            )[:10]  # Top 10 high-impact products
            
            # Format product details with all key metrics
            product_details = "\n".join([
                f"- **{p.get('product_name', 'N/A')}** ({p.get('platform', 'N/A')}): "
                f"Current ₫{p.get('current_price', 0):,.0f} → Recommended ₫{p.get('recommended_price', 0):,.0f} "
                f"({p.get('price_change_pct', 0):.1%} change), "
                f"Current Revenue: ₫{p.get('current_revenue', 0):,.0f}, "
                f"Projected Revenue: ₫{p.get('projected_revenue', 0):,.0f} "
                f"({p.get('expected_revenue_change_pct', 0):.1%} uplift), "
                f"Confidence: {p.get('confidence', 0):.1%}, "
                f"Orders: {p.get('current_orders', 0):,}"
                for p in sorted_products
            ])
            
            # Get constraints from request if available
            request_data = dss_result_raw.get("input", {})
            constraints = {
                "min_confidence": request_data.get("min_confidence", 0.7),
                "min_price_change": request_data.get("min_price_change_pct", 0.02),
                "max_discount": request_data.get("max_discount_pct", 0.15),
            }
            
            base_prompt += f"""

III. PRICE OPTIMIZATION ANALYSIS - STRICT REQUIREMENTS

**BUSINESS CONSTRAINTS**:
- Minimum confidence threshold: {constraints['min_confidence']:.0%}
- Minimum price change: {constraints['min_price_change']:.1%}
- Maximum discount allowed: {constraints['max_discount']:.1%}

**TOP 10 HIGH-IMPACT PRODUCTS** (sorted by revenue impact):
{product_details}

---

**MANDATORY OUTPUT STRUCTURE** - You MUST follow this EXACTLY:

1. **summary_insights** (exactly 5 insights):
   Each insight MUST:
   - Reference AT LEAST 1 specific product name OR category/platform from the data above
   - Include specific numbers (prices in ₫, percentages, revenue amounts)
   - Format: "[CATEGORY/PLATFORM] Specific observation with actual numbers"
   
   ✅ GOOD Example: "Laptop category: 'Dell Inspiron 15' can increase from ₫15,000,000 to ₫16,500,000 (+10.0%) with 85% confidence, projecting +12% revenue uplift from current ₫50,000,000"
   ❌ BAD Example: "Products show potential for price optimization" (too vague, no product names, no numbers)

2. **recommended_actions** (exactly 7 actions):
   
   **MUST include 3 PRODUCT-LEVEL actions** (use actual product names from table):
   Format: "[PRIORITY: HIGH] Increase/decrease price for {{{{product_name}}}} ({{{{platform}}}}) from ₫{{{{current}}}} to ₫{{{{recommended}}}} ({{{{change_pct}}}}%), expecting {{{{revenue_pct}}}}% revenue change. Conditions: confidence={{{{conf}}}}%, current_orders={{{{orders}}}}"
   
   **MUST include 2 CATEGORY/PLATFORM-LEVEL actions**:
   Format: "[CATEGORY: {{name}}] Adjust pricing for {{platform}} products by +{{min}}% to +{{max}}%, prioritizing items with confidence ≥80% and revenue share >5%"
   
   **MUST include 1 A/B TEST action**:
   Must specify exact test parameters, product name, duration (days/weeks), success metrics
   
   **MUST include 1 MONITORING action**:
   Must specify KPIs to track, alert thresholds, review frequency

**CRITICAL RULES**:
- NEVER use phrases like "theအံ system is analyzing" or "prioritize high-revenue products" without specifics
- ALWAYS use actual product names from the table above (copy exactly as shown)
- ALWAYS include specific numbers: current price, recommended price, percentages
- ALWAYS specify platform (tiki/lazada/shopee)
- DO NOT invent products not in the data
- If current_revenue < ₫1,000 for ALL products → treat as "new products scenario" and focus on market testing actions instead of revenue optimization

**OUTPUT FORMAT** - Return ONLY valid JSON (no markdown, no code blocks):
{{
  "summary_insights": ["insight1 with product name and numbers", "insight2 with category and %", "insight3...", "insight4...", "insight5..."],
  "recommended_actions": ["[PRIORITY: HIGH] Product action 1", "[PRIORITY: HIGH] Product action 2", "[PRIORITY: HIGH] Product action 3", "[CATEGORY: X] Category action 1", "[CATEGORY: Y] Category action 2", "[A/B TEST] Test action", "[MONITORING] Monitor action"]
}}
"""
        elif scenario == "product_recommendation":
            base_prompt += """

III. PRODUCT RECOMMENDATION ANALYSIS REQUIREMENTS:

DATA PROVIDED:
- Overall KPIs: number of source products, number of recommendations, average similarity, average orders.
- Detailed recommendation list with: source product, recommended product, platform, category, average price, total orders, similarity score.

ANALYZE THE SPECIFIC DATA PROVIDED TO DELIVER:

1. **Specific insights about recommendation trends**:
   - Which product groups are recommended most? (STATE ACTUAL PRODUCT NAMES FROM DATA)
   - Highest similarity in which category/platform? (STATE NUMBERS %)
   - Product pairs frequently recommended together (potential combos).

2. **Recommendation quality assessment**:
   - Do recommended products have high ratings (>4.0) and many orders?
   - Are prices reasonable compared to source products?
   - Which platform has the best quality recommendations?

3. **Recommendation system optimization suggestions**:
   - Increase weight for products with high ratings/many orders.
   - Adjust similarity thresholds for each category.
   - Add seasonal factors or market trends.

4. **Specific marketing actions**:
   - Create combos/bundles for frequently paired products.
   - Cross-sell campaigns for categories with potential.
   - How to display recommendations on product detail pages.

CRITICAL REQUIREMENTS:
- ✅ MUST be based on the provided JSON data, DO NOT use generic information.
- ✅ Return exactly 4-6 "summary_insights" and 4-6 "recommended_actions".
- ✅ EACH insight/action MUST contain at least 1 of the following:
  • Specific product name from table_data
  • KPI numbers (%, quantity, values)
  • Platform name (tiki, lazada...)
  • Specific category name
  • Similarity score
  
📌 GOOD OUTPUT EXAMPLE (SPECIFIC):
{
  "summary_insights": [
    "15 recommendations for 3 source products, avg similarity 85%",
    "Product 'Oppo A58 Phone' recommended most (5 times) with similarity ≥0.9",
    "Platform tiki accounts for 80% recommendations, mainly Mobile Phones category"
  ],
  "recommended_actions": [
    "Create combo 'Oppo A58 + Bluetooth Headphones' as recommended together with similarity 0.95",
    "Display 4 recommendations with similarity ≥0.8 on tiki product pages to optimize conversion",
    "Focus A/B test cross-sell for Mobile Phones category (60% of recommendations)"
  ]
}

❌ BAD OUTPUT EXAMPLE (GENERIC - AVOID):
{
  "summary_insights": [
    "System has many quality recommendations",  ← No numbers
    "Products have high similarity"  ← No product names
  ],
  "recommended_actions": [
    "Should create product combos",  ← Not specific which products
    "Improve recommendation display"  ← No details how
  ]
}

If data is limited (<5 recommendations), still state the exact number and provide specific improvement suggestions.
"""
        elif scenario == "review_sentiment":
            base_prompt += """

III. REVIEW & SENTIMENT ANALYSIS - STRICT REQUIREMENTS

**MANDATORY OUTPUT STRUCTURE** - You MUST follow this EXACTLY:

1. **summary_insights** (exactly 5 insights):
   Each insight MUST:
   - Reference AT LEAST 1 specific product name OR category/platform from the data
   - Include sentiment percentages (% positive, negative, neutral)
   - Include specific numbers (review count, rating values)
   - Format: "[CATEGORY/PLATFORM] Specific observation with sentiment % and numbers"
   
   ✅ GOOD Example: "Mobile Phones category: 'Samsung Galaxy A70' has 68% positive sentiment (avg 4.2/5, 150 reviews) but 25% negative reviews mention battery life issues"
   ❌ BAD Example: "Products have mixed reviews" (too vague, no product names, no percentages)

2. **recommended_actions** (exactly 7 actions):
   
   **MUST include 3 PRODUCT-LEVEL actions** (use actual product names from data):
   Format: "[PRIORITY: HIGH] Address {specific issue} for '{product_name}' ({platform}): {negative_sentiment}% of {review_count} reviews mention {issue}. Action: {specific solution with timeline}"
   
   **MUST include 2 CATEGORY/PLATFORM-LEVEL actions**:
   Format: "[CATEGORY: {name}] Improve {specific aspect} across {platform} - affects {product_count} products with avg {sentiment}% negative sentiment. Focus on {top 2-3 issues}"
   
   **MUST include 1 CUSTOMER SERVICE action**:
   Must specify response targets, training needs, escalation procedures
   
   **MUST include 1 MONITORING action**:
   Must specify sentiment tracking frequency, alert thresholds, review response SLA

**CRITICAL RULES**:
- NEVER use phrases like "monitor customer feedback" or "improve product quality" without specifics
- ALWAYS use actual product names from the table above (copy exactly as shown)
- ALWAYS include sentiment percentages and review counts
- ALWAYS specify platform (tiki/lazada/shopee)
- ALWAYS mention specific issues (delivery, quality, packaging, customer service, etc.)
- DO NOT invent products not in the data
- If all products have >80% positive sentiment → focus on "maintain quality" actions, not false problems

**DATA INTERPRETATION**:
- sentiment_score > 0.6 = Positive
- sentiment_score 0.4-0.6 = Neutral
- sentiment_score < 0.4 = Negative
- Calculate percentages from review counts
- Identify common complaint themes from sentiment patterns

**OUTPUT FORMAT** - Return ONLY valid JSON (no markdown, no code blocks):
{
  "summary_insights": ["insight1 with product and %", "insight2 with category and numbers", "insight3...", "insight4...", "insight5..."],
  "recommended_actions": ["[PRIORITY: HIGH] Product action 1", "[PRIORITY: HIGH] Product action 2", "[PRIORITY: HIGH] Product action 3", "[CATEGORY: X] Category action 1", "[CATEGORY: Y] Category action 2", "[CUSTOMER SERVICE] Service action", "[MONITORING] Monitor action"]
}

**IMPORTANT: All output must be in English.**
"""
        else:
            base_prompt += """
            
III. Analysis Requirements:
- Based on the data above, provide key insights and recommended actions for the business team.
"""
        
        base_prompt += """
        
IV. Desired Output:
- Return JSON with 2 fields:
  - "summary_insights": list of 3-7 bullet points summarizing key findings
  - "recommended_actions": list of 3-7 specific, actionable steps
        
Write concisely, clearly, using language accessible to non-technical business stakeholders.
**IMPORTANT: All output must be in English.**
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
                f"Analyzing approximately {int(total_products)} products within the selected scope."
            )

        avg_price = kpi.get("avg_price")
        if avg_price:
            summary_insights.append(
                f"Average current price for this product group is approximately ₫{avg_price:,.0f}."
            )

        avg_rating = kpi.get("avg_rating")
        total_reviews = kpi.get("total_reviews") or kpi.get("review_count")
        if avg_rating and total_reviews:
            summary_insights.append(
                f"Average rating is approximately {avg_rating:.2f} with total ~{int(total_reviews)} reviews."
            )
        elif avg_rating:
            summary_insights.append(
                f"Average rating is approximately {avg_rating:.2f}."
            )

        # ===== 2. Logic theo từng scenario =====
        if scenario == "price_prediction":
            # Dùng KPI: num_products, num_with_recommendation, current_revenue, projected_revenue, expected_revenue_uplift_pct, avg_confidence
            num_products = kpi.get("num_products", 0)
            num_with_reco = kpi.get("num_with_recommendation", 0)
            current_total_rev = kpi.get("current_total_revenue", 0) or kpi.get("current_revenue", 0)
            projected_total_rev = kpi.get("projected_total_revenue", 0) or kpi.get("projected_revenue", 0)
            uplift_pct = (kpi.get("expected_revenue_uplift_pct", 0) or 0.0) * 100
            avg_conf = (kpi.get("avg_confidence", 0) or 0.0) * 100

            # Sort by business impact for focused actions
            sorted_by_impact = sorted(
                table_data,
                key=lambda x: abs(x.get("expected_revenue_change_pct", 0) or 0) * (x.get("current_revenue", 0) or 1),
                reverse=True
            ) if table_data else []

            # Insight 1: Overall metrics
            if num_products:
                summary_insights.append(
                    f"Analyzed {num_products} products with {num_with_reco} having price recommendations. "
                    f"Average model confidence: {avg_conf:.1f}%"
                )

            # Insight 2: Revenue potential
            if current_total_rev and projected_total_rev:
                rev_diff = projected_total_rev - current_total_rev
                summary_insights.append(
                    f"Expected revenue impact: ₫{current_total_rev:,.0f} → ₫{projected_total_rev:,.0f} "
                    f"({uplift_pct:+.2f}% change, ₫{rev_diff:+,.0f} difference)"
                )

            # Insights 3-4: Top 2 products by impact
            for i, prod in enumerate(sorted_by_impact[:2], 1):
                summary_insights.append(
                    f"Top {i} by impact: '{prod.get('product_name', 'N/A')}' ({prod.get('platform', 'N/A')}) - "
                    f"Current ₫{prod.get('current_price', 0):,.0f}, Recommended ₫{prod.get('recommended_price', 0):,.0f} "
                    f"({prod.get('price_change_pct', 0):+.1%}), Revenue impact: {prod.get('expected_revenue_change_pct', 0):+.1%}"
                )

            # Insight 5: Distribution analysis
            high_conf_count = len([p for p in table_data if (p.get('confidence', 0) or 0) >= 0.8])
            if high_conf_count:
                summary_insights.append(
                    f"{high_conf_count} products have confidence ≥80%, making them low-risk candidates for immediate adjustment"
                )
            elif table_data:
                avg_conf_val = sum((p.get('confidence', 0) or 0) for p in table_data) / len(table_data)
                summary_insights.append(
                    f"Average confidence across products: {avg_conf_val:.1%}. Consider A/B testing before full rollout"
                )

            # Pad to 5 insights if needed
            while len(summary_insights) < 5:
                summary_insights.append("Additional market research recommended for comprehensive pricing strategy")

            # Actions 1-3: Product-level (top 3 by impact)
            for i, prod in enumerate(sorted_by_impact[:3], 1):
                action = (
                    f"[PRIORITY: HIGH] Adjust price for '{prod.get('product_name', 'N/A')}' "
                    f"({prod.get('platform', 'N/A')}) from ₫{prod.get('current_price', 0):,.0f} to "
                    f"₫{prod.get('recommended_price', 0):,.0f} ({prod.get('price_change_pct', 0):+.1%}). "
                    f"Expected revenue change: {prod.get('expected_revenue_change_pct', 0):+.1%}. "
                    f"Confidence: {prod.get('confidence', 0):.1%}, Current orders: {prod.get('current_orders', 0):,}"
                )
                recommended_actions.append(action)

            # Action 4: Category-level aggregation
            if len(sorted_by_impact) > 3:
                remaining = sorted_by_impact[3:]
                avg_change = sum((p.get('price_change_pct', 0) or 0) for p in remaining) / len(remaining)
                high_conf_remaining = [p for p in remaining if (p.get('confidence', 0) or 0) >= 0.75]
                
                # Group by category
                categories_dict = {}
                for p in remaining:
                    cat = p.get('category_name', 'Other')
                    if cat not in categories_dict:
                        categories_dict[cat] = []
                    categories_dict[cat].append(p)
                
                if categories_dict:
                    # Get largest category
                    largest_cat = max(categories_dict.items(), key=lambda x: len(x[1]))
                    cat_name, cat_products = largest_cat
                    cat_avg_change = sum((p.get('price_change_pct', 0) or 0) for p in cat_products) / len(cat_products)
                    
                    recommended_actions.append(
                        f"[CATEGORY: {cat_name}] Apply {cat_avg_change:+.1%} average price adjustment "
                        f"to {len(cat_products)} products, prioritizing {len([p for p in cat_products if (p.get('confidence',0) or 0) >= 0.75])} items with confidence ≥75%"
                    )
                else:
                     recommended_actions.append(
                        f"[CATEGORY-LEVEL] Apply {avg_change:+.1%} average price adjustment "
                        f"to remaining {len(remaining)} products, prioritizing {len(high_conf_remaining)} items with confidence ≥75%"
                    )

            # Action 5: Platform-specific strategy
            if table_data:
                platform_stats = {}
                for p in table_data:
                    platform = p.get('platform', 'unknown')
                    if platform not in platform_stats:
                        platform_stats[platform] = {
                            'count': 0,
                            'total_change': 0.0,
                            'high_conf': 0
                        }
                    platform_stats[platform]['count'] += 1
                    platform_stats[platform]['total_change'] += (p.get('price_change_pct', 0) or 0)
                    if (p.get('confidence', 0) or 0) >= 0.8:
                        platform_stats[platform]['high_conf'] += 1
                
                if platform_stats:
                    # Get platform with most recommendations
                    top_platform = max(platform_stats.items(), key=lambda x: x[1]['count'])
                    platform_name, stats = top_platform
                    avg_platform_change = stats['total_change'] / stats['count'] if stats['count'] > 0 else 0
                    
                    recommended_actions.append(
                        f"[PLATFORM: {platform_name}] Prioritize {platform_name} with {stats['count']} recommendations "
                        f"({stats['high_conf']} high-confidence). Average suggested change: {avg_platform_change:+.1%}"
                    )

            # Action 6: A/B testing
            if sorted_by_impact:
                # Find product with medium confidence (0.6-0.8)
                mid_conf_prod = next(
                    (p for p in sorted_by_impact if 0.6 <= (p.get('confidence', 0) or 0) < 0.8),
                    sorted_by_impact[0] if sorted_by_impact else None
                )
                if mid_conf_prod:
                    recommended_actions.append(
                        f"[A/B TEST] Run 2-week price test for '{mid_conf_prod.get('product_name', 'N/A')}': "
                        f"50% traffic at current ₫{mid_conf_prod.get('current_price', 0):,.0f}, "
                        f"50% at recommended ₫{mid_conf_prod.get('recommended_price', 0):,.0f}. "
                        f"Track conversion rate, revenue per session, and customer feedback"
                    )
                else:
                    recommended_actions.append(
                        "[A/B TEST] Conduct controlled experiments on 3-5 medium-confidence products (60-80%) before full rollout"
                    )

            # Action 7: Monitoring
            recommended_actions.append(
                f"[MONITORING] Track daily: (1) Revenue per product (+/-5% alert), "
                f"(2) Conversion rate changes (+/-10% alert), (3) Competitor pricing. "
                f"Weekly review for first month post-implementation"
            )

            # Pad to 7 actions if needed
            while len(recommended_actions) < 7:
                recommended_actions.append("Monitor market conditions and customer feedback for strategic price adjustments")


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

            # Phân tích chi tiết từ table_data để tạo gợi ý cụ thể
            top_recommended = {}
            platform_counts = {}
            category_counts = {}
            high_similarity_products = []  # similarity > 0.8
            
            for row in table_data[:15]:  # Phân tích 15 gợi ý hàng đầu
                if not isinstance(row, dict):
                    continue
                    
                # Thu thập tên sản phẩm được gợi ý
                rec_product = row.get("recommended_product_name", "")
                if rec_product:
                    top_recommended[rec_product] = row.get("similarity_score", 0)
                
                # Đếm platform distribution
                platform = row.get("platform", "")
                if platform:
                    platform_counts[platform] = platform_counts.get(platform, 0) + 1
                
                # Đếm category distribution
                category = row.get("category_name", "")
                if category:
                    category_counts[category] = category_counts.get(category, 0) + 1
                
                # Tìm recommendations có similarity rất cao
                sim_score = row.get("similarity_score", 0)
                if sim_score >= 0.8:
                    high_similarity_products.append({
                        "name": rec_product[:40],
                        "score": sim_score
                    })

            # ACTION 1: Combo/Bundle với tên sản phẩm thực
            if top_recommended:
                top_product_names = ", ".join(list(top_recommended.keys())[:3])
                recommended_actions.append(
                    f"Tạo combo/bundle cho những sản phẩm thường được gợi ý cùng nhau như: {top_product_names}"
                )
            else:
                recommended_actions.append(
                    "Tạo combo/bundle cho những cặp sản phẩm thường xuyên được gợi ý cùng nhau."
                )
            
            # ACTION 2: Phân tích platform/category để ưu tiên hiển thị
            if platform_counts:
                top_platform = max(platform_counts, key=platform_counts.get)
                platform_pct = (platform_counts[top_platform] / max(len(table_data), 1)) * 100
                
                if len(high_similarity_products) > 0:
                    recommended_actions.append(
                        f"Ưu tiên hiển thị {len(high_similarity_products)} gợi ý có độ tương đồng cao (≥0.8) "
                        f"trên trang chi tiết sản phẩm {top_platform} ({platform_pct:.0f}% gợi ý) để tối ưu cross-sell."
                    )
                elif category_counts:
                    top_category = max(category_counts, key=category_counts.get)
                    recommended_actions.append(
                        f"Tập trung hiển thị gợi ý cho category '{top_category}' trên platform {top_platform} "
                        f"vì đây là nhóm có nhiều recommendations nhất."
                    )
                else:
                    recommended_actions.append(
                        f"Ưu tiên hiển thị 3–5 gợi ý có similarity cao nhất trên {top_platform} "
                        f"để tăng tỉ lệ cross-sell."
                    )
            else:
                recommended_actions.append(
                    "Ưu tiên hiển thị 3–5 gợi ý có similarity cao nhất ngay dưới trang chi tiết sản phẩm."
                )
            
            # ACTION 3: A/B testing với metrics cụ thể
            if avg_sim >= 70:  # Similarity trung bình cao
                recommended_actions.append(
                    f"Chất lượng gợi ý tốt (similarity TB: {avg_sim:.0f}%). "
                    f"Theo dõi conversion rate và AOV của {num_reco} gợi ý này để đánh giá hiệu quả."
                )
            else:
                recommended_actions.append(
                    f"Similarity trung bình ở mức {avg_sim:.0f}%. "
                    f"Thử nghiệm tăng ngưỡng min_similarity lên 0.7 và theo dõi tỉ lệ click/add-to-cart."
                )
                             
        elif scenario == "review_sentiment":
            # KPI: num_products, total_reviews, avg_positive_pct, avg_negative_pct, num_products_with_critical_negative, avg_rating
            total_reviews = kpi.get("total_reviews", 0)
            avg_pos = (kpi.get("avg_positive_pct") or 0.0) * 100
            avg_neg = (kpi.get("avg_negative_pct") or 0.0) * 100
            num_critical = kpi.get("num_products_with_critical_negative", 0)
            avg_rating = kpi.get("avg_rating")
            num_products = kpi.get("num_products", 0)

            # Insight 1: Overall metrics
            if total_reviews:
                summary_insights.append(
                    f"Analyzing {int(total_reviews)} reviews across {num_products} products: "
                    f"{avg_pos:.1f}% positive sentiment, {avg_neg:.1f}% negative sentiment, "
                    f"average rating {avg_rating:.2f}/5.0"
                )

            # Insight 2: Critical products
            if num_critical:
                summary_insights.append(
                    f"{num_critical} products have critically high negative sentiment (≥30% negative reviews), "
                    f"requiring immediate attention and issue resolution"
                )
            elif avg_neg < 10:
                summary_insights.append(
                    f"Low negative sentiment rate ({avg_neg:.1f}%) indicates generally satisfied customers across product range"
                )

            # Insights 3-5: Product-level analysis from table_data
            sorted_products = sorted(
                [p for p in table_data if isinstance(p, dict)],
                key=lambda x: (x.get('negative_pct', 0) or 0),
                reverse=True
            )[:5]

            for prod in sorted_products[:3]:
                neg_pct = (prod.get('negative_pct', 0) or 0) * 100
                pos_pct = (prod.get('positive_pct', 0) or 0) * 100
                review_count = prod.get('review_count', 0)
                
                if neg_pct >= 20:
                    summary_insights.append(
                        f"'{prod.get('product_name', 'N/A')}' ({prod.get('platform', 'N/A')}): "
                        f"{neg_pct:.1f}% negative sentiment from {review_count} reviews - "
                        f"requires investigation of recurring complaint themes"
                    )

            # Pad insights if needed
            while len(summary_insights) < 5:
                if  avg_pos >= 70:
                    summary_insights.append(
                        f"Strong customer satisfaction with {avg_pos:.1f}% positive sentiment - "
                        f"focus on maintaining quality standards"
                    )
                else:
                    summary_insights.append(
                        "Additional sentiment analysis recommended for comprehensive product feedback assessment"
                    )

            # Actions 1-3: Product-level (top 3 by negative sentiment)
            for i, prod in enumerate(sorted_products[:3], 1):
                neg_pct = (prod.get('negative_pct', 0) or 0) * 100
                if neg_pct >= 15:
                    recommended_actions.append(
                        f"[PRIORITY: HIGH] Investigate '{prod.get('product_name', 'N/A')}' ({prod.get('platform', 'N/A')}): "
                        f"{neg_pct:.1f}% negative ({prod.get('review_count', 0)} reviews). "
                        f"Action: Analyze complaint themes (quality, delivery, description accuracy), "
                        f"respond to negative reviews within 48h, implement corrective measures"
                    )

            # Action 4: Category-level
            if avg_neg >= 15:
                recommended_actions.append(
                    f"[CATEGORY-LEVEL] Average negative sentiment at {avg_neg:.1f}% - "
                    f"conduct root cause analysis across top complaint categories (quality, delivery, customer service). "
                    f"Target: reduce negative sentiment to <10% within 2 months"
                )
            else:
                recommended_actions.append(
                    f"[CATEGORY-LEVEL] Maintain current quality standards (negative sentiment: {avg_neg:.1f}%). "
                    f"Monitor for emerging issues and proactively address concerns before escalation"
                )

            # Action 5: Platform-specific
            if table_data:
                platform_stats = {}
                for p in table_data[:20]:
                    if not isinstance(p, dict):
                        continue
                    platform = p.get('platform', 'unknown')
                    if platform not in platform_stats:
                        platform_stats[platform] = {'neg': [], 'count': 0}
                    platform_stats[platform]['neg'].append((p.get('negative_pct', 0) or 0) * 100)
                    platform_stats[platform]['count'] += 1
                
                if platform_stats:
                    worst_platform = max(platform_stats.items(), 
                                       key=lambda x: sum(x[1]['neg']) / len(x[1]['neg']) if x[1]['neg'] else 0)
                    platform_name, stats = worst_platform
                    avg_neg_platform = sum(stats['neg']) / len(stats['neg']) if stats['neg'] else 0
                    
                    recommended_actions.append(
                        f"[PLATFORM: {platform_name}] Focus improvement efforts on {platform_name} "
                        f"({stats['count']} products, {avg_neg_platform:.1f}% avg negative sentiment). "
                        f"Review seller compliance, fulfillment accuracy, product descriptions"
                    )

            # Action 6: Customer service
            recommended_actions.append(
                f"[CUSTOMER SERVICE] Implement 24-hour response SLA for negative reviews (1-2 stars). "
                f"Train support team on common complaint resolution. "
                f"Set escalation threshold: ≥3 negative reviews mentioning same issue within 7 days"
            )

            # Action 7: Monitoring
            recommended_actions.append(
                f"[MONITORING] Daily sentiment tracking: alert if product negative sentiment exceeds 25% or "
                f"receives ≥5 negative reviews in 24h. Weekly report on sentiment trends by category/platform. "
                f"Monthly review response rate target: ≥80% for negative reviews"
            )

            # Pad to 7 actions if needed
            while len(recommended_actions) < 7:
                recommended_actions.append(
                    "Monitor emerging sentiment patterns and adjust quality control processes accordingly"
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
