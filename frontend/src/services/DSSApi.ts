import axios from 'axios';
import Cookies from 'js-cookie';

/** API root */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://isp490.onrender.com';

/** Axios instance trỏ tới /api */
const api = axios.create({
  baseURL: `${API_BASE_URL}/api`,
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

/* ------------------------- Interceptors ------------------------- */

/** Gắn Bearer token cho mọi request */
api.interceptors.request.use(
  (config) => {
    const token = Cookies.get('access_token');
    if (token) {
      config.headers = config.headers ?? {};
      (config.headers as any).Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

/* ------------------------- Type Definitions ------------------------- */

// Price Prediction DSS
export interface PricePredictionRequest {
  from_date: string;
  to_date: string;
  platforms?: string[];
  categories?: string[];
  page?: number;
  page_size?: number;
  scope_mode?: 'top_n' | 'specific_products' | 'by_product' | 'by_category';
  top_n?: number;
  product_keys?: string[];
  max_discount_pct?: number;
  min_margin_pct?: number;
  min_confidence?: number;
  min_price_change_pct?: number;
}

export interface PricePredictionResponse {
  scenario: string;
  filters: {
    from_date: string;
    to_date: string;
    platforms: string[];
    categories: string[];
  };
  kpi_summary: {
    num_products: number;
    num_with_recommendation: number;
    current_revenue: number;
    projected_revenue: number;
    expected_revenue_uplift_pct: number;
    avg_confidence: number;
  };
  table_data: Array<{
    product_key: string;
    product_name: string;
    platform: string;
    category_name: string;
    current_price: number;
    predicted_price: number;
    price_diff: number;
    price_change_pct: number;
    current_revenue: number;
    projected_revenue: number;
    expected_revenue_change_pct: number;
    confidence: number;
    current_orders: number;
    avg_rating: number;
    total_reviews: number;
  }>;
  total_count: number;
  session_id?: number;  // Added for async AI polling
  ai_generation_status?: 'pending' | 'generating' | 'completed' | 'failed' | 'skipped';  // Added for async AI polling
  ai_summary_insights: string[];
  ai_recommended_actions: string[];
  generated_at: string;
  ai_model_used: string;
}

// Product Recommendation DSS
export interface ProductRecommendationRequest {
  from_date?: string;
  to_date?: string;
  platforms?: string[];
  categories?: string[];
  scope_mode: 'by_product' | 'by_category';
  source_product_key?: string;
  top_k?: number;
  min_similarity?: number;
  min_co_purchase_rate?: number;
}

export interface ProductRecommendationResponse {
  scenario: string;
  filters: {
    from_date?: string;
    to_date?: string;
    platforms: string[];
    categories: string[];
    scope_mode: string;
  };
  kpi_summary: {
    num_source_products: number;
    num_recommendations: number;
    avg_similarity: string;
    avg_orders_for_recommended: string;
  };
  table_data: Array<{
    source_product_key: string;
    source_product_name: string;
    recommended_product_key: string;
    recommended_product_name: string;
    platform: string;
    category_name: string;
    avg_price: string;
    total_orders: string;
    similarity_score: string;
    recommendation_type: string;
  }>;
  ai_summary_insights: string[];
  ai_recommended_actions: string[];
  generated_at: string;
  ai_model_used: string;
}

// Review Sentiment DSS
export interface ReviewSentimentRequest {
  from_date: string;
  to_date: string;
  platforms?: string[];
  categories?: string[];
  min_reviews_per_product?: number;
  sentiment_focus?: 'all' | 'only_negative' | 'only_positive';
  negative_threshold?: number;
}

export interface ReviewSentimentResponse {
  scenario: string;
  filters: {
    from_date: string;
    to_date: string;
    platforms: string[];
    categories: string[];
  };
  kpi_summary: {
    num_products: number;
    total_reviews: number;
    avg_positive_pct: number;
    avg_negative_pct: number;
    num_products_with_critical_negative: number;
    avg_rating: number;
  };
  table_data: Array<{
    product_key: string;
    product_name: string;
    platform: string;
    category_name: string;
    total_reviews: number;
    positive_count: number;
    neutral_count: number;
    negative_count: number;
    positive_pct: number;
    neutral_pct: number;
    negative_pct: number;
    avg_rating: number;
    sample_negative_reviews: Array<{
      review_body: string | null;
      rating: number;
      helpful_votes: number;
    }>;
    sample_positive_reviews: Array<{
      review_body: string | null;
      rating: number;
      helpful_votes: number;
    }>;
    top_positive_reasons: string[];
    top_negative_reasons: string[];
    is_critical: boolean;
  }>;
  ai_summary_insights: string[];
  ai_recommended_actions: string[];
  generated_at: string;
  ai_model_used: string;
}

// Product Review Details
export interface ProductReviewDetailsParams {
  product_key: string;
  sentiment_filter?: 'all' | 'positive' | 'negative' | 'neutral';
  sort_by?: 'helpful_votes' | 'rating' | 'date';
  limit?: number;
}

export interface ProductReviewDetailsResponse {
  product_key: string;
  product_name: string;
  total_reviews: number;
  sentiment_breakdown: {
    positive: number;
    neutral: number;
    negative: number;
  };
  reviews: Array<{
    review_id: string;
    rating: number;
    sentiment_label: string;
    sentiment_score: number;
    review_title?: string;
    review_body: string | null;
    helpful_votes: number;
    reviewer_name: string;
    review_date: string;
  }>;
}

// DSS Health Check
export interface DSSHealthResponse {
  status: string;
  components: {
    database: string;
    ai: {
      status: string;
      model: string;
    };
    ml_tables: {
      status: string;
      count: number;
    };
  };
}

// Data Status
export interface DataStatusResponse {
  status: string;
  latest_fact_date: string;
  latest_ml_date: string;
  days_since_last_fact: number;
  days_since_last_ml: number;
  warnings: string[];
  recommendations: string[];
}

// DSS Scenarios
export interface DSSScenario {
  key: string;
  name: string;
  description: string;
  endpoint: string;
  use_cases: string[];
  kpi_outputs?: string[];
  required_inputs: string[];
  optional_inputs: string[];
}

export interface DSSScenariosResponse {
  scenarios: DSSScenario[];
}

// DSS Decision Management
export interface DSSActionItem {
  action_type: string;
  target_level: string;
  product_key?: string;
  product_sk?: number;
  platform_sk?: number;
  category_sk?: number;
  current_value?: number;
  recommended_value?: number;
  chosen_value?: number;
  unit?: string;
  planned_start_date?: string;
  planned_end_date?: string;
  status: string;
  note?: string;
}

export interface SaveDSSDecisionRequest {
  scenario_key: string;
  session_id?: number;
  filters?: Record<string, any>;
  kpi_summary?: Record<string, any>;
  ai_summary_insights?: string[];
  ai_recommended_actions?: string[];
  date_adjustment_info?: Record<string, any>;
  title: string;
  description?: string;
  status: string;
  actions: DSSActionItem[];
}

export interface DSSDecisionSummary {
  decision_id: number;
  scenario_key: string;
  title: string;
  status: string;
  created_by: number;
  created_by_email?: string;
  created_at: string;
  num_actions: number;
}

export interface DSSDecisionListResponse {
  total: number;
  page: number;
  page_size: number;
  items: DSSDecisionSummary[];
}

export interface DSSActionItemResponse extends DSSActionItem {
  action_id: number;
  product_name?: string;
  category_name?: string;
  platform_name?: string;
}

export interface DSSDecisionDetailResponse {
  decision_id: number;
  session_id: number;
  scenario_key: string;
  title: string;
  description?: string;
  status: string;
  created_by: number;
  created_by_email?: string;
  created_at: string;
  updated_at: string;
  approved_by?: number;
  approved_by_email?: string;
  approved_at?: string;
  filters: Record<string, any>;
  kpi_summary: Record<string, any>;
  ai_summary_insights: string[];
  ai_recommended_actions: string[];
  date_adjustment_info?: Record<string, any>;
  actions: DSSActionItemResponse[];
}

/* ------------------------- API Functions ------------------------- */

/**
 * Run Price Prediction DSS Analysis
 */
export const runPricePredictionDSS = async (data: PricePredictionRequest): Promise<PricePredictionResponse> => {
  const response = await api.post('/v1/dss/price/run', data);
  return response.data;
};

/**
 * Run Product Recommendation DSS Analysis
 */
export const runProductRecommendationDSS = async (data: ProductRecommendationRequest): Promise<ProductRecommendationResponse> => {
  const response = await api.post('/v1/dss/reco/run', data);
  return response.data;
};

/**
 * Run Review Sentiment DSS Analysis
 */
export const runReviewSentimentDSS = async (data: ReviewSentimentRequest): Promise<ReviewSentimentResponse> => {
  const response = await api.post('/v1/dss/review/run', data);
  return response.data;
};

/**
 * Get Product Review Details
 */
export const getProductReviewDetails = async (params: ProductReviewDetailsParams): Promise<ProductReviewDetailsResponse> => {
  const { product_key, ...queryParams } = params;
  const response = await api.get(`/v1/dss/review/${product_key}/details`, { params: queryParams });
  return response.data;
};

/**
 * DSS Health Check
 */
export const getDSSHealth = async (): Promise<DSSHealthResponse> => {
  const response = await api.get('/v1/dss/health');
  return response.data;
};

/**
 * Get Data Status
 */
export const getDataStatus = async (): Promise<DataStatusResponse> => {
  const response = await api.get('/v1/dss/data/status');
  return response.data;
};

/**
 * List DSS Scenarios
 */
export const getDSSScenarios = async (): Promise<DSSScenariosResponse> => {
  const response = await api.get('/v1/dss/scenarios');
  return response.data;
};

/**
 * Save DSS Decision
 */
export const saveDSSDecision = async (data: SaveDSSDecisionRequest): Promise<DSSDecisionDetailResponse> => {
  const response = await api.post('/v1/dss/decisions', data);
  return response.data;
};

/**
 * List DSS Decisions
 */
export const listDSSDecisions = async (params?: {
  scenario_key?: string;
  status?: string;
  from_date?: string;
  to_date?: string;
  page?: number;
  page_size?: number;
}): Promise<DSSDecisionListResponse> => {
  const response = await api.get('/v1/dss/decisions', { params });
  return response.data;
};

/**
 * Get DSS Decision Detail
 */
export const getDSSDecisionDetail = async (decisionId: number): Promise<DSSDecisionDetailResponse> => {
  const response = await api.get(`/v1/dss/decisions/${decisionId}`);
  return response.data;
};

/**
 * Poll AI Generation Status
 * 
 * After running a DSS analysis, use this to check if async AI generation has completed.
 * Poll every 2-3 seconds until status is 'completed' or 'failed'.
 * 
 * @param sessionId - Session ID from DSS run response
 * @returns AI generation status and updated insights/actions
 */
export const pollAIGenerationStatus = async (sessionId: number): Promise<{
  session_id: number;
  ai_generation_status: 'pending' | 'generating' | 'completed' | 'failed' | 'skipped';
  ai_summary_insights?: string[];
  ai_recommended_actions?: string[];
  ai_model_used?: string;
  error_message?: string;
}> => {
  const response = await api.get(`/v1/dss/price/${sessionId}/ai-summary`);
  return response.data;
};

// ============================================
// DSS ANALYSIS SESSIONS (HISTORY)
// ============================================

export interface DSSSessionItem {
  session_id: number;
  scenario_key: string;
  scenario_name: string;
  filters: Record<string, any>;
  kpi_summary: Record<string, any>;
  ai_generation_status: string;
  ai_model_used: string;
  generated_at: string;
  source_endpoint: string;
  user_email?: string;
  has_decision: boolean;
  decision_id?: number;
}

export interface DSSSessionListResponse {
  total: number;
  page: number;
  page_size: number;
  items: DSSSessionItem[];
}

export interface DSSSessionDetailResponse {
  session_id: number;
  scenario_key: string;
  scenario_name: string;
  filters: Record<string, any>;
  kpi_summary: Record<string, any>;
  ai_summary_insights: string[];
  ai_recommended_actions: string[];
  date_adjustment_info: Record<string, any>;
  ai_generation_status: string;
  ai_model_used: string;
  generated_at: string;
  source_endpoint: string;
  user_email?: string;
  decision?: {
    decision_id: number;
    title: string;
    status: string;
  };
}

/**
 * List DSS Analysis Sessions (History)
 * 
 * Get paginated list of DSS runs that may or may not have been saved as decisions.
 */
export const listDSSSessions = async (params: {
  scenario_key?: string;
  from_date?: string;
  to_date?: string;
  page?: number;
  page_size?: number;
}): Promise<DSSSessionListResponse> => {
  const response = await api.get('/v1/dss/sessions', { params });
  return response.data;
};

/**
 * Get DSS Session Detail
 * 
 * Get full details of a specific DSS analysis session including KPIs and AI insights.
 */
export const getDSSSessionDetail = async (sessionId: number): Promise<DSSSessionDetailResponse> => {
  const response = await api.get(`/v1/dss/sessions/${sessionId}`);
  return response.data;
};
