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
  scope_mode?: 'top_n' | 'specific_products';
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
  required_inputs: string[];
  optional_inputs: string[];
}

export interface DSSScenariosResponse {
  scenarios: DSSScenario[];
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

// AI Summary API
export interface AISummarizeRequest {
  model_type: string;
  ml_results: any;
  business_context?: {
    platform?: string;
    product_key?: string;
    category?: string;
  };
}

export interface AISummarizeResponse {
  summary: string;
  insights: string[];
  recommendations: Array<{
    type: string;
    title: string;
    description: string;
    impact: string;
    effort: string;
    priority: string;
  }>;
  anomalies: string[];
  risks: string[];
}

/**
 * Get AI Summary for DSS Results
 */
export const getAISummary = async (data: AISummarizeRequest): Promise<AISummarizeResponse> => {
  const response = await api.post('/v1/ai/summarize', data);
  return response.data;
};
