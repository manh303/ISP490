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

export interface MLModel {
  model_name: string;
  model_type: string;
  model_version: string;
  training_data_until: string;
  metrics: Record<string, any>;
  status: string;
  model_sk: number;
  created_at: string;
}

export interface CreateModelRequest {
  model_name: string;
  model_type: string;
  model_version: string;
  training_data_until: string;
  metrics: Record<string, any>;
  status: string;
}

export interface UpdateModelRequest {
  training_data_until?: string;
  metrics?: Record<string, any>;
  status?: string;
}

export interface PricePredictionHistory {
  product_key: string;
  platform_code: string;
  model_name: string;
  model_version: string;
  points: {
    date: string;
    platform_code: string;
    product_key: string;
    product_name: string;
    model_name: string;
    model_version: string;
    predicted_price: number;
    ci_lower: number;
    ci_upper: number;
    run_id: string;
  }[];
}

export interface OnlinePricePredictionRequest {
  platform_code: string;
  product_key: string;
  current_price: number;
  avg_rating: number;
  review_count: number;
  model_name: string;
  model_version: string;
}

export interface OnlinePricePredictionResponse {
  predicted_price: number;
  ci_lower: number;
  ci_upper: number;
  model_name: string;
  model_version: string;
  latency_ms: number;
}

export interface Recommendations {
  source_product_key: string;
  platform_code: string;
  model_name: string;
  model_version: string;
  date: string;
  recommendations: {
    rank: number;
    recommended_product_key: string;
    product_name: string;
    similarity_score: number;
    min_price: number;
    avg_rating: number;
  }[];
}

export interface SentimentSummary {
  product_key: string;
  platform_code: string;
  model_name: string;
  model_version: string;
  from_date: string;
  to_date: string;
  points: {
    date: string;
    product_key: string;
    platform_code: string;
    total_reviews: number;
    positive: number;
    negative: number;
    neutral: number;
    positive_ratio: number;
  }[];
}

export interface OnlineSentimentRequest {
  platform_code: string;
  product_key: string;
  review_text: string;
  model_name: string;
  model_version: string;
}

export interface OnlineSentimentResponse {
  label: string;
  score: number;
  model_name: string;
  model_version: string;
  latency_ms: number;
}

export interface DSSRunRequest {
  model_type: 'price_prediction' | 'product_recommendation' | 'review_sentiment';
  input_data: Record<string, any>;
}

export interface DSSRunResponse {
  model_results: Record<string, any>;
  charts_data?: any[];
  tables_data?: any[];
  metrics?: Record<string, any>;
}

export interface AISummarizeRequest {
  model_type: string;
  ml_results: Record<string, any>;
  business_context?: Record<string, any>;
}

export interface AISummarizeResponse {
  summary: string;
  insights: string[];
  anomalies: string[];
  risks: string[];
  recommendations: {
    title: string;
    description: string;
    impact: 'Cao' | 'Trung bình' | 'Thấp';
    effort: 'Cao' | 'Trung bình' | 'Thấp';
    priority: 'Cao' | 'Trung bình' | 'Thấp';
  }[];
}

/* ------------------------- API Functions ------------------------- */

/**
 * List Models
 */
export const listModels = async (params?: { type?: string; status?: string }): Promise<MLModel[]> => {
  const response = await api.get('/v1/ml/models', { params });
  return response.data;
};

/**
 * Create Model
 */
export const createModel = async (data: CreateModelRequest): Promise<MLModel> => {
  const response = await api.post('/v1/ml/models', data);
  return response.data;
};

/**
 * Get Model
 */
export const getModel = async (model_sk: number): Promise<MLModel> => {
  const response = await api.get(`/v1/ml/models/${model_sk}`);
  return response.data;
};

/**
 * Update Model
 */
export const updateModel = async (model_sk: number, data: UpdateModelRequest): Promise<MLModel> => {
  const response = await api.patch(`/v1/ml/models/${model_sk}`, data);
  return response.data;
};

/**
 * Get Price Prediction History
 */
export const getPricePredictionHistory = async (params: {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  model_name?: string;
  model_version?: string;
}): Promise<PricePredictionHistory> => {
  const response = await api.get('/v1/ml/price-predictions/history', { params });
  return response.data;
};

/**
 * Online Price Prediction
 */
export const onlinePricePrediction = async (data: OnlinePricePredictionRequest): Promise<OnlinePricePredictionResponse> => {
  const response = await api.post('/v1/ml/price-predictions/online', data);
  return response.data;
};

/**
 * Get Recommendations
 */
export const getRecommendations = async (params: {
  source_product_key: string;
  platform_code: string;
  model_name?: string;
  model_version?: string;
  limit?: number;
}): Promise<Recommendations> => {
  const response = await api.get('/v1/ml/recommendations', { params });
  return response.data;
};

/**
 * Get Sentiment Summary
 */
export const getSentimentSummary = async (params: {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  model_name?: string;
  model_version?: string;
}): Promise<SentimentSummary> => {
  const response = await api.get('/v1/ml/sentiment/summary', { params });
  return response.data;
};

/**
 * Online Sentiment
 */
export const onlineSentiment = async (data: OnlineSentimentRequest): Promise<OnlineSentimentResponse> => {
  const response = await api.post('/v1/ml/sentiment/online', data);
  return response.data;
};

/**
 * Get Status Summary
 */
export const getStatusSummary = async (): Promise<StatusSummary> => {
  const response = await api.get('/v1/ml/status/summary');
  return response.data;
};

/**
 * Run DSS Analysis
 */
export const runDSSAnalysis = async (data: DSSRunRequest): Promise<DSSRunResponse> => {
  const response = await api.post('/api/v1/dss/run', data);
  return response.data;
};

/**
 * Get AI Summary
 */
export const getAISummary = async (data: AISummarizeRequest): Promise<AISummarizeResponse> => {
  const response = await api.post('/api/v1/ai/summarize', data);
  return response.data;
};
