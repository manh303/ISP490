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

/** Source System */
export interface SourceSystem {
  source_id: number;
  code: string;
  name: string;
  owner_contact: string;
  dataset_count: number;
}

/** Source System Detail */
export interface SourceSystemDetail extends SourceSystem {
  datasets: Dataset[];
}

/** Dataset */
export interface Dataset {
  dataset_id: number;
  layer: string;
  schema_name: string;
  table_name: string;
  dataset_type: string;
  pii_class: string | null;
  retention_days: number | null;
}

/** Dataset Detail */
export interface DatasetDetail {
  dataset_id: number;
  source_code: string;
  source_name: string;
  layer: string;
  schema_name: string;
  table_name: string;
  dataset_type: string;
  pii_class: string | null;
  retention_days: number | null;
  created_at: string;
  updated_at: string;
  row_count: number | null;
  size_mb: number | null;
  last_loaded_at: string | null;
  freshness_hours: number | null;
  upstream_sources: any[];
  downstream_targets: any[];
  quality_issues_count: number;
  expectations_count: number;
}

/** Schema */
export interface Schema {
  schema_name: string;
  table_count: number;
  total_rows: number | null;
  total_size_gb: number | null;
}

/** Table in Schema */
export interface TableInSchema {
  dataset_id: number;
  table_name: string;
  dataset_type: string;
  pii_class: string | null;
  source_code: string;
  row_count: number | null;
  size_mb: number | null;
  last_loaded_at: string | null;
}

/* ------------------------- API Functions ------------------------- */

/** Get All Source Systems */
export const getAllSourceSystems = async (): Promise<SourceSystem[]> => {
  const response = await api.get('/v1/business-metadata/sources');
  return response.data;
};

/** Get Source System Details */
export const getSourceSystemDetails = async (code: string): Promise<SourceSystemDetail> => {
  const response = await api.get(`/v1/business-metadata/sources/${code}`);
  return response.data;
};

/** Get All Datasets */
export const getAllDatasets = async (params?: {
  layer?: string;
  source_code?: string;
  pii_only?: boolean;
}): Promise<DatasetDetail[]> => {
  const response = await api.get('/v1/business-metadata/catalog/datasets', { params });
  return response.data;
};

/** Get Dataset Details */
export const getDatasetDetails = async (dataset_id: number): Promise<DatasetDetail> => {
  const response = await api.get(`/v1/business-metadata/catalog/datasets/${dataset_id}`);
  return response.data;
};

/** Search Data Catalog */
export const searchDataCatalog = async (q: string, limit: number = 50): Promise<DatasetDetail[]> => {
  const response = await api.get('/v1/business-metadata/catalog/search', {
    params: { q, limit }
  });
  return response.data;
};

/** Get All Schemas */
export const getAllSchemas = async (): Promise<Schema[]> => {
  const response = await api.get('/v1/business-metadata/catalog/schemas');
  return response.data;
};

/** Get Tables in Schema */
export const getTablesInSchema = async (schema_name: string): Promise<TableInSchema[]> => {
  const response = await api.get(`/v1/business-metadata/catalog/schemas/${schema_name}/tables`);
  return response.data;
};

/** Business Term */
export interface BusinessTerm {
  term_id: number;
  term_name: string;
  definition: string;
  steward: string;
  status: string;
  related_datasets?: any[];
}

/** Business Term Detail */
export interface BusinessTermDetail extends BusinessTerm {
  related_datasets: any[];
}

/** Expectation */
export interface Expectation {
  exp_id: number;
  dataset_id: number;
  schema_name: string;
  table_name: string;
  name: string;
  severity: string;
  check_sql: string;
  owner: string | null;
  tags: string | null;
  last_check_passed: boolean | null;
  last_check_time: string | null;
}

/** Expectation Result */
export interface ExpectationResult {
  check_time: string;
  passed: boolean;
  failed_count: number;
  details: string;
}

/** Job */
export interface Job {
  job_id: number;
  job_name: string;
  owner: string;
  schedule: string;
  active: boolean;
  related_datasets: any[] | null;
}

/** Job Detail */
export interface JobDetail extends Job {
  // Add more fields if needed
}

/** Get All Business Terms */
export const getAllBusinessTerms = async (params?: { status?: string }): Promise<BusinessTerm[]> => {
  const response = await api.get('/v1/business-metadata/glossary/terms', { params });
  return response.data;
};

/** Create Business Term */
export const createBusinessTerm = async (params: {
  term_name: string;
  definition: string;
  steward?: string;
  status?: string;
}): Promise<BusinessTerm> => {
  const response = await api.post('/v1/business-metadata/glossary/terms', null, { params });
  return response.data;
};

/** Get Business Term Detail */
export const getBusinessTermDetail = async (term_id: number): Promise<BusinessTermDetail> => {
  const response = await api.get(`/v1/business-metadata/glossary/terms/${term_id}`);
  return response.data;
};

/** Search Business Glossary */
export const searchBusinessGlossary = async (q: string, limit: number = 50): Promise<BusinessTerm[]> => {
  const response = await api.get('/v1/business-metadata/glossary/search', { params: { q, limit } });
  return response.data;
};

/** Get All Data Expectations */
export const getAllExpectations = async (params?: {
  severity?: string;
  dataset_id?: number;
}): Promise<Expectation[]> => {
  const response = await api.get('/v1/business-metadata/expectations', { params });
  return response.data;
};

/** Create Data Expectation */
export const createExpectation = async (params: {
  dataset_id: number;
  name: string;
  severity: string;
  check_sql: string;
  owner?: string;
  tags?: string;
}): Promise<Expectation> => {
  const response = await api.post('/v1/business-metadata/expectations', null, { params });
  return response.data;
};

/** Get Expectation Check Results */
export const getExpectationResults = async (exp_id: number, limit: number = 20): Promise<ExpectationResult[]> => {
  const response = await api.get(`/v1/business-metadata/expectations/${exp_id}/results`, { params: { limit } });
  return response.data;
};

/** Get All Jobs */
export const getAllJobs = async (params?: { active_only?: boolean }): Promise<Job[]> => {
  const response = await api.get('/v1/business-metadata/jobs', { params });
  return response.data;
};

/** Get Job Details */
export const getJobDetails = async (job_id: number): Promise<JobDetail> => {
  const response = await api.get(`/v1/business-metadata/jobs/${job_id}`);
  return response.data;
};
