import React, { useState, useEffect } from 'react';
import PageBreadcrumb from "../../../components/common/PageBreadCrumb";
import PageMeta from "../../../components/common/PageMeta";
import { getPlatforms, getCategories, getOverviewReport, type Platform as ApiPlatform, type Category as ApiCategory, type OverviewReport as ApiOverviewReport } from '../../../services/analyticsApi';
import { getDSSHealth, getDataStatus, listDSSDecisions, getDSSDecisionDetail, type DSSHealth as ApiDSSHealth, type DataStatus as ApiDataStatus, type DSSDecisionSummary, type DSSDecisionDetail as ApiDSSDecisionDetail } from '../../../services/DSSApi';
import { getActivityLogs } from '../../../services/adminApi';
import { Button } from '../../../components/ui/figma/button';
import { Input } from '../../../components/ui/figma/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../../components/ui/figma/select';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../../components/ui/figma/table';
import { Badge } from '../../../components/ui/figma/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../../components/ui/figma/tabs';
import { Eye, Download, TrendingUp, Users, Activity, Database, Brain, TableIcon } from 'lucide-react';

// Types for our data
interface Platform extends ApiPlatform {}
interface Category extends ApiCategory {}
interface OverviewReport extends ApiOverviewReport {}

interface DSSHealth {
  status: string;
  components: {
    database: string;
    ai: { status: string; model: string };
    ml_tables: { status: string; count: number };
  };
}

interface DataStatus {
  latest_fact_date: string;
  latest_ml_date: string;
  days_since_last_fact: number;
  days_since_last_ml: number;
  warnings: string[];
  recommendations: string[];
}

interface DSSDecision {
  decision_id: number;
  title: string;
  scenario_key: string;
  status: string;
  created_by: number;
  created_by_email?: string;
  created_at: string;
}

interface DSSDecisionDetail {
  decision_id: number;
  title: string;
  scenario_key: string;
  status: string;
  filters: Record<string, any>;
  kpi_summary: Record<string, any>;
  ai_summary_insights: string[];
  ai_recommended_actions: string[];
  actions: Array<{
    action_type: string;
    target_level: string;
    product_key?: string;
    current_value?: number;
    recommended_value?: number;
    status: string;
  }>;
}

export default function AdminDashboard() {
  // Filter states
  const [fromDate, setFromDate] = useState(() => {
    const date = new Date();
    date.setDate(date.getDate() - 7);
    return date.toISOString().split('T')[0];
  });
  const [toDate, setToDate] = useState(() => new Date().toISOString().split('T')[0]);
  const [selectedPlatform, setSelectedPlatform] = useState<string>('');
  const [selectedCategory, setSelectedCategory] = useState<string>('');
  
  // Data states
  const [platforms, setPlatforms] = useState<Platform[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [overviewReport, setOverviewReport] = useState<OverviewReport | null>(null);
  const [dssHealth, setDssHealth] = useState<DSSHealth | null>(null);
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [dssDecisions, setDssDecisions] = useState<DSSDecision[]>([]);
  const [decisionDetail, setDecisionDetail] = useState<DSSDecisionDetail | null>(null);
  const [activityLogs, setActivityLogs] = useState<any[]>([]);
  
  // UI states
  const [loading, setLoading] = useState(true);
  const [showDecisionModal, setShowDecisionModal] = useState(false);
  const [selectedMetric, setSelectedMetric] = useState<'revenue' | 'reviews' | 'price' | 'rating'>('revenue');
  const [decisionPage, setDecisionPage] = useState(1);
  const [decisionScenario, setDecisionScenario] = useState('');
  const [decisionStatus, setDecisionStatus] = useState('');

  // Load initial data
  useEffect(() => {
    loadFilters();
  }, []);

  // Load dashboard data when filters change
  useEffect(() => {
    if (platforms.length > 0) {
      loadDashboardData();
    }
  }, [fromDate, toDate, selectedPlatform, selectedCategory]);

  // Load DSS decisions
  useEffect(() => {
    loadDSSDecisions();
  }, [decisionPage, decisionScenario, decisionStatus, fromDate, toDate]);

  const loadFilters = async () => {
    try {
      const [platformsRes, categoriesRes] = await Promise.all([
        getPlatforms(),
        getCategories()
      ]);
      setPlatforms(platformsRes);
      setCategories(categoriesRes);
    } catch (error) {
      console.error('Failed to load filters:', error);
    }
  };

  const loadDashboardData = async () => {
    setLoading(true);
    try {
      const params = {
        from_date: fromDate,
        to_date: toDate,
        platform_code: selectedPlatform || undefined,
        category_key: selectedCategory || undefined,
      };

      const [
        overviewRes,
        healthRes,
        dataStatusRes,
        activityRes
      ] = await Promise.all([
        getOverviewReport(params),
        getDSSHealth(),
        getDataStatus(),
        getActivityLogs({ limit: 10, sort: '-created_at' })
      ]);

      setOverviewReport(overviewRes);
      setDssHealth(healthRes);
      setDataStatus(dataStatusRes);
      setActivityLogs(activityRes.data || []);
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadDSSDecisions = async () => {
    try {
      const params: any = {
        from_date: fromDate,
        to_date: toDate,
        page: decisionPage,
        page_size: 10,
      };
      if (decisionScenario) params.scenario_key = decisionScenario;
      if (decisionStatus) params.status = decisionStatus;

      const response = await listDSSDecisions(params);
      setDssDecisions(response.items);
    } catch (error) {
      console.error('Failed to load DSS decisions:', error);
    }
  };

  const loadDecisionDetail = async (decisionId: number) => {
    try {
      const response = await getDSSDecisionDetail(decisionId);
      setDecisionDetail(response);
      setShowDecisionModal(true);
    } catch (error) {
      console.error('Failed to load decision detail:', error);
    }
  };

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND'
    }).format(amount);
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat('vi-VN').format(num);
  };

  if (loading) {
    return (
      <div>
        <PageMeta
          title="Admin Dashboard"
          description="Vietnam Electronics E-commerce Admin Dashboard"
        />
        <PageBreadcrumb pageTitle="Admin Dashboard" />
        <div className="min-h-screen rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            <span className="ml-3 text-gray-600">Loading dashboard data...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <PageMeta
        title="Admin Dashboard"
        description="Vietnam Electronics E-commerce Admin Dashboard"
      />
      <PageBreadcrumb pageTitle="Admin Dashboard" />

      <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12 space-y-8">

        {/* Hàng 0: Filters Bar */}
        <Card>
          <CardHeader>
            <CardTitle>Filters</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-4 items-end">
              <div>
                <label className="block text-sm font-medium mb-1">From Date</label>
                <Input
                  type="date"
                  value={fromDate}
                  onChange={(e) => setFromDate(e.target.value)}
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">To Date</label>
                <Input
                  type="date"
                  value={toDate}
                  onChange={(e) => setToDate(e.target.value)}
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Platform</label>
                <Select value={selectedPlatform} onValueChange={setSelectedPlatform}>
                  <SelectTrigger className="w-40">
                    <SelectValue placeholder="All Platforms" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="">All Platforms</SelectItem>
                    {platforms.map(platform => (
                      <SelectItem key={platform.platform_code} value={platform.platform_code}>
                        {platform.platform_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Category</label>
                <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                  <SelectTrigger className="w-40">
                    <SelectValue placeholder="All Categories" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="">All Categories</SelectItem>
                    {categories.slice(0, 50).map(category => (
                      <SelectItem key={category.category_key} value={category.category_key}>
                        {category.category_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <Button onClick={() => loadDashboardData()}>Apply Filters</Button>
            </div>
          </CardContent>
        </Card>

        {/* Hàng 1: Cards Overview */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Business KPI Cards */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold">Business KPIs</h3>
            <div className="grid grid-cols-2 gap-4">
              <Card>
                <CardContent className="p-4">
                  <div className="text-2xl font-bold text-green-600">
                    {overviewReport ? formatCurrency(overviewReport.kpis.total_revenue) : 'N/A'}
                  </div>
                  <div className="text-sm text-gray-600">Total Revenue</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4">
                  <div className="text-2xl font-bold text-blue-600">
                    {overviewReport ? formatNumber(overviewReport.kpis.total_products) : 'N/A'}
                  </div>
                  <div className="text-sm text-gray-600">Total Products</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4">
                  <div className="text-2xl font-bold text-purple-600">
                    {overviewReport ? formatNumber(overviewReport.kpis.total_reviews) : 'N/A'}
                  </div>
                  <div className="text-sm text-gray-600">Total Reviews</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4">
                  <div className="text-2xl font-bold text-orange-600">
                    {overviewReport ? `${formatCurrency(overviewReport.kpis.avg_price)} / ${overviewReport.kpis.avg_rating?.toFixed(1)}⭐` : 'N/A'}
                  </div>
                  <div className="text-sm text-gray-600">Avg Price / Rating</div>
                </CardContent>
              </Card>
            </div>
          </div>

          {/* System Health & Data Status */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold">System Status</h3>
            
            {/* System Health */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <Database className="w-4 h-4" />
                  System Health
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="flex items-center justify-between">
                  <span>Overall:</span>
                  <Badge variant={dssHealth?.status === 'healthy' ? 'default' : 'destructive'}>
                    {dssHealth?.status || 'Unknown'}
                  </Badge>
                </div>
                <div className="flex items-center justify-between text-sm">
                  <span>Database:</span>
                  <Badge variant={dssHealth?.components.database === 'healthy' ? 'default' : 'destructive'}>
                    {dssHealth?.components.database || 'Unknown'}
                  </Badge>
                </div>
                <div className="flex items-center justify-between text-sm">
                  <span>AI/LLM:</span>
                  <Badge variant={dssHealth?.components.ai.status === 'healthy' ? 'default' : 'destructive'}>
                    {dssHealth?.components.ai.model || 'Unknown'}
                  </Badge>
                </div>
                <div className="flex items-center justify-between text-sm">
                  <span>ML Tables:</span>
                  <Badge variant={dssHealth?.components.ml_tables.status === 'healthy' ? 'default' : 'destructive'}>
                    {dssHealth?.components.ml_tables.count || 0} tables
                  </Badge>
                </div>
              </CardContent>
            </Card>

            {/* Data Freshness */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <TrendingUp className="w-4 h-4" />
                  Data Freshness
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="text-sm">
                  <div>Latest Fact: {dataStatus?.latest_fact_date || 'N/A'}</div>
                  <div>Latest ML: {dataStatus?.latest_ml_date || 'N/A'}</div>
                  <div>Days since fact: {dataStatus?.days_since_last_fact || 'N/A'}</div>
                  <div>Days since ML: {dataStatus?.days_since_last_ml || 'N/A'}</div>
                </div>
                {dataStatus?.warnings && dataStatus.warnings.length > 0 && (
                  <div className="mt-2">
                    <div className="text-sm font-medium text-red-600">Warnings:</div>
                    {dataStatus.warnings.map((warning, idx) => (
                      <div key={idx} className="text-xs text-red-500">• {warning}</div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </div>

        {/* Hàng 2: Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Overview Trend Chart */}
          <div className="lg:col-span-2">
            <Card>
              <CardHeader>
                <CardTitle>Overview Trends</CardTitle>
                <Select value={selectedMetric} onValueChange={(value: any) => setSelectedMetric(value)}>
                  <SelectTrigger className="w-40">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="revenue">Revenue</SelectItem>
                    <SelectItem value="reviews">Reviews</SelectItem>
                    <SelectItem value="price">Price</SelectItem>
                    <SelectItem value="rating">Rating</SelectItem>
                  </SelectContent>
                </Select>
              </CardHeader>
              <CardContent>
                {overviewReport?.trends.points && (
                  <div className="h-64">
                    {/* Simple chart representation - replace with actual chart library */}
                    <div className="text-center text-gray-500 mt-20">
                      Chart: {selectedMetric} over time
                      <br />
                      {overviewReport.trends.points.length} data points
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Platform Comparison & Category Share */}
          <div className="space-y-4">
            {/* Platform Comparison */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Platform Comparison</CardTitle>
              </CardHeader>
              <CardContent>
                {overviewReport?.platform_comparison.map(platform => (
                  <div key={platform.platform_code} className="flex justify-between items-center py-2 border-b last:border-b-0">
                    <span className="font-medium">{platform.platform_name}</span>
                    <span className="text-sm text-gray-600">
                      {selectedMetric === 'revenue' && formatCurrency(platform.total_revenue)}
                      {selectedMetric === 'reviews' && formatNumber(platform.total_reviews)}
                      {selectedMetric === 'price' && formatCurrency(platform.avg_price)}
                      {selectedMetric === 'rating' && `${platform.avg_rating?.toFixed(1)}⭐`}
                    </span>
                  </div>
                ))}
              </CardContent>
            </Card>

            {/* Category Share */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Category Share</CardTitle>
              </CardHeader>
              <CardContent>
                {selectedPlatform ? (
                  overviewReport?.category_share.map(category => (
                    <div key={category.category_name} className="flex justify-between items-center py-2 border-b last:border-b-0">
                      <span className="text-sm">{category.category_name}</span>
                      <span className="text-sm font-medium">{(category.revenue_share * 100).toFixed(1)}%</span>
                    </div>
                  ))
                ) : (
                  <div className="text-center text-gray-500 py-4">
                    Select a platform to view category share
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </div>

        {/* Hàng 3: DSS Decisions */}
        <Card>
          <CardHeader>
            <CardTitle>DSS Decisions</CardTitle>
            <div className="flex gap-4">
              <Select value={decisionScenario} onValueChange={setDecisionScenario}>
                <SelectTrigger className="w-40">
                  <SelectValue placeholder="All Scenarios" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">All Scenarios</SelectItem>
                  <SelectItem value="price_prediction">Price Prediction</SelectItem>
                  <SelectItem value="product_recommendation">Recommendation</SelectItem>
                  <SelectItem value="review_sentiment">Review Sentiment</SelectItem>
                </SelectContent>
              </Select>
              <Select value={decisionStatus} onValueChange={setDecisionStatus}>
                <SelectTrigger className="w-40">
                  <SelectValue placeholder="All Status" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">All Status</SelectItem>
                  <SelectItem value="DRAFT">Draft</SelectItem>
                  <SelectItem value="APPROVED">Approved</SelectItem>
                  <SelectItem value="IMPLEMENTED">Implemented</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Title</TableHead>
                  <TableHead>Scenario</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Created By</TableHead>
                  <TableHead>Created At</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {dssDecisions.map(decision => (
                  <TableRow key={decision.decision_id}>
                    <TableCell>{decision.decision_id}</TableCell>
                    <TableCell>{decision.title}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{decision.scenario_key}</Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={
                        decision.status === 'APPROVED' ? 'default' :
                        decision.status === 'IMPLEMENTED' ? 'secondary' : 'outline'
                      }>
                        {decision.status}
                      </Badge>
                    </TableCell>
                    <TableCell>{decision.created_by_email || decision.created_by}</TableCell>
                    <TableCell>{new Date(decision.created_at).toLocaleDateString()}</TableCell>
                    <TableCell>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => loadDecisionDetail(decision.decision_id)}
                      >
                        <Eye className="w-4 h-4 mr-1" />
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {/* Hàng 4: User & Activity Log (Optional) */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* User Summary */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Users className="w-4 h-4" />
                User Summary
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-center text-gray-500">
                User summary data would go here
              </div>
            </CardContent>
          </Card>

          {/* Recent Activity */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Recent Activity
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {activityLogs.slice(0, 5).map((log: any) => (
                  <div key={log.log_id} className="flex justify-between items-center py-2 border-b last:border-b-0">
                    <div>
                      <div className="text-sm font-medium">{log.action?.replace(/_/g, ' ')}</div>
                      <div className="text-xs text-gray-500">{log.email}</div>
                    </div>
                    <div className="text-xs text-gray-500">
                      {new Date(log.created_at).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Decision Detail Modal */}
        {showDecisionModal && decisionDetail && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 max-w-4xl w-full mx-4 max-h-[90vh] overflow-y-auto">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-xl font-semibold">{decisionDetail.title}</h3>
                <Button variant="outline" onClick={() => setShowDecisionModal(false)}>
                  Close
                </Button>
              </div>
              
              <Tabs defaultValue="context">
                <TabsList>
                  <TabsTrigger value="context">Context</TabsTrigger>
                  <TabsTrigger value="insights">AI Insights</TabsTrigger>
                  <TabsTrigger value="actions">Actions</TabsTrigger>
                </TabsList>
                
                <TabsContent value="context" className="space-y-4">
                  <div>
                    <h4 className="font-medium">Filters Used</h4>
                    <pre className="text-sm bg-gray-100 p-2 rounded mt-1">
                      {JSON.stringify(decisionDetail.filters, null, 2)}
                    </pre>
                  </div>
                  <div>
                    <h4 className="font-medium">KPI Summary</h4>
                    <pre className="text-sm bg-gray-100 p-2 rounded mt-1">
                      {JSON.stringify(decisionDetail.kpi_summary, null, 2)}
                    </pre>
                  </div>
                </TabsContent>
                
                <TabsContent value="insights" className="space-y-4">
                  <div>
                    <h4 className="font-medium">AI Summary Insights</h4>
                    <ul className="list-disc list-inside space-y-1">
                      {decisionDetail.ai_summary_insights.map((insight, idx) => (
                        <li key={idx} className="text-sm">{insight}</li>
                      ))}
                    </ul>
                  </div>
                  <div>
                    <h4 className="font-medium">AI Recommended Actions</h4>
                    <ul className="list-disc list-inside space-y-1">
                      {decisionDetail.ai_recommended_actions.map((action, idx) => (
                        <li key={idx} className="text-sm">{action}</li>
                      ))}
                    </ul>
                  </div>
                </TabsContent>
                
                <TabsContent value="actions" className="space-y-4">
                  <div className="space-y-2">
                    {decisionDetail.actions.map((action, idx) => (
                      <div key={idx} className="border rounded p-3">
                        <div className="flex justify-between items-center">
                          <span className="font-medium">{action.action_type}</span>
                          <Badge variant={action.status === 'completed' ? 'default' : 'outline'}>
                            {action.status}
                          </Badge>
                        </div>
                        <div className="text-sm text-gray-600 mt-1">
                          Target: {action.target_level}
                          {action.product_key && ` - Product: ${action.product_key}`}
                        </div>
                        {action.current_value && action.recommended_value && (
                          <div className="text-sm mt-1">
                            {action.current_value} → {action.recommended_value}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </TabsContent>
              </Tabs>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}