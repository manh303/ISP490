import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../ui/figma/card';
import { Badge } from '../../../ui/figma/badge';
import { Database, TrendingUp } from 'lucide-react';
import { type OverviewReport } from '../../../../services/analyticsApi';
import { type DSSHealthResponse, type DataStatusResponse } from '../../../../services/DSSApi';

interface AdminDashboardOverviewCardsProps {
  overviewReport: OverviewReport | null;
  dssHealth: DSSHealthResponse | null;
  dataStatus: DataStatusResponse | null;
  formatCurrency: (amount: number) => string;
  formatNumber: (num: number) => string;
}

export default function AdminDashboardOverviewCards({
  overviewReport,
  dssHealth,
  dataStatus,
  formatCurrency,
  formatNumber,
}: AdminDashboardOverviewCardsProps) {
  return (
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
  );
}