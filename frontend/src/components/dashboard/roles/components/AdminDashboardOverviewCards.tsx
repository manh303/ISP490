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
        <h3 className="text-lg font-semibold">Chỉ số KPI Kinh doanh</h3>
        <div className="grid grid-cols-2 gap-4">
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-green-600">
                {overviewReport ? formatCurrency(overviewReport.kpis.total_revenue) : 'N/A'}
              </div>
              <div className="text-sm text-gray-600">Tổng doanh thu</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-blue-600">
                {overviewReport ? formatNumber(overviewReport.kpis.total_products) : 'N/A'}
              </div>
              <div className="text-sm text-gray-600">Tổng sản phẩm</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-purple-600">
                {overviewReport ? formatNumber(overviewReport.kpis.total_reviews) : 'N/A'}
              </div>
              <div className="text-sm text-gray-600">Tổng đánh giá</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-orange-600">
                {overviewReport ? `${formatCurrency(overviewReport.kpis.avg_price)} / ${overviewReport.kpis.avg_rating?.toFixed(1)}⭐` : 'N/A'}
              </div>
              <div className="text-sm text-gray-600">Giá TB / Đánh giá TB</div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* System Health & Data Status */}
      <div className="space-y-4">
        <h3 className="text-lg font-semibold">Trạng thái hệ thống</h3>

        {/* System Health */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Database className="w-4 h-4" />
              Tình trạng hệ thống
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <div className="flex items-center justify-between">
              <span>Tổng thể:</span>
              <Badge variant={dssHealth?.status === 'healthy' ? 'default' : 'destructive'}>
                {dssHealth?.status || 'Không xác định'}
              </Badge>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span>Cơ sở dữ liệu:</span>
              <Badge variant={dssHealth?.components.database === 'healthy' ? 'default' : 'destructive'}>
                {dssHealth?.components.database || 'Không xác định'}
              </Badge>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span>AI/LLM:</span>
              <Badge variant={dssHealth?.components.ai.status === 'healthy' ? 'default' : 'destructive'}>
                {dssHealth?.components.ai.model || 'Không xác định'}
              </Badge>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span>Bảng ML:</span>
              <Badge variant={dssHealth?.components.ml_tables.status === 'healthy' ? 'default' : 'destructive'}>
                {dssHealth?.components.ml_tables.count || 0} bảng
              </Badge>
            </div>
          </CardContent>
        </Card>

        {/* Data Freshness */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <TrendingUp className="w-4 h-4" />
              Tính mới dữ liệu
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <div className="text-sm">
              <div>Dữ liệu mới nhất: {dataStatus?.latest_fact_date || 'N/A'}</div>
              <div>ML mới nhất: {dataStatus?.latest_ml_date || 'N/A'}</div>
              <div>Ngày kể từ dữ liệu: {dataStatus?.days_since_last_fact || 'N/A'}</div>
              <div>Ngày kể từ ML: {dataStatus?.days_since_last_ml || 'N/A'}</div>
            </div>
            {dataStatus?.warnings && dataStatus.warnings.length > 0 && (
              <div className="mt-2">
                <div className="text-sm font-medium text-red-600">Cảnh báo:</div>
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