import React from 'react';
import { Button } from '../../../ui/figma/button';
import { Badge } from '../../../ui/figma/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../../ui/figma/tabs';
import { type DSSDecisionDetailResponse } from '../../../../services/DSSApi';

interface DSSDecisionModalProps {
  showModal: boolean;
  decisionDetail: DSSDecisionDetailResponse | null;
  onClose: () => void;
}

export default function DSSDecisionModal({
  showModal,
  decisionDetail,
  onClose,
}: DSSDecisionModalProps) {
  if (!showModal || !decisionDetail) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 max-w-4xl w-full mx-4 max-h-[90vh] overflow-y-auto">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-xl font-semibold">{decisionDetail.title}</h3>
          <Button variant="outline" onClick={onClose}>
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
              <h4 className="font-medium mb-2">Filters Used</h4>
              <div className="bg-gray-50 rounded-lg p-4 space-y-2">
                {decisionDetail.filters && (
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    {decisionDetail.filters.from_date && (
                      <div className="flex justify-between">
                        <span className="text-gray-500">From Date:</span>
                        <span className="font-medium">{decisionDetail.filters.from_date}</span>
                      </div>
                    )}
                    {decisionDetail.filters.to_date && (
                      <div className="flex justify-between">
                        <span className="text-gray-500">To Date:</span>
                        <span className="font-medium">{decisionDetail.filters.to_date}</span>
                      </div>
                    )}
                    {decisionDetail.filters.platforms && decisionDetail.filters.platforms.length > 0 && (
                      <div className="flex justify-between">
                        <span className="text-gray-500">Platforms:</span>
                        <span className="font-medium">{decisionDetail.filters.platforms.join(', ')}</span>
                      </div>
                    )}
                    {decisionDetail.filters.categories && decisionDetail.filters.categories.length > 0 && (
                      <div className="flex justify-between">
                        <span className="text-gray-500">Categories:</span>
                        <span className="font-medium">{decisionDetail.filters.categories.join(', ')}</span>
                      </div>
                    )}
                    {decisionDetail.filters.scope_mode && (
                      <div className="flex justify-between">
                        <span className="text-gray-500">Scope Mode:</span>
                        <span className="font-medium">{decisionDetail.filters.scope_mode}</span>
                      </div>
                    )}
                    {decisionDetail.filters.product_keys && (
                      <div className="flex justify-between col-span-2">
                        <span className="text-gray-500">Product Keys:</span>
                        <span className="font-medium">{decisionDetail.filters.product_keys ? decisionDetail.filters.product_keys.join(', ') : 'All'}</span>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
            <div>
              <h4 className="font-medium mb-2">KPI Summary</h4>
              <div className="bg-gray-50 rounded-lg p-4">
                {decisionDetail.kpi_summary && (
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
                    {decisionDetail.kpi_summary.is_estimated !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">Estimated</div>
                        <div className="font-bold">{decisionDetail.kpi_summary.is_estimated ? 'Yes' : 'No'}</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.num_products !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">Products</div>
                        <div className="font-bold text-blue-600">{decisionDetail.kpi_summary.num_products}</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.avg_confidence !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">Avg Confidence</div>
                        <div className="font-bold text-green-600">{(decisionDetail.kpi_summary.avg_confidence * 100).toFixed(1)}%</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.current_revenue !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">Current Revenue</div>
                        <div className="font-bold">{decisionDetail.kpi_summary.current_revenue.toLocaleString('vi-VN')} đ</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.projected_revenue !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">Projected Revenue</div>
                        <div className="font-bold text-green-600">{decisionDetail.kpi_summary.projected_revenue.toLocaleString('vi-VN')} đ</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.expected_revenue_uplift_pct !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">Expected Uplift</div>
                        <div className="font-bold text-purple-600">+{(decisionDetail.kpi_summary.expected_revenue_uplift_pct * 100).toFixed(2)}%</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.num_with_recommendation !== undefined && (
                      <div className="p-3 bg-white rounded border">
                        <div className="text-gray-500 text-xs">With Recommendations</div>
                        <div className="font-bold">{decisionDetail.kpi_summary.num_with_recommendation}</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.estimation_method && (
                      <div className="p-3 bg-white rounded border col-span-2">
                        <div className="text-gray-500 text-xs">Estimation Method</div>
                        <div className="font-medium text-sm">{decisionDetail.kpi_summary.estimation_method}</div>
                      </div>
                    )}
                    {decisionDetail.kpi_summary.estimation_note && (
                      <div className="p-3 bg-yellow-50 rounded border border-yellow-200 col-span-full">
                        <div className="text-yellow-700 text-xs">Note</div>
                        <div className="text-sm text-yellow-800">{decisionDetail.kpi_summary.estimation_note}</div>
                      </div>
                    )}
                  </div>
                )}
              </div>
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
  );
}