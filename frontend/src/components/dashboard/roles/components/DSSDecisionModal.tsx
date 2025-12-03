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
  );
}