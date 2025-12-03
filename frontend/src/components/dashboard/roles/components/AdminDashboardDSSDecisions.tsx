import React from 'react';
import { Button } from '../../../ui/figma/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../../ui/figma/select';
import { Card, CardContent, CardHeader, CardTitle } from '../../../ui/figma/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../../ui/figma/table';
import { Badge } from '../../../ui/figma/badge';
import { Eye } from 'lucide-react';
import { type DSSDecisionSummary } from '../../../../services/DSSApi';

interface AdminDashboardDSSDecisionsProps {
  dssDecisions: DSSDecisionSummary[];
  decisionScenario: string;
  decisionStatus: string;
  onScenarioChange: (value: string) => void;
  onStatusChange: (value: string) => void;
  onViewDecision: (decisionId: number) => void;
}

export default function AdminDashboardDSSDecisions({
  dssDecisions,
  decisionScenario,
  decisionStatus,
  onScenarioChange,
  onStatusChange,
  onViewDecision,
}: AdminDashboardDSSDecisionsProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>DSS Decisions</CardTitle>
        <div className="flex gap-4">
          <Select value={decisionScenario} onValueChange={onScenarioChange}>
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
          <Select value={decisionStatus} onValueChange={onStatusChange}>
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
                    onClick={() => onViewDecision(decision.decision_id)}
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
  );
}