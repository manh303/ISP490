import { useState, useEffect } from 'react';
import { 
  Download, 
  FileDown, 
  Lightbulb,
  AlertCircle,
  CheckCircle,
  FileText,
  Loader2,
  RefreshCw
} from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';
import { Badge } from '../../components/ui/figma/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../components/ui/figma/table';
import { AnalyticsDashboard } from './AnalyticsDashboard';
import { ProductAnalytics } from './ProductAnalytics';
import { ReviewAnalytics } from './ReviewAnalytics';
import { PlatformAnalytics } from './PlatformAnalytics';

export function AnalystWireframe() {
  const [activeTab, setActiveTab] = useState('dashboard');

  const tabs = [
    { id: 'dashboard', label: 'Analytics Dashboard', component: AnalyticsDashboard },
    { id: 'product', label: 'Product Analytics', component: ProductAnalytics },
    { id: 'review', label: 'Review Analytics', component: ReviewAnalytics },
    { id: 'platform', label: 'Platform Analytics', component: PlatformAnalytics },
  ];

  const ActiveComponent = tabs.find(tab => tab.id === activeTab)?.component || AnalyticsDashboard;

  return (
    <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm" style={{ minHeight: '800px' }}>
      <div className="flex h-full flex-col">
        {/* Tab Navigation */}
        <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
          <div className="flex items-center gap-4">
            {tabs.map((tab) => (
              <Button
                key={tab.id}
                variant={activeTab === tab.id ? 'default' : 'outline'}
                size="sm"
                onClick={() => setActiveTab(tab.id)}
              >
                {tab.label}
              </Button>
            ))}
          </div>
        </div>

        {/* Active Tab Content */}
        <div className="flex-1">
          <ActiveComponent />
        </div>
      </div>
    </div>
  );
}