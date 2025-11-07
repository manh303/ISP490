import { useState } from 'react';
import { BarChart3, Play, FileText, Database, GitBranch, Activity } from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';
import { Badge } from '../../components/ui/figma/badge';

export function DataEngineerWireframe() {
  const [reportsPerPage, setReportsPerPage] = useState('10');
  
  const mockReports = Array.from({ length: 20 }, (_, i) => ({
    id: i + 1,
    name: `Pipeline ${i + 1} - Data Processing`,
    date: `2025-11-0${(i % 9) + 1}`,
    status: i % 3 === 0 ? 'Running' : i % 3 === 1 ? 'Success' : 'Failed',
  }));

  const displayedReports = mockReports.slice(0, parseInt(reportsPerPage));

  return (
    <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm" style={{ height: '800px' }}>
      <div className="flex h-full">
        {/* Sidebar */}
        <div className="w-64 bg-gray-50 border-r border-gray-200 p-4">
          <div className="mb-8">
            <h2 className="text-gray-900 mb-6">Tên hệ thống</h2>
          </div>
          
          <nav className="space-y-2">
            <div className="text-gray-900 bg-gray-200 px-4 py-2 rounded">
              Dashboard
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Data Pipelines
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              ETL Jobs
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Data Quality Monitoring
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Schema Management
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Data Warehouse
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Logs & Debugging
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Settings
            </div>
          </nav>
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col bg-white">
          {/* Header */}
          <div className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
            <div className="flex items-center gap-4">
              <span className="text-gray-600">thông báo</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-gray-600">Tên người dùng (Data Engineer)</span>
              <Button variant="ghost" size="sm" className="text-gray-600">
                log out
              </Button>
            </div>
          </div>

          {/* Main Dashboard Area - Full Width */}
          <div className="flex-1 p-6 bg-white">
            <div className="h-full bg-gray-50 border border-gray-200 rounded-lg p-6 flex flex-col">
              {/* Dashboard Display */}
              <div className="flex-1 grid grid-cols-3 gap-6 mb-6">
                {/* Data Flow Visualization */}
                <div className="col-span-2 flex items-center justify-center border border-gray-200 rounded-lg bg-white">
                  <div className="text-center">
                    <Database className="h-16 w-16 text-gray-400 mx-auto mb-4" />
                    <p className="text-gray-600">Data Pipeline Visualization</p>
                    <p className="text-gray-500 text-sm mt-2">Biểu đồ luồng dữ liệu và trạng thái pipeline</p>
                  </div>
                </div>

                {/* Stats Cards */}
                <div className="space-y-4">
                  <div className="bg-white border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-2">
                      <Activity className="h-5 w-5 text-green-500" />
                      <span className="text-gray-600 text-sm">Active Pipelines</span>
                    </div>
                    <div className="text-gray-900 text-2xl">24</div>
                  </div>

                  <div className="bg-white border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-2">
                      <GitBranch className="h-5 w-5 text-blue-500" />
                      <span className="text-gray-600 text-sm">Data Sources</span>
                    </div>
                    <div className="text-gray-900 text-2xl">12</div>
                  </div>

                  <div className="bg-white border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-2">
                      <BarChart3 className="h-5 w-5 text-purple-500" />
                      <span className="text-gray-600 text-sm">Data Volume</span>
                    </div>
                    <div className="text-gray-900 text-2xl">2.4TB</div>
                  </div>
                </div>
              </div>
              
              {/* Action Buttons */}
              <div className="flex gap-3 justify-center">
                <Button>
                  <Play className="h-4 w-4 mr-2" />
                  Run Pipeline
                </Button>
                <Button variant="outline">
                  <Database className="h-4 w-4 mr-2" />
                  Create ETL Job
                </Button>
                <Button variant="outline">
                  <FileText className="h-4 w-4 mr-2" />
                  View Logs
                </Button>
              </div>
            </div>
          </div>

          {/* Recent Jobs Section */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-gray-900">Recent Pipeline Jobs</h3>
              <div className="flex items-center gap-2">
                <span className="text-gray-600 text-sm">Hiển thị:</span>
                <Select value={reportsPerPage} onValueChange={setReportsPerPage}>
                  <SelectTrigger className="w-24 bg-white border-gray-300">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="5">5</SelectItem>
                    <SelectItem value="10">10</SelectItem>
                    <SelectItem value="15">15</SelectItem>
                    <SelectItem value="20">20</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-3 max-h-32 overflow-auto">
              {displayedReports.map((report) => (
                <div 
                  key={report.id}
                  className="bg-white border border-gray-200 rounded p-3 flex items-center justify-between hover:bg-gray-50 cursor-pointer"
                >
                  <div>
                    <p className="text-gray-700 text-sm">{report.name}</p>
                    <p className="text-gray-500 text-xs">{report.date}</p>
                  </div>
                  <Badge 
                    variant={
                      report.status === 'Running' ? 'secondary' : 
                      report.status === 'Success' ? 'default' : 
                      'destructive'
                    }
                  >
                    {report.status}
                  </Badge>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
