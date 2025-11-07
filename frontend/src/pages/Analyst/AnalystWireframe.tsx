import { useState } from 'react';
import { 
  Download, 
  FileDown, 
  Lightbulb,
  AlertCircle,
  CheckCircle,
  FileText,
  BarChart3,
  LineChart,
  PieChart
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

export function AnalystWireframe() {
  const [itemsPerPage, setItemsPerPage] = useState('5');
  const [activeView, setActiveView] = useState('dashboard');
  
  // Mock DSS recommendations
  const dssRecommendations = [
    {
      id: 1,
      title: 'Xu hướng tăng trưởng doanh thu',
      type: 'Positive',
      description: 'Doanh thu Q4 tăng 15% so với Q3. Đề xuất tăng đầu tư vào kênh bán hàng online.',
      impact: 'High',
    },
    {
      id: 2,
      title: 'Cảnh báo chi phí vận hành',
      type: 'Warning',
      description: 'Chi phí vận hành tăng 8% trong 2 tháng gần nhất. Cần kiểm tra và tối ưu hóa quy trình.',
      impact: 'Medium',
    },
    {
      id: 3,
      title: 'Cơ hội mở rộng thị trường',
      type: 'Opportunity',
      description: 'Phân tích cho thấy tiềm năng tăng 20% doanh thu nếu mở rộng sang khu vực miền Trung.',
      impact: 'High',
    },
    {
      id: 4,
      title: 'Hiệu suất nhân sự',
      type: 'Positive',
      description: 'Năng suất nhân sự tăng 12% sau khi áp dụng quy trình mới. Đề xuất nhân rộng mô hình.',
      impact: 'Medium',
    },
    {
      id: 5,
      title: 'Rủi ro về nguồn cung',
      type: 'Critical',
      description: 'Phụ thuộc cao vào 1 nhà cung cấp chính. Cần đa dạng hóa nguồn cung để giảm rủi ro.',
      impact: 'High',
    },
  ];

  const displayedRecommendations = dssRecommendations.slice(0, parseInt(itemsPerPage));

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'Positive':
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case 'Warning':
        return <AlertCircle className="h-4 w-4 text-yellow-500" />;
      case 'Critical':
        return <AlertCircle className="h-4 w-4 text-red-500" />;
      case 'Opportunity':
        return <Lightbulb className="h-4 w-4 text-blue-500" />;
      default:
        return <AlertCircle className="h-4 w-4 text-gray-500" />;
    }
  };

  const getTypeVariant = (type: string): "default" | "secondary" | "destructive" | "outline" => {
    switch (type) {
      case 'Positive':
        return 'default';
      case 'Warning':
        return 'secondary';
      case 'Critical':
        return 'destructive';
      case 'Opportunity':
        return 'outline';
      default:
        return 'secondary';
    }
  };

  return (
    <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm" style={{ height: '800px' }}>
      <div className="flex h-full">
        {/* Sidebar */}
        <div className="w-64 bg-gray-50 border-r border-gray-200 p-4 relative">
          <div className="mb-8">
            <h2 className="text-gray-900 mb-6">Tên hệ thống</h2>
          </div>
          
          <nav className="space-y-2">
            <div 
              className={`px-4 py-2 rounded cursor-pointer ${activeView === 'dashboard' ? 'text-gray-900 bg-gray-200' : 'text-gray-600 hover:bg-gray-100'}`}
              onClick={() => setActiveView('dashboard')}
            >
              Dashboard
            </div>
            <div 
              className={`px-4 py-2 rounded cursor-pointer ${activeView === 'dss' ? 'text-gray-900 bg-gray-200' : 'text-gray-600 hover:bg-gray-100'}`}
              onClick={() => setActiveView('dss')}
            >
              Đề xuất DSS
            </div>
            <div 
              className={`px-4 py-2 rounded cursor-pointer ${activeView === 'report' ? 'text-gray-900 bg-gray-200' : 'text-gray-600 hover:bg-gray-100'}`}
              onClick={() => setActiveView('report')}
            >
              Báo cáo
            </div>
            <div 
              className={`px-4 py-2 rounded cursor-pointer ${activeView === 'data' ? 'text-gray-900 bg-gray-200' : 'text-gray-600 hover:bg-gray-100'}`}
              onClick={() => setActiveView('data')}
            >
              Dữ liệu
            </div>
          </nav>
          
          <div className="absolute bottom-4 left-4 w-48 space-y-2">
            <Button variant="outline" className="w-full">
              Đổi mật khẩu
            </Button>
            <Button variant="outline" className="w-full">
              Tài khoản
            </Button>
          </div>
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col bg-white overflow-hidden">
          {/* Header */}
          <div className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
            <div className="flex items-center gap-4">
              <span className="text-gray-600">thông báo</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-gray-600">Tên người dùng (Analyst)</span>
              <Button variant="ghost" size="sm" className="text-gray-600">
                log out
              </Button>
            </div>
          </div>

          {/* Export Controls */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
            <div className="flex items-center gap-4 justify-between">
              <div className="flex items-center gap-3">
                <Button variant="outline" size="sm">
                  <Download className="h-4 w-4 mr-2" />
                  Export Dashboard
                </Button>
                <Button variant="outline" size="sm">
                  <FileDown className="h-4 w-4 mr-2" />
                  Export Data
                </Button>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-gray-600 text-sm">Hiển thị:</span>
                <Select value={itemsPerPage} onValueChange={setItemsPerPage}>
                  <SelectTrigger className="w-24 bg-white border-gray-300">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="5">5</SelectItem>
                    <SelectItem value="6">6</SelectItem>
                    <SelectItem value="10">10</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {/* Dashboard Charts - Wireframe */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white">
            <h3 className="text-gray-900 mb-3">Dashboard</h3>
            <div className="grid grid-cols-3 gap-4">
              {/* Chart 1 - Bar Chart */}
              <div className="border-2 border-dashed border-gray-300 rounded p-4 h-48 flex flex-col items-center justify-center bg-gray-50">
                <BarChart3 className="h-12 w-12 text-gray-400 mb-2" />
                <span className="text-gray-600 text-sm">Biểu đồ cột</span>
                <span className="text-gray-500 text-xs">Doanh thu theo tháng</span>
              </div>
              
              {/* Chart 2 - Line Chart */}
              <div className="border-2 border-dashed border-gray-300 rounded p-4 h-48 flex flex-col items-center justify-center bg-gray-50">
                <LineChart className="h-12 w-12 text-gray-400 mb-2" />
                <span className="text-gray-600 text-sm">Biểu đồ đường</span>
                <span className="text-gray-500 text-xs">Xu hướng tăng trưởng</span>
              </div>
              
              {/* Chart 3 - Pie Chart */}
              <div className="border-2 border-dashed border-gray-300 rounded p-4 h-48 flex flex-col items-center justify-center bg-gray-50">
                <PieChart className="h-12 w-12 text-gray-400 mb-2" />
                <span className="text-gray-600 text-sm">Biểu đồ tròn</span>
                <span className="text-gray-500 text-xs">Phân bổ ngân sách</span>
              </div>
            </div>
          </div>

          {/* DSS Recommendations Section */}
          <div className="flex-1 overflow-auto px-6 py-4 bg-white">
            <div className="mb-4 flex items-center gap-2">
              <Lightbulb className="h-5 w-5 text-blue-500" />
              <h3 className="text-gray-900">Đề xuất DSS (Decision Support System)</h3>
            </div>
            
            <Table>
              <TableHeader>
                <TableRow className="border-gray-200 hover:bg-gray-50">
                  <TableHead className="text-gray-600 w-12">ID</TableHead>
                  <TableHead className="text-gray-600 w-48">Tiêu đề</TableHead>
                  <TableHead className="text-gray-600 w-96">Mô tả</TableHead>
                  <TableHead className="text-gray-600 w-28">Loại</TableHead>
                  <TableHead className="text-gray-600 w-24">Mức độ</TableHead>
                  <TableHead className="text-gray-600 w-28">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {displayedRecommendations.map((rec) => (
                  <TableRow key={rec.id} className="border-gray-200 hover:bg-gray-50">
                    <TableCell className="text-gray-700">{rec.id}</TableCell>
                    <TableCell className="text-gray-900">
                      <div className="flex items-center gap-2">
                        {getTypeIcon(rec.type)}
                        <span className="line-clamp-2">{rec.title}</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-gray-700 text-sm">
                      <div className="line-clamp-2">{rec.description}</div>
                    </TableCell>
                    <TableCell>
                      <Badge variant={getTypeVariant(rec.type)}>
                        {rec.type}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge 
                        variant={
                          rec.impact === 'High' ? 'destructive' : 'default'
                        }
                      >
                        {rec.impact}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-2">
                        <Button size="sm" variant="outline">
                          <FileText className="h-4 w-4" />
                        </Button>
                        <Button size="sm" variant="ghost">
                          <Download className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Hiển thị {displayedRecommendations.length} / {dssRecommendations.length} đề xuất
            </div>
            <Button>
              <FileDown className="h-4 w-4 mr-2" />
              Xuất báo cáo DSS
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}