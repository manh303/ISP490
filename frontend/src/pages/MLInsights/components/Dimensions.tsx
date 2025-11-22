import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Database, Calendar, RefreshCw } from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../../components/ui/figma/table';

const dimensions = [
  {
    name: 'dim_product',
    records: 1245678,
    columns: 12,
    lastUpdated: '2024-11-16 08:30',
    status: 'Active',
    description: 'Product master data including SKU, name, category, brand',
  },
  {
    name: 'dim_customer',
    records: 892340,
    columns: 18,
    lastUpdated: '2024-11-16 08:15',
    status: 'Active',
    description: 'Customer profile and demographic information',
  },
  {
    name: 'dim_platform',
    records: 8,
    columns: 6,
    lastUpdated: '2024-11-15 22:00',
    status: 'Active',
    description: 'E-commerce platform reference data',
  },
  {
    name: 'dim_date',
    records: 3650,
    columns: 10,
    lastUpdated: '2024-11-16 00:00',
    status: 'Active',
    description: 'Date dimension for time-based analysis',
  },
  {
    name: 'dim_category',
    records: 456,
    columns: 8,
    lastUpdated: '2024-11-16 08:30',
    status: 'Active',
    description: 'Product category hierarchy',
  },
];

export function Dimensions() {
  return (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Total Dimensions</div>
                <div className="text-gray-900">{dimensions.length}</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-blue-50 flex items-center justify-center">
                <Database className="w-6 h-6 text-[#1d4ed8]" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Total Records</div>
                <div className="text-gray-900">
                  {dimensions.reduce((sum, dim) => sum + dim.records, 0).toLocaleString()}
                </div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-green-50 flex items-center justify-center">
                <RefreshCw className="w-6 h-6 text-green-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Last Updated</div>
                <div className="text-gray-900">Nov 16, 08:30</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-purple-50 flex items-center justify-center">
                <Calendar className="w-6 h-6 text-purple-600" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Dimensions Table */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-gray-900">Dimension Tables</CardTitle>
            <Button className="bg-[#1d4ed8] hover:bg-[#1e3a8a]">
              <RefreshCw className="w-4 h-4 mr-2" />
              Refresh All
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Dimension Name</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Records</TableHead>
                <TableHead>Columns</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Updated</TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {dimensions.map((dim, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Database className="w-4 h-4 text-[#1d4ed8]" />
                      <span className="text-gray-900">{dim.name}</span>
                    </div>
                  </TableCell>
                  <TableCell className="max-w-sm">
                    <div className="text-sm text-gray-600">{dim.description}</div>
                  </TableCell>
                  <TableCell>
                    <span className="text-gray-900">{dim.records.toLocaleString()}</span>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className="text-xs">{dim.columns} cols</Badge>
                  </TableCell>
                  <TableCell>
                    <Badge className="bg-green-500 text-xs">{dim.status}</Badge>
                  </TableCell>
                  <TableCell className="text-sm text-gray-500">{dim.lastUpdated}</TableCell>
                  <TableCell>
                    <Button size="sm" variant="outline">View Schema</Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
