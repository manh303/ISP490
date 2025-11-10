import { useState } from 'react';
import { Search, UserPlus, Edit, Trash2 } from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../components/ui/figma/table';
import { Badge } from '../../components/ui/figma/badge';

export function AdminWireframe() {
  const [itemsPerPage, setItemsPerPage] = useState('10');
  
  const mockUsers = Array.from({ length: 15 }, (_, i) => ({
    id: i + 1,
    name: `Người dùng ${i + 1}`,
    email: `user${i + 1}@example.com`,
    role: i % 3 === 0 ? 'Admin' : i % 3 === 1 ? 'Analyst' : 'Data Engineer',
    status: i % 4 === 0 ? 'Inactive' : 'active',
  }));

  const displayedUsers = mockUsers.slice(0, parseInt(itemsPerPage));

  return (
    <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm" style={{ height: '800px' }}>
      <div className="flex h-full">
        {/* Sidebar */}
        <div className="w-64 bg-gray-50 border-r border-gray-200 p-4 relative">
          <div className="mb-8">
            <h2 className="text-gray-900 mb-6">Tên hệ thống</h2>
          </div>
          
          <nav className="space-y-2">
            <div className="text-gray-900 bg-gray-200 px-4 py-2 rounded">
              Quản lý Tài khoản
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Cài đặt hệ thống
            </div>
            <div className="text-gray-600 px-4 py-2 hover:bg-gray-100 rounded cursor-pointer">
              Nhật ký hoạt động
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
        <div className="flex-1 flex flex-col bg-white">
          {/* Header */}
          <div className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
            <div className="flex items-center gap-4">
              <span className="text-gray-600">thông báo</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-gray-600">Tên người dùng (Admin)</span>
              <Button variant="ghost" size="sm" className="text-gray-600">
                log out
              </Button>
            </div>
          </div>

          {/* Controls */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
            <div className="flex items-center gap-4 justify-end">
              <div className="flex items-center gap-2">
                <span className="text-gray-600 text-sm">Hiển thị:</span>
                <Select value={itemsPerPage} onValueChange={setItemsPerPage}>
                  <SelectTrigger className="w-24 bg-white border-gray-300">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="5">5</SelectItem>
                    <SelectItem value="10">10</SelectItem>
                    <SelectItem value="15">15</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {/* Table */}
          <div className="flex-1 overflow-auto px-6 py-4 bg-white">
            <Table>
              <TableHeader>
                <TableRow className="border-gray-200 hover:bg-gray-50">
                  <TableHead className="text-gray-600">STT</TableHead>
                  <TableHead className="text-gray-600">Tên</TableHead>
                  <TableHead className="text-gray-600">Email</TableHead>
                  <TableHead className="text-gray-600">Role</TableHead>
                  <TableHead className="text-gray-600">Status</TableHead>
                  <TableHead className="text-gray-600">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {displayedUsers.map((user) => (
                  <TableRow key={user.id} className="border-gray-200 hover:bg-gray-50">
                    <TableCell className="text-gray-700">{user.id}</TableCell>
                    <TableCell className="text-gray-700">{user.name}</TableCell>
                    <TableCell className="text-gray-700">{user.email}</TableCell>
                    <TableCell>
                      <Badge variant={user.role === 'Admin' ? 'default' : 'secondary'}>
                        {user.role}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={user.status === 'active' ? 'default' : 'destructive'}>
                        {user.status}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-2">
                        <Button size="sm" variant="outline">
                          <Edit className="h-4 w-4" />
                        </Button>
                        <Button size="sm" variant="destructive">
                          <Trash2 className="h-4 w-4" />
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
              Hiển thị {displayedUsers.length} / {mockUsers.length} tài khoản
            </div>
            <Button>
              <UserPlus className="h-4 w-4 mr-2" />
              Add
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}