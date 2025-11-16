import React, { useEffect, useState, useMemo } from "react";
import { userApi } from "../../services/userApi";
import { getAllRoles } from "../../services/roleApi";
import { Button } from '../../components/ui/figma/button';
import { Badge } from '../../components/ui/figma/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../components/ui/figma/table';
import { Eye, RotateCcw, Trash2, Search, X } from 'lucide-react';
import { useToast } from "../../contexts/ToastContext";

interface User {
  user_id: number;
  full_name: string;
  email: string;
  phone: string;
  role_code: string;
  role_name: string;
  status: string;
}

interface DeletedUsersListProps {
  onSelectUser: (id: number) => void;
}

export default function DeletedUsersList({ onSelectUser }: DeletedUsersListProps) {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [total, setTotal] = useState(0);
  const { showToast } = useToast();

  // Filter states
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedRole, setSelectedRole] = useState<string>("all");
  const [availableRoles, setAvailableRoles] = useState<Array<{ role_code: string; role_name: string }>>([]);
  
  // Pagination states
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(10);

  // Fetch users and roles
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        // Fetch deleted users
        const userData = await userApi.getDeletedUsers();
        if (userData.success) {
          setUsers(userData.data);
          setTotal(userData.total);
        } else if (Array.isArray(userData)) {
          setUsers(userData);
        } else if (userData.detail) {
          setError(userData.detail);
        } else {
          throw new Error("API trả về lỗi");
        }

        // Fetch roles
        const rolesData = await getAllRoles({ page: 1, limit: 100 });
        if (rolesData.success) {
          setAvailableRoles(rolesData.data.map(role => ({
            role_code: role.role_code,
            role_name: role.role_name
          })));
        }
      } catch (err: any) {
        setError(err?.response?.data?.detail || "Lỗi không xác định");
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const handleRestore = async (id: number) => {
    try {
      const res = await userApi.restoreUser(id);
      if (res && res.detail) {
        setError(res.detail);
        showToast(res.detail, 'error');
      } else {
        setUsers(users.filter(u => u.user_id !== id));
        setTotal(prev => prev - 1);
        showToast('✓ Khôi phục tài khoản thành công!', 'success');
      }
    } catch (err: any) {
      const errorMsg = err?.response?.data?.detail || "Khôi phục thất bại";
      setError(errorMsg);
      showToast(errorMsg, 'error');
    }
  };

  const handlePermanentDelete = async (id: number) => {
    const confirmed = window.confirm("Bạn có chắc chắn muốn xóa vĩnh viễn người dùng này? Hành động này không thể hoàn tác!");
    if (!confirmed) return;
    try {
      const res = await userApi.deleteUser(id);
      if (res && res.detail) {
        setError(res.detail);
        showToast(res.detail, 'error');
      } else {
        setUsers(users.filter(u => u.user_id !== id));
        setTotal(prev => prev - 1);
        showToast('✓ Xóa vĩnh viễn tài khoản thành công!', 'success');
      }
    } catch (err: any) {
      const errorMsg = err?.response?.data?.detail || "Xóa vĩnh viễn thất bại";
      setError(errorMsg);
      showToast(errorMsg, 'error');
    }
  };

  // Filter and paginate users
  const filteredUsers = useMemo(() => {
    let filtered = users;

    // Filter by search term (email, name, or phone)
    if (searchTerm.trim()) {
      const searchLower = searchTerm.toLowerCase();
      filtered = filtered.filter(user => 
        user.email.toLowerCase().includes(searchLower) ||
        user.full_name.toLowerCase().includes(searchLower) ||
        (user.phone && user.phone.includes(searchTerm))
      );
    }

    // Filter by role
    if (selectedRole !== "all") {
      filtered = filtered.filter(user => user.role_code === selectedRole);
    }

    return filtered;
  }, [users, searchTerm, selectedRole]);

  // Paginate filtered users
  const paginatedUsers = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return filteredUsers.slice(startIndex, endIndex);
  }, [filteredUsers, currentPage, itemsPerPage]);

  // Calculate total pages
  const totalPages = Math.ceil(filteredUsers.length / itemsPerPage);

  // Reset to first page when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, selectedRole, itemsPerPage]);

  // Clear all filters
  const handleClearFilters = () => {
    setSearchTerm("");
    setSelectedRole("all");
  };

  return (
    <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold">Người dùng đã vô hiệu hóa</h2>
        <div className="text-gray-500 text-sm">
          Hiển thị {paginatedUsers.length} / {filteredUsers.length} (Tổng: {total})
        </div>
      </div>

      {/* Filters Section */}
      <div className="mb-4 space-y-3">
        <div className="flex flex-wrap gap-3 items-end">
          {/* Search Input */}
          <div className="flex-1 min-w-[250px]">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Tìm kiếm theo Email, Tên hoặc SĐT
            </label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
              <input
                type="text"
                placeholder="Nhập email, tên hoặc số điện thoại..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          </div>

          {/* Role Filter */}
          <div className="w-[200px]">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Lọc theo Vai trò
            </label>
            <select
              value={selectedRole}
              onChange={(e) => setSelectedRole(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="all">Tất cả vai trò</option>
              {availableRoles.map((role) => (
                <option key={role.role_code} value={role.role_code}>
                  {role.role_name}
                </option>
              ))}
            </select>
          </div>

          {/* Items per page */}
          <div className="w-[120px]">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Hiển thị
            </label>
            <select
              value={itemsPerPage}
              onChange={(e) => setItemsPerPage(Number(e.target.value))}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value={5}>5</option>
              <option value={10}>10</option>
              <option value={20}>20</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </div>

          {/* Clear Filters Button */}
          {(searchTerm || selectedRole !== "all") && (
            <Button
              variant="outline"
              onClick={handleClearFilters}
              className="flex items-center gap-2"
            >
              <X className="h-4 w-4" />
              Xóa bộ lọc
            </Button>
          )}
        </div>

        {/* Active Filters Display */}
        {(searchTerm || selectedRole !== "all") && (
          <div className="flex flex-wrap gap-2 text-sm">
            <span className="text-gray-600">Đang lọc:</span>
            {searchTerm && (
              <Badge variant="secondary" className="flex items-center gap-1">
                Tìm kiếm: "{searchTerm}"
                <X 
                  className="h-3 w-3 cursor-pointer" 
                  onClick={() => setSearchTerm("")}
                />
              </Badge>
            )}
            {selectedRole !== "all" && (
              <Badge variant="secondary" className="flex items-center gap-1">
                Vai trò: {availableRoles.find(r => r.role_code === selectedRole)?.role_name}
                <X 
                  className="h-3 w-3 cursor-pointer" 
                  onClick={() => setSelectedRole("all")}
                />
              </Badge>
            )}
          </div>
        )}
      </div>

      {loading && <div className="text-gray-500">Đang tải...</div>}
      {error && <div className="text-red-500 mb-2">{error}</div>}
      
      {!loading && filteredUsers.length === 0 && (
        <div className="text-center py-8 text-gray-500">
          {searchTerm || selectedRole !== "all" 
            ? "Không tìm thấy người dùng phù hợp với bộ lọc" 
            : "Không có người dùng đã xóa"}
        </div>
      )}

      {!loading && filteredUsers.length > 0 && (
        <>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>STT</TableHead>
                  <TableHead>Tên</TableHead>
                  <TableHead>Email</TableHead>
                  <TableHead>Vai trò</TableHead>
                  <TableHead>Trạng thái</TableHead>
                  <TableHead>Hành động</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {paginatedUsers.map((user, idx) => (
                  <TableRow key={user.user_id}>
                    <TableCell>{(currentPage - 1) * itemsPerPage + idx + 1}</TableCell>
                    <TableCell>{user.full_name}</TableCell>
                    <TableCell>{user.email}</TableCell>
                    <TableCell>
                      <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'}>
                        {user.role_name || "Chưa xác định"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      {user.status === 'active' ? (
                        <Badge variant="default" className="bg-green-500 text-white">Hoạt động</Badge>
                      ) : (
                        <Badge variant="destructive" className="bg-gray-500 text-white">Vô hiệu hóa</Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-2 items-center">
                        <Button size="sm" variant="outline" onClick={() => handleRestore(user.user_id)} title="Khôi phục">
                          <RotateCcw className="h-4 w-4" />
                        </Button>
                        <Button
                          size="sm"
                          variant="destructive"
                          onClick={() => handlePermanentDelete(user.user_id)}
                          title="Xóa vĩnh viễn"
                          className="flex items-center gap-1 bg-red-600 hover:bg-red-700 text-white font-semibold px-3 py-1 rounded"
                          disabled={loading}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                        <Button size="sm" variant="outline" onClick={() => onSelectUser(user.user_id)} title="Xem chi tiết">
                          <Eye className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          {/* Pagination Controls */}
          <div className="flex flex-col sm:flex-row justify-between items-center mt-4 gap-3">
            <div className="text-gray-600 text-sm">
              Trang {currentPage} / {totalPages || 1}
            </div>
            <div className="flex gap-2 items-center">
              <Button 
                size="sm" 
                variant="outline" 
                disabled={currentPage === 1} 
                onClick={() => setCurrentPage(1)}
              >
                ««
              </Button>
              <Button 
                size="sm" 
                variant="outline" 
                disabled={currentPage === 1} 
                onClick={() => setCurrentPage(currentPage - 1)}
              >
                « Trang trước
              </Button>
              
              {/* Page numbers */}
              <div className="flex gap-1">
                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                  let pageNum;
                  if (totalPages <= 5) {
                    pageNum = i + 1;
                  } else if (currentPage <= 3) {
                    pageNum = i + 1;
                  } else if (currentPage >= totalPages - 2) {
                    pageNum = totalPages - 4 + i;
                  } else {
                    pageNum = currentPage - 2 + i;
                  }
                  
                  return (
                    <Button
                      key={pageNum}
                      size="sm"
                      variant={currentPage === pageNum ? "default" : "outline"}
                      onClick={() => setCurrentPage(pageNum)}
                      className="min-w-[40px]"
                    >
                      {pageNum}
                    </Button>
                  );
                })}
              </div>

              <Button 
                size="sm" 
                variant="outline" 
                disabled={currentPage === totalPages || totalPages === 0} 
                onClick={() => setCurrentPage(currentPage + 1)}
              >
                Trang sau »
              </Button>
              <Button 
                size="sm" 
                variant="outline" 
                disabled={currentPage === totalPages || totalPages === 0} 
                onClick={() => setCurrentPage(totalPages)}
              >
                »»
              </Button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
