import React, { useEffect, useState, useMemo } from "react";
import { getActivityLogs, exportActivityLogs, getActivityLogDetail } from "../../services/adminApi";
import { Button } from '../../components/ui/figma/button';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '../../components/ui/figma/table';
import { Search, X, Calendar, Eye, Download } from 'lucide-react';
import { Input } from '../../components/ui/figma/input';
import { useAuth } from "../../contexts/AuthContext";

interface ActivityLog {
    log_id: number;
    user_id: number | null;
    email: string | null;
    full_name: string | null;
    role_at_time: string | null;
    action: string;
    module: string | null;
    resource_type: string | null;
    resource: string | null;
    request_method: string | null;
    status: string;
    ip_address: string | null;
    user_agent: string | null;
    message: string | null;
    created_at: string;
}

interface ActivityLogDetail extends ActivityLog {
    request_payload: any;
    before_data: any;
    after_data: any;
    details: any;
}

interface Pagination {
    page: number;
    limit: number;
    total: number;
    pages: number;
}

interface ActivityLogsTableProps {
    // Optional props if needed
}

export default function ActivityLogsTable({}: ActivityLogsTableProps) {
    const { user } = useAuth();

    const [logs, setLogs] = useState<ActivityLog[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [pagination, setPagination] = useState<Pagination | null>(null);
    
    // Filter states
    const [searchTerm, setSearchTerm] = useState("");
    const [selectedAction, setSelectedAction] = useState<string>("");
    const [selectedModule, setSelectedModule] = useState<string>("");
    const [selectedStatus, setSelectedStatus] = useState<string>("");
    const [selectedRole, setSelectedRole] = useState<string>("");
    const [startDate, setStartDate] = useState<string>("");
    const [endDate, setEndDate] = useState<string>("");
    const [sortBy, setSortBy] = useState<string>("-created_at");
    
    // Pagination states
    const [currentPage, setCurrentPage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(20);

    // Modal states
    const [selectedLog, setSelectedLog] = useState<ActivityLog | null>(null);
    const [selectedLogDetail, setSelectedLogDetail] = useState<ActivityLogDetail | null>(null);
    const [showModal, setShowModal] = useState(false);
    const [detailLoading, setDetailLoading] = useState(false);

    // Unique actions for filter
    const uniqueActions = useMemo(() => {
        const actions = logs.map(log => log.action);
        return Array.from(new Set(actions));
    }, [logs]);

    // Fetch logs
    const fetchLogs = async (page = currentPage) => {
        setLoading(true);
        setError(null);
        try {
            const params: any = {
                page,
                limit: itemsPerPage,
                sort: sortBy,
            };
            if (searchTerm) params.keyword = searchTerm;
            if (selectedAction) params.action = selectedAction;
            if (selectedModule) params.module = selectedModule;
            if (selectedStatus) params.status = selectedStatus;
            if (selectedRole) params.role = selectedRole;
            if (startDate) params.start_date = startDate;
            if (endDate) params.end_date = endDate;
            
            const response = await getActivityLogs(params);
            if (response.success) {
                setLogs(response.data);
                setPagination(response.pagination);
            } else {
                setError('Failed to fetch activity logs');
            }
        } catch (err: any) {
            setError(err.message || 'Failed to fetch activity logs');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchLogs();
    }, [currentPage, itemsPerPage, sortBy]);

    const handleFilterChange = () => {
        setCurrentPage(1);
        fetchLogs(1);
    };

    useEffect(() => {
        const timeoutId = setTimeout(() => {
            handleFilterChange();
        }, 500);
        return () => clearTimeout(timeoutId);
    }, [searchTerm, selectedAction, selectedModule, selectedStatus, selectedRole, startDate, endDate]);

    const clearFilters = () => {
        setSearchTerm("");
        setSelectedAction("");
        setSelectedModule("");
        setSelectedStatus("");
        setSelectedRole("");
        setStartDate("");
        setEndDate("");
        setCurrentPage(1);
        fetchLogs(1);
    };

    const handleExport = async () => {
        try {
            const params: any = {};
            if (searchTerm) params.keyword = searchTerm;
            if (selectedAction) params.action = selectedAction;
            if (selectedModule) params.module = selectedModule;
            if (selectedStatus) params.status = selectedStatus;
            if (selectedRole) params.role = selectedRole;
            if (startDate) params.start_date = startDate;
            if (endDate) params.end_date = endDate;
            
            const blob = await exportActivityLogs(params);
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `activity_logs_${new Date().toISOString().split('T')[0]}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
        } catch (err: any) {
            setError(err.message || 'Failed to export activity logs');
        }
    };

    const openDetailsModal = async (log: ActivityLog) => {
        setSelectedLog(log);
        setSelectedLogDetail(null);
        setShowModal(true);
        setDetailLoading(true);
        
        try {
            const response = await getActivityLogDetail(log.log_id);
            if (response.success) {
                setSelectedLogDetail(response.data);
            } else {
                setError('Failed to fetch log details');
            }
        } catch (err: any) {
            setError(err.message || 'Failed to fetch log details');
        } finally {
            setDetailLoading(false);
        }
    };

    const closeDetailsModal = () => {
        setSelectedLog(null);
        setSelectedLogDetail(null);
        setShowModal(false);
        setDetailLoading(false);
    };

    if (loading) {
        return <div className="text-center py-8">Đang tải nhật ký hoạt động...</div>;
    }

    if (error) {
        return <div className="text-center py-8 text-red-500">Lỗi: {error}</div>;
    }

    return (
        <div className="activity-logs-table">
            {/* Filters */}
            <div className="flex flex-wrap gap-4 mb-6">
                <div className="flex-1 min-w-[200px]">
                    <div className="relative">
                        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                        <Input
                            placeholder="Tìm kiếm..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="pl-10"
                        />
                    </div>
                </div>
                
                <select
                    value={selectedAction}
                    onChange={(e) => setSelectedAction(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-md w-32"
                >
                    <option value="">Tất cả hành động</option>
                    {uniqueActions.map(action => (
                        <option key={action} value={action}>{action.replace(/_/g, ' ')}</option>
                    ))}
                </select>
                
                <select
                    value={selectedModule}
                    onChange={(e) => setSelectedModule(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-md"
                >
                    <option value="">Tất cả module</option>
                    <option value="IAM">IAM</option>
                    <option value="ANALYTICS">Analytics</option>
                    <option value="DSS">DSS</option>
                    <option value="ML">ML</option>
                    <option value="DATA_PIPELINE">Data Pipeline</option>
                </select>
                
                <select
                    value={selectedStatus}
                    onChange={(e) => setSelectedStatus(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-md"
                >
                    <option value="">Tất cả trạng thái</option>
                    <option value="success">Thành công</option>
                    <option value="error">Lỗi</option>
                </select>
                
                <Input
                    placeholder="Vai trò"
                    value={selectedRole}
                    onChange={(e) => setSelectedRole(e.target.value)}
                    className="w-32"
                />
                
                <Input
                    type="date"
                    placeholder="Ngày bắt đầu"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className="w-40"
                />
                
                <Input
                    type="date"
                    placeholder="Ngày kết thúc"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="w-40"
                />
                
                <Button variant="outline" onClick={clearFilters}>
                    <X className="h-4 w-4 mr-2" />
                    Xóa bộ lọc
                </Button>
                
                <Button variant="outline" onClick={handleExport}>
                    <Download className="h-4 w-4 mr-2" />
                    Xuất CSV
                </Button>
            </div>

            {/* Table */}
            <div className="border rounded-lg">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>ID</TableHead>
                            <TableHead>Người dùng</TableHead>
                            <TableHead>Hành động</TableHead>
                            <TableHead>Module</TableHead>
                            <TableHead>Tài nguyên</TableHead>
                            <TableHead>Phương thức</TableHead>
                            <TableHead>Trạng thái</TableHead>
                            <TableHead>Thời gian</TableHead>
                            <TableHead>Chi tiết</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {logs.map((log) => (
                            <TableRow key={log.log_id}>
                                <TableCell>{log.log_id}</TableCell>
                                <TableCell>
                                    <div>
                                        <div className="font-medium">{log.full_name || log.email || 'N/A'}</div>
                                        <div className="text-sm text-gray-500">{log.email}</div>
                                        {log.role_at_time && <div className="text-xs text-gray-400">{log.role_at_time}</div>}
                                    </div>
                                </TableCell>
                                <TableCell>
                                    <span className="px-2 py-1 rounded text-sm bg-blue-100 text-blue-800">
                                        {log.action.replace(/_/g, ' ')}
                                    </span>
                                </TableCell>
                                <TableCell>
                                    {log.module && (
                                        <span className="px-2 py-1 rounded text-sm bg-purple-100 text-purple-800">
                                            {log.module}
                                        </span>
                                    )}
                                </TableCell>
                                <TableCell className="max-w-xs truncate">
                                    {log.resource_type && log.resource ? `${log.resource_type}#${log.resource}` : log.resource || 'N/A'}
                                </TableCell>
                                <TableCell>
                                    {log.request_method && (
                                        <span className="px-2 py-1 rounded text-sm bg-gray-100 text-gray-800">
                                            {log.request_method}
                                        </span>
                                    )}
                                </TableCell>
                                <TableCell>
                                    <span className={`px-2 py-1 rounded text-sm ${
                                        log.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                                    }`}>
                                        {log.status === 'success' ? 'Thành công' : 'Thất bại'}
                                    </span>
                                </TableCell>
                                <TableCell>{new Date(log.created_at).toLocaleString()}</TableCell>
                                <TableCell>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => openDetailsModal(log)}
                                        className="h-8 px-2"
                                    >
                                        <Eye className="h-4 w-4 mr-1" />
                                        Chi tiết
                                    </Button>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>

            {/* Pagination */}
            {pagination && pagination.pages > 1 && (
                <div className="flex justify-between items-center mt-4">
                    <div className="flex items-center gap-2">
                        <span className="text-sm text-gray-600">Số hàng mỗi trang:</span>
                        <select
                            value={itemsPerPage}
                            onChange={(e) => {
                                setItemsPerPage(Number(e.target.value));
                                setCurrentPage(1);
                            }}
                            className="px-2 py-1 border border-gray-300 rounded"
                        >
                            <option value={10}>10</option>
                            <option value={20}>20</option>
                            <option value={50}>50</option>
                            <option value={100}>100</option>
                        </select>
                    </div>
                    
                    <div className="flex items-center gap-2">
                        <Button
                            variant="outline"
                            onClick={() => setCurrentPage(prev => Math.max(prev - 1, 1))}
                            disabled={currentPage === 1}
                        >
                            Trước
                        </Button>
                        <span className="text-sm">
                            Trang {pagination.page} của {pagination.pages} (Tổng: {pagination.total})
                        </span>
                        <Button
                            variant="outline"
                            onClick={() => setCurrentPage(prev => Math.min(prev + 1, pagination.pages))}
                            disabled={currentPage === pagination.pages}
                        >
                            Tiếp
                        </Button>
                    </div>
                </div>
            )}
            
            {/* Details Modal */}
            {showModal && selectedLog && (
                <div className="fixed inset-0 backdrop-blur-sm flex items-center justify-center z-50">
                    <div className="bg-white rounded-lg p-6 max-w-4xl w-full mx-4 max-h-[90vh] overflow-y-auto shadow-2xl border">
                        <div className="flex justify-between items-center mb-4">
                            <h3 className="text-lg font-semibold">Chi tiết nhật ký #{selectedLog.log_id}</h3>
                            <Button variant="outline" size="sm" onClick={closeDetailsModal}>
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                        
                        {detailLoading ? (
                            <div className="text-center py-8">Đang tải chi tiết...</div>
                        ) : selectedLogDetail ? (
                            <div className="space-y-6">
                                {/* Basic Information */}
                                <div className="grid grid-cols-2 gap-4">
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">ID Nhật ký</label>
                                        <p className="text-sm">{selectedLogDetail.log_id}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">User ID</label>
                                        <p className="text-sm">{selectedLogDetail.user_id || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Email</label>
                                        <p className="text-sm">{selectedLogDetail.email || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Họ tên</label>
                                        <p className="text-sm">{selectedLogDetail.full_name || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Vai trò</label>
                                        <p className="text-sm">{selectedLogDetail.role_at_time || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Hành động</label>
                                        <p className="text-sm">{selectedLogDetail.action.replace(/_/g, ' ')}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Module</label>
                                        <p className="text-sm">{selectedLogDetail.module || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Loại tài nguyên</label>
                                        <p className="text-sm">{selectedLogDetail.resource_type || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Tài nguyên</label>
                                        <p className="text-sm">{selectedLogDetail.resource || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Phương thức</label>
                                        <p className="text-sm">{selectedLogDetail.request_method || 'N/A'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Trạng thái</label>
                                        <p className="text-sm">{selectedLogDetail.status === 'success' ? 'Thành công' : 'Thất bại'}</p>
                                    </div>
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Thời gian</label>
                                        <p className="text-sm">{new Date(selectedLogDetail.created_at).toLocaleString()}</p>
                                    </div>
                                </div>
                                
                                <div>
                                    <label className="text-sm font-medium text-gray-600">IP Address</label>
                                    <p className="text-sm">{selectedLogDetail.ip_address || 'N/A'}</p>
                                </div>
                                
                                <div>
                                    <label className="text-sm font-medium text-gray-600">User Agent</label>
                                    <p className="text-sm break-all">{selectedLogDetail.user_agent || 'N/A'}</p>
                                </div>
                                
                                {selectedLogDetail.message && (
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Thông điệp</label>
                                        <p className="text-sm">{selectedLogDetail.message}</p>
                                    </div>
                                )}
                                
                                {/* Request Payload */}
                                {selectedLogDetail.request_payload && (
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Dữ liệu yêu cầu (Request Payload)</label>
                                        <pre className="text-xs bg-gray-100 p-3 rounded mt-1 whitespace-pre-wrap overflow-x-auto">
                                            {typeof selectedLogDetail.request_payload === 'string' 
                                                ? selectedLogDetail.request_payload 
                                                : JSON.stringify(selectedLogDetail.request_payload, null, 2)}
                                        </pre>
                                    </div>
                                )}
                                
                                {/* Before Data */}
                                {selectedLogDetail.before_data && (
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Dữ liệu trước khi thay đổi (Before Data)</label>
                                        <pre className="text-xs bg-blue-50 p-3 rounded mt-1 whitespace-pre-wrap overflow-x-auto">
                                            {typeof selectedLogDetail.before_data === 'string' 
                                                ? selectedLogDetail.before_data 
                                                : JSON.stringify(selectedLogDetail.before_data, null, 2)}
                                        </pre>
                                    </div>
                                )}
                                
                                {/* After Data */}
                                {selectedLogDetail.after_data && (
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Dữ liệu sau khi thay đổi (After Data)</label>
                                        <pre className="text-xs bg-green-50 p-3 rounded mt-1 whitespace-pre-wrap overflow-x-auto">
                                            {typeof selectedLogDetail.after_data === 'string' 
                                                ? selectedLogDetail.after_data 
                                                : JSON.stringify(selectedLogDetail.after_data, null, 2)}
                                        </pre>
                                    </div>
                                )}
                                
                                {/* Details */}
                                {selectedLogDetail.details && (
                                    <div>
                                        <label className="text-sm font-medium text-gray-600">Chi tiết kỹ thuật (Details)</label>
                                        <pre className="text-xs bg-yellow-50 p-3 rounded mt-1 whitespace-pre-wrap overflow-x-auto">
                                            {typeof selectedLogDetail.details === 'string' 
                                                ? selectedLogDetail.details 
                                                : JSON.stringify(selectedLogDetail.details, null, 2)}
                                        </pre>
                                    </div>
                                )}
                            </div>
                        ) : (
                            <div className="text-center py-8 text-red-500">Không thể tải chi tiết nhật ký</div>
                        )}
                        
                        <div className="flex justify-end mt-6">
                            <Button onClick={closeDetailsModal}>
                                Đóng
                            </Button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}