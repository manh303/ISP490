import React, { useEffect, useState, useMemo } from "react";
import { getActivityLogs } from "../../services/adminApi";
import { Button } from '../../components/ui/figma/button';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '../../components/ui/figma/table';
import { Search, X, Calendar, Eye } from 'lucide-react';
import { Input } from '../../components/ui/figma/input';
import { useAuth } from "../../contexts/AuthContext";
interface ActivityLog {
    log_id: number;
    user_id: number | null;
    email: string | null;
    action: string;
    resource: string;
    details: string;
    ip_address: string;
    user_agent: string;
    status: string;
    created_at: string;
}

interface ActivityLogsTableProps {
    // Optional props if needed
}

export default function ActivityLogsTable({}: ActivityLogsTableProps) {
    const { user } = useAuth();
    // Function to get action type (no longer HTTP method)
    const getActionType = (action: string) => {
        return action; // Actions are now direct like "USER_SIGNIN"
    };

    // Function to format details
    const formatDetails = (details: string) => {
        try {
            const parsed = JSON.parse(details);
            const parts = [];
            if (parsed.role) parts.push(`Vai trò: ${parsed.role}`);
            if (parsed.method) parts.push(`Phương thức: ${parsed.method}`);
            if (parsed.ip_address) parts.push(`IP: ${parsed.ip_address}`);
            if (parsed.user_agent) parts.push(`Trình duyệt: ${parsed.user_agent}`);
            if (parsed.status_code) parts.push(`Mã trạng thái: ${parsed.status_code}`);
            if (parsed.process_time) parts.push(`Thời gian xử lý: ${parsed.process_time}s`);
            if (parsed.path) parts.push(`Đường dẫn: ${parsed.path}`);
            if (parsed.query_params && Object.keys(parsed.query_params).length > 0) {
                parts.push(`Tham số: ${JSON.stringify(parsed.query_params, null, 2)}`);
            }
            // Add any other fields that might be present
            Object.keys(parsed).forEach(key => {
                if (!['role', 'method', 'ip_address', 'user_agent', 'status_code', 'process_time', 'path', 'query_params'].includes(key)) {
                    parts.push(`${key}: ${parsed[key]}`);
                }
            });
            return parts.join('\n');
        } catch {
            return details;
        }
    };

    const [logs, setLogs] = useState<ActivityLog[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    
    // Filter states
    const [searchTerm, setSearchTerm] = useState("");
    const [selectedAction, setSelectedAction] = useState<string>("all");
    const [startDate, setStartDate] = useState<string>("");
    const [endDate, setEndDate] = useState<string>("");
    
    // Pagination states
    const [currentPage, setCurrentPage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(10);

    // Modal states
    const [selectedLog, setSelectedLog] = useState<ActivityLog | null>(null);
    const [showModal, setShowModal] = useState(false);

    // CRUD operations mapping - dynamically generated from API response
    const crudOperations = useMemo(() => {
        const uniqueActions = Array.from(new Set(logs.map(log => log.action)));
        const actionLabels: { [key: string]: string } = {
            'USER_SIGNIN': 'Đăng nhập',
            'USER_SIGNOUT': 'Đăng xuất',
            'USER_REGISTER': 'Đăng ký',
            'USER_UPDATE': 'Cập nhật người dùng',
            'DATA_READ': 'Đọc dữ liệu',
            'DATA_CREATE': 'Tạo dữ liệu',
            'DATA_UPDATE': 'Cập nhật dữ liệu',
            'DATA_DELETE': 'Xóa dữ liệu',
            'REPORT_GENERATE': 'Tạo báo cáo',
            'SYSTEM_ACCESS': 'Truy cập hệ thống'
        };
        
        return [
            { value: 'all', label: 'Tất cả' },
            ...uniqueActions.map(action => ({
                value: action,
                label: actionLabels[action] || action.replace(/_/g, ' ')
            }))
        ];
    }, [logs]);

    // Fetch logs
    const fetchLogs = async () => {
        setLoading(true);
        setError(null);
        try {
            const params: any = {};
            if (startDate) params.start_date = startDate;
            if (endDate) params.end_date = endDate;
            if (user?.user_id) params.user_id = user.user_id;
            const data = await getActivityLogs(params);
            const logsArray = data?.data?.logs || [];
            setLogs(logsArray);
        } catch (err: any) {
            setError(err.message || 'Failed to fetch activity logs');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchLogs();
    }, [startDate, endDate]);

    // Filtered logs based on search term and CRUD action
    const filteredLogs = useMemo(() => {
        return logs.filter(log => {
            const matchesSearch = 
                log.action.toLowerCase().includes(searchTerm.toLowerCase()) ||
                log.email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                log.resource?.toLowerCase().includes(searchTerm.toLowerCase());
            
            const actionType = getActionType(log.action);
            const matchesAction = selectedAction === "all" || actionType === selectedAction;
            
            return matchesSearch && matchesAction;
        });
    }, [logs, searchTerm, selectedAction]);

    // Paginated logs
    const paginatedLogs = useMemo(() => {
        const startIndex = (currentPage - 1) * itemsPerPage;
        return filteredLogs.slice(startIndex, startIndex + itemsPerPage);
    }, [filteredLogs, currentPage, itemsPerPage]);

    const totalPages = Math.ceil(filteredLogs.length / itemsPerPage);

    const clearFilters = () => {
        setSearchTerm("");
        setSelectedAction("all");
        setStartDate("");
        setEndDate("");
        setCurrentPage(1);
    };

    const openDetailsModal = (log: ActivityLog) => {
        setSelectedLog(log);
        setShowModal(true);
    };

    const closeDetailsModal = () => {
        setSelectedLog(null);
        setShowModal(false);
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
                            placeholder="Tìm kiếm nhật ký..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="pl-10"
                        />
                    </div>
                </div>
                
                <select
                    value={selectedAction}
                    onChange={(e) => setSelectedAction(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-md"
                >
                    {crudOperations.map(operation => (
                        <option key={operation.value} value={operation.value}>
                            {operation.label}
                        </option>
                    ))}
                </select>
                
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
            </div>

            {/* Table */}
            <div className="border rounded-lg">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>ID Nhật ký</TableHead>
                            <TableHead>Người dùng</TableHead>
                            <TableHead>Hành động</TableHead>
                            <TableHead>Tài nguyên</TableHead>
                            <TableHead>Trạng thái</TableHead>
                            <TableHead>Thời gian</TableHead>
                            <TableHead>Chi tiết</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {paginatedLogs.map((log) => (
                            <TableRow key={log.log_id}>
                                <TableCell>{log.log_id}</TableCell>
                                <TableCell>{log.email || log.user_id || 'N/A'}</TableCell>
                                <TableCell>
                                    <span className={`px-2 py-1 rounded text-sm ${
                                        log.action.includes('SIGNIN') || log.action.includes('LOGIN') ? 'bg-green-100 text-green-800' :
                                        log.action.includes('LOGOUT') ? 'bg-gray-100 text-gray-800' :
                                        log.action.includes('CREATE') || log.action.includes('REGISTER') ? 'bg-blue-100 text-blue-800' :
                                        log.action.includes('UPDATE') || log.action.includes('EDIT') ? 'bg-yellow-100 text-yellow-800' :
                                        log.action.includes('DELETE') || log.action.includes('REMOVE') ? 'bg-red-100 text-red-800' :
                                        log.action.includes('READ') || log.action.includes('VIEW') ? 'bg-purple-100 text-purple-800' :
                                        log.action.includes('REPORT') ? 'bg-indigo-100 text-indigo-800' :
                                        'bg-gray-100 text-gray-800'
                                    }`}>
                                        {log.action.replace(/_/g, ' ')}
                                    </span>
                                </TableCell>
                                <TableCell className="max-w-xs truncate">{log.resource}</TableCell>
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
            {totalPages > 1 && (
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
                            <option value={25}>25</option>
                            <option value={50}>50</option>
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
                            Trang {currentPage} của {totalPages}
                        </span>
                        <Button
                            variant="outline"
                            onClick={() => setCurrentPage(prev => Math.min(prev + 1, totalPages))}
                            disabled={currentPage === totalPages}
                        >
                            Tiếp
                        </Button>
                    </div>
                </div>
            )}
            
            {/* Details Modal */}
            {showModal && selectedLog && (
                <div className="fixed inset-0 backdrop-blur-sm flex items-center justify-center z-50">
                    <div className="bg-white rounded-lg p-6 max-w-2xl w-full mx-4 max-h-[80vh] overflow-y-auto shadow-2xl border">
                        <div className="flex justify-between items-center mb-4">
                            <h3 className="text-lg font-semibold">Chi tiết nhật ký #{selectedLog.log_id}</h3>
                            <Button variant="outline" size="sm" onClick={closeDetailsModal}>
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                        
                        <div className="space-y-4">
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="text-sm font-medium text-gray-600">ID Nhật ký</label>
                                    <p className="text-sm">{selectedLog.log_id}</p>
                                </div>
                                <div>
                                    <label className="text-sm font-medium text-gray-600">Người dùng</label>
                                    <p className="text-sm">{selectedLog.email || selectedLog.user_id || 'N/A'}</p>
                                </div>
                                <div>
                                    <label className="text-sm font-medium text-gray-600">Hành động</label>
                                    <p className="text-sm">{selectedLog.action.replace(/_/g, ' ')}</p>
                                </div>
                                <div>
                                    <label className="text-sm font-medium text-gray-600">Tài nguyên</label>
                                    <p className="text-sm">{selectedLog.resource}</p>
                                </div>
                                <div>
                                    <label className="text-sm font-medium text-gray-600">Trạng thái</label>
                                    <p className="text-sm">{selectedLog.status === 'success' ? 'Thành công' : 'Thất bại'}</p>
                                </div>
                                <div>
                                    <label className="text-sm font-medium text-gray-600">Thời gian</label>
                                    <p className="text-sm">{new Date(selectedLog.created_at).toLocaleString()}</p>
                                </div>
                            </div>
                            
                            <div>
                                <label className="text-sm font-medium text-gray-600">IP Address</label>
                                <p className="text-sm">{selectedLog.ip_address || 'N/A'}</p>
                            </div>
                            
                            <div>
                                <label className="text-sm font-medium text-gray-600">User Agent</label>
                                <p className="text-sm break-all">{selectedLog.user_agent || 'N/A'}</p>
                            </div>
                            
                            <div>
                                <label className="text-sm font-medium text-gray-600">Chi tiết kỹ thuật</label>
                                <pre className="text-xs bg-gray-100 p-3 rounded mt-1 whitespace-pre-wrap">
                                    {formatDetails(selectedLog.details)}
                                </pre>
                            </div>
                        </div>
                        
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