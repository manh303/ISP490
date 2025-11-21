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
import { Search, X, Calendar } from 'lucide-react';
import { Input } from '../../components/ui/figma/input';
import { useAuth } from "../../contexts/AuthContext";
interface ActivityLog {
    id: number;
    user_id: number;
    action: string;
    timestamp: string;
    details?: string;
    user_email?: string;
}

interface ActivityLogsTableProps {
    // Optional props if needed
}

export default function ActivityLogsTable({}: ActivityLogsTableProps) {
    const { user } = useAuth();
    console.log('Current user in ActivityLogsTable:', user);
    
    // Default dates: last 7 days
    const getDefaultStartDate = () => {
        const date = new Date();
        date.setDate(date.getDate() - 7);
        return date.toISOString().split('T')[0];
    };
    
    const getDefaultEndDate = () => {
        return new Date().toISOString().split('T')[0];
    };

    const [logs, setLogs] = useState<ActivityLog[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    
    // Filter states
    const [searchTerm, setSearchTerm] = useState("");
    const [selectedAction, setSelectedAction] = useState<string>("all");
    const [startDate, setStartDate] = useState<string>(getDefaultStartDate());
    const [endDate, setEndDate] = useState<string>(getDefaultEndDate());
    const [availableActions, setAvailableActions] = useState<string[]>([]);
    
    // Pagination states
    const [currentPage, setCurrentPage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(10);

    // Fetch logs
    const fetchLogs = async () => {
        setLoading(true);
        setError(null);
        try {
            const params: any = {};
            if (selectedAction !== "all") params.action = selectedAction;
            if (user?.user_id) {
                const userIdNum = parseInt(String(user.user_id));
                if (!isNaN(userIdNum)) {
                    params.user_id = userIdNum;
                }
            }
            if (startDate) params.start_date = startDate;
            if (endDate) params.end_date = endDate;
            
            console.log('API params:', params); // Debug log
            
            const data = await getActivityLogs(params);
            const logsArray = Array.isArray(data) ? data : data.logs || [];
            setLogs(logsArray);
            
            // Extract unique actions
            const actions = [...new Set(logsArray.map((log: ActivityLog) => log.action).filter(Boolean))].map(String);
            setAvailableActions(actions);
        } catch (err: any) {
            setError(err.message || 'Failed to fetch activity logs');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchLogs();
    }, [selectedAction, user?.user_id, startDate, endDate]);

    // Filtered logs based on search term
    const filteredLogs = useMemo(() => {
        return logs.filter(log =>
            log.action.toLowerCase().includes(searchTerm.toLowerCase()) ||
            log.user_email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
            log.details?.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }, [logs, searchTerm]);

    // Paginated logs
    const paginatedLogs = useMemo(() => {
        const startIndex = (currentPage - 1) * itemsPerPage;
        return filteredLogs.slice(startIndex, startIndex + itemsPerPage);
    }, [filteredLogs, currentPage, itemsPerPage]);

    const totalPages = Math.ceil(filteredLogs.length / itemsPerPage);

    const clearFilters = () => {
        setSearchTerm("");
        setSelectedAction("all");
        setStartDate(getDefaultStartDate());
        setEndDate(getDefaultEndDate());
        setCurrentPage(1);
    };

    if (loading) {
        return <div className="text-center py-8">Loading activity logs...</div>;
    }

    if (error) {
        return <div className="text-center py-8 text-red-500">Error: {error}</div>;
    }

    return (
        <div className="activity-logs-table">
            {/* Current User Info */}
            {user && (
                <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                    <p className="text-sm text-blue-800">
                        <strong>Filtering logs for user:</strong> {user.full_name} (ID: {user.user_id})
                    </p>
                </div>
            )}
            
            {/* Filters */}
            <div className="flex flex-wrap gap-4 mb-6">
                <div className="flex-1 min-w-[200px]">
                    <div className="relative">
                        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                        <Input
                            placeholder="Search logs, email, action..."
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
                    <option value="all">All Actions</option>
                    {availableActions.map(action => (
                        <option key={action} value={action}>{action}</option>
                    ))}
                </select>
                
                <Input
                    type="date"
                    placeholder="Start Date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className="w-40"
                />
                
                <Input
                    type="date"
                    placeholder="End Date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="w-40"
                />
                
                <Button variant="outline" onClick={clearFilters}>
                    <X className="h-4 w-4 mr-2" />
                    Clear Filters
                </Button>
            </div>

            {/* Table */}
            <div className="border rounded-lg">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>ID</TableHead>
                            <TableHead>User</TableHead>
                            <TableHead>Action</TableHead>
                            <TableHead>Timestamp</TableHead>
                            <TableHead>Details</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {paginatedLogs.map((log) => (
                            <TableRow key={log.id}>
                                <TableCell>{log.id}</TableCell>
                                <TableCell>{log.user_email || log.user_id}</TableCell>
                                <TableCell>
                                    <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-sm">
                                        {log.action}
                                    </span>
                                </TableCell>
                                <TableCell>{new Date(log.timestamp).toLocaleString()}</TableCell>
                                <TableCell className="max-w-xs truncate">{log.details || '-'}</TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
                <div className="flex justify-between items-center mt-4">
                    <div className="flex items-center gap-2">
                        <span className="text-sm text-gray-600">Rows per page:</span>
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
                            Previous
                        </Button>
                        <span className="text-sm">
                            Page {currentPage} of {totalPages}
                        </span>
                        <Button
                            variant="outline"
                            onClick={() => setCurrentPage(prev => Math.min(prev + 1, totalPages))}
                            disabled={currentPage === totalPages}
                        >
                            Next
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
}