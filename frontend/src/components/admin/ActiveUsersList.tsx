
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
import { Edit, Eye, Trash2, Search, X } from 'lucide-react';

interface User {
    user_id: number;
    email: string;
    full_name: string;
    phone: string;
    status: string;
    role_code: string;
    role_name: string;
    last_login_at: string;
    created_at: string;
    updated_at: string;
}

interface ActiveUsersListProps {
    onSelectUser: (id: number, editMode?: boolean) => void;
}

export default function ActiveUsersList({ onSelectUser }: ActiveUsersListProps) {
    const [users, setUsers] = useState<User[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [total, setTotal] = useState(0);
    
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
                // Fetch users
                const userData = await userApi.getActiveUsers();
                if (userData.success) {
                    setUsers(userData.data);
                    setTotal(userData.total);
                } else {
                    throw new Error("API returned error");
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
                 const detail = err?.response?.data?.detail;
            let errorMsg = "Unable to create role";
            if (typeof detail === 'string') {
                errorMsg = detail;
            } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
                errorMsg = detail[0].msg;
            } else if (detail?.msg) {
                errorMsg = detail.msg;
            }
                setError(errorMsg);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    // Filter and paginate users
    const filteredUsers = useMemo(() => {
        let filtered = users;

        // Filter by search term (email, name, or phone)
        if (searchTerm.trim()) {
            const searchLower = searchTerm.toLowerCase();
            filtered = filtered.filter(user => 
                (user.email?.toLowerCase().includes(searchLower)) ||
                (user.full_name?.toLowerCase().includes(searchLower)) ||
                (user.phone?.includes(searchTerm))
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
                <h2 className="text-xl font-semibold">Active Users</h2>
                <div className="text-gray-500 text-sm">
                    Showing {paginatedUsers.length} / {filteredUsers.length} (Total: {total})
                </div>
            </div>

            {/* Filters Section */}
            <div className="mb-4 space-y-3">
                <div className="flex flex-wrap gap-3 items-end">
                    {/* Search Input */}
                    <div className="flex-1 min-w-[250px]">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Search by Email, Name or Phone
                        </label>
                        <div className="relative">
                            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                            <input
                                type="text"
                                placeholder="Enter email, name or phone number..."
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                            />
                        </div>
                    </div>

                    {/* Role Filter */}
                    <div className="w-[200px]">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Filter by Role
                        </label>
                        <select
                            value={selectedRole}
                            onChange={(e) => setSelectedRole(e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                        >
                            <option value="all">All Roles</option>
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
                            Show
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
                            Clear filters
                        </Button>
                    )}
                </div>

                {/* Active Filters Display */}
                {(searchTerm || selectedRole !== "all") && (
                    <div className="flex flex-wrap gap-2 text-sm">
                        <span className="text-gray-600">Filtering:</span>
                        {searchTerm && (
                            <Badge variant="secondary" className="flex items-center gap-1">
                                Search: "{searchTerm}"
                                <X 
                                    className="h-3 w-3 cursor-pointer" 
                                    onClick={() => setSearchTerm("")}
                                />
                            </Badge>
                        )}
                        {selectedRole !== "all" && (
                            <Badge variant="secondary" className="flex items-center gap-1">
                                Role: {availableRoles.find(r => r.role_code === selectedRole)?.role_name}
                                <X 
                                    className="h-3 w-3 cursor-pointer" 
                                    onClick={() => setSelectedRole("all")}
                                />
                            </Badge>
                        )}
                    </div>
                )}
            </div>

            {loading && <div className="text-gray-500">Loading...</div>}
            {error && <div className="text-red-500 mb-2">{error}</div>}
            
            {!loading && filteredUsers.length === 0 && (
                <div className="text-center py-8 text-gray-500">
                    {searchTerm || selectedRole !== "all" 
                        ? "No users found matching the filters" 
                        : "No users available"}
                </div>
            )}

            {!loading && filteredUsers.length > 0 && (
                <>
                    <div className="overflow-x-auto">
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>No.</TableHead>
                                    <TableHead>Name</TableHead>
                                    <TableHead>Email</TableHead>
                                    <TableHead>Phone</TableHead>
                                    <TableHead>Role</TableHead>
                                    <TableHead>Status</TableHead>
                                    <TableHead>Last Login</TableHead>
                                    <TableHead>Actions</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {paginatedUsers.map((user, idx) => (
                                    <TableRow key={user.user_id}>
                                        <TableCell>{(currentPage - 1) * itemsPerPage + idx + 1}</TableCell>
                                        <TableCell>{user.full_name}</TableCell>
                                        <TableCell>{user.email}</TableCell>
                                        <TableCell>{user.phone}</TableCell>
                                        <TableCell>
                                            <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'}>
                                                {user.role_name}
                                            </Badge>
                                        </TableCell>
                                        <TableCell>
                                            {user.status === 'active' ? (
                                                <Badge variant="default" className="bg-green-500 text-white">Active</Badge>
                                            ) : (
                                                <Badge variant="destructive" className="bg-gray-500 text-white">Disabled</Badge>
                                            )}
                                        </TableCell>
                                        <TableCell>{user.last_login_at ? new Date(user.last_login_at).toLocaleString() : '-'}</TableCell>
                                        <TableCell>
                                            <div className="flex gap-2">
                                                <Button size="sm" variant="outline" onClick={() => onSelectUser(user.user_id, false)} title="View Details">
                                                    <Eye className="h-4 w-4" />
                                                </Button>

                                                <Button size="sm" variant="outline" onClick={() => onSelectUser(user.user_id, true)} title="Edit">
                                                    <Edit className="h-4 w-4" />
                                                </Button>
                                                <Button size="sm" variant="destructive" title="Delete">
                                                    <Trash2 className="h-4 w-4" />
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
                                « Previous
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
