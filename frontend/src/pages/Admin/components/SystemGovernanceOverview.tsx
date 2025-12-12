import React from 'react';
import { Card, CardContent } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Users, Shield, Database, Brain, TrendingUp, ArrowRight } from 'lucide-react';
import { Link } from 'react-router-dom';

interface SystemGovernanceOverviewProps {
    totalUsers: number;
    activeUsersLast30Days: number;
    totalRoles: number;
    totalPermissions: number;
    avgPermissionsPerRole: number;
    totalDatasets: number;
    datasetsWithOwner: number;
    datasetsWithoutOwner: number;
    totalDSSScenarios: number;
    activeMLModels: number;
    lastRetrainDate: string | null;
    isLoading?: boolean;
}

export default function SystemGovernanceOverview({
    totalUsers,
    activeUsersLast30Days,
    totalRoles,
    totalPermissions,
    avgPermissionsPerRole,
    totalDatasets,
    datasetsWithOwner,
    datasetsWithoutOwner,
    totalDSSScenarios,
    activeMLModels,
    lastRetrainDate,
    isLoading = false,
}: SystemGovernanceOverviewProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <TrendingUp className="w-5 h-5 text-blue-600" />
                    System & Governance Overview
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    {[1, 2, 3, 4].map((i) => (
                        <Card key={i} className="animate-pulse">
                            <CardContent className="p-6">
                                <div className="h-20 bg-gray-200 dark:bg-gray-700 rounded"></div>
                            </CardContent>
                        </Card>
                    ))}
                </div>
            </div>
        );
    }

    const kpiCards = [
        {
            title: 'Total Users',
            value: totalUsers,
            subtext: `Active last 30 days: ${activeUsersLast30Days}`,
            icon: Users,
            color: 'text-blue-600',
            bgColor: 'bg-blue-50 dark:bg-blue-950',
            link: '/admin/users',
        },
        {
            title: 'Roles & Permissions',
            value: `${totalRoles} roles`,
            subtext: `${totalPermissions} permissions • Avg: ${avgPermissionsPerRole.toFixed(1)}/role`,
            icon: Shield,
            color: 'text-purple-600',
            bgColor: 'bg-purple-50 dark:bg-purple-950',
            link: '/admin/roles',
        },
        {
            title: 'Registered Datasets',
            value: totalDatasets,
            subtext: (
                <span>
                    With owner: <span className="text-green-600">{datasetsWithOwner}</span> •
                    Without: <span className="text-orange-600">{datasetsWithoutOwner}</span>
                </span>
            ),
            icon: Database,
            color: 'text-green-600',
            bgColor: 'bg-green-50 dark:bg-green-950',
            link: '/admin/catalog',
        },
        {
            title: 'DSS & ML Assets',
            value: `${totalDSSScenarios} scenarios`,
            subtext: (
                <span>
                    {activeMLModels} ML models • Last retrain: {lastRetrainDate || 'N/A'}
                </span>
            ),
            icon: Brain,
            color: 'text-orange-600',
            bgColor: 'bg-orange-50 dark:bg-orange-950',
            link: '/admin/dss-scenarios',
        },
    ];

    return (
        <div className="space-y-4">
            <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                <TrendingUp className="w-5 h-5 text-blue-600" />
                System & Governance Overview
            </h2>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {kpiCards.map((card, index) => {
                    const Icon = card.icon;
                    return (
                        <Link to={card.link} key={index}>
                            <Card className="hover:shadow-lg transition-all duration-200 cursor-pointer group border-l-4 border-l-transparent hover:border-l-blue-500">
                                <CardContent className="p-5">
                                    <div className="flex items-start justify-between">
                                        <div className={`p-2 rounded-lg ${card.bgColor}`}>
                                            <Icon className={`w-5 h-5 ${card.color}`} />
                                        </div>
                                        <ArrowRight className="w-4 h-4 text-gray-400 opacity-0 group-hover:opacity-100 transition-opacity" />
                                    </div>

                                    <div className="mt-4">
                                        <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                            {typeof card.value === 'number' ? card.value.toLocaleString() : card.value}
                                        </div>
                                        <div className="text-sm font-medium text-gray-600 dark:text-gray-400 mt-1">
                                            {card.title}
                                        </div>
                                        <div className="text-xs text-gray-500 dark:text-gray-500 mt-2">
                                            {card.subtext}
                                        </div>
                                    </div>
                                </CardContent>
                            </Card>
                        </Link>
                    );
                })}
            </div>
        </div>
    );
}
