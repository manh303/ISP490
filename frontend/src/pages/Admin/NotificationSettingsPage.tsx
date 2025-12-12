import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Bell, Mail, MessageSquare, AlertTriangle, Save, TestTube } from 'lucide-react';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface NotificationChannel {
    id: string;
    name: string;
    enabled: boolean;
    config: Record<string, string>;
}

interface AlertConfig {
    id: string;
    name: string;
    description: string;
    enabled: boolean;
    channels: string[];
}

const defaultChannels: NotificationChannel[] = [
    {
        id: 'email',
        name: 'Email (Mailjet)',
        enabled: true,
        config: {
            api_key: '••••••••••••••••',
            api_secret: '••••••••••••••••',
            sender_email: 'noreply@ecommerce-dss.com',
            sender_name: 'E-Commerce DSS',
        },
    },
    {
        id: 'slack',
        name: 'Slack Webhook',
        enabled: false,
        config: {
            webhook_url: '',
            channel: '#alerts',
        },
    },
];

const defaultAlerts: AlertConfig[] = [
    {
        id: 'pipeline_failure',
        name: 'Pipeline Failure',
        description: 'Notify when ETL/ML pipeline fails',
        enabled: true,
        channels: ['email'],
    },
    {
        id: 'new_user',
        name: 'New User Registration',
        description: 'Notify when a new user registers',
        enabled: true,
        channels: ['email'],
    },
    {
        id: 'dss_approval',
        name: 'DSS Decision Pending Approval',
        description: 'Notify when a DSS Decision needs Admin approval',
        enabled: true,
        channels: ['email'],
    },
    {
        id: 'security_alert',
        name: 'Security Alert',
        description: 'Notify about unusual login, brute force attempts, etc.',
        enabled: true,
        channels: ['email', 'slack'],
    },
    {
        id: 'data_quality',
        name: 'Data Quality Alert',
        description: 'Notify when data quality issues are detected',
        enabled: false,
        channels: ['email'],
    },
    {
        id: 'ai_rate_limit',
        name: 'AI Rate Limit',
        description: 'Notify when approaching AI API limit (Gemini)',
        enabled: true,
        channels: ['email'],
    },
];

export default function NotificationSettingsPage() {
    const [channels, setChannels] = useState<NotificationChannel[]>(defaultChannels);
    const [alerts, setAlerts] = useState<AlertConfig[]>(defaultAlerts);
    const [saving, setSaving] = useState(false);
    const [testingEmail, setTestingEmail] = useState(false);

    const toggleChannel = (id: string) => {
        setChannels(prev =>
            prev.map(c => (c.id === id ? { ...c, enabled: !c.enabled } : c))
        );
    };

    const toggleAlert = (id: string) => {
        setAlerts(prev =>
            prev.map(a => (a.id === id ? { ...a, enabled: !a.enabled } : a))
        );
    };

    const handleSave = async () => {
        setSaving(true);
        await new Promise(resolve => setTimeout(resolve, 1000));
        setSaving(false);
        alert('Notification settings saved!');
    };

    const handleTestEmail = async () => {
        setTestingEmail(true);
        await new Promise(resolve => setTimeout(resolve, 2000));
        setTestingEmail(false);
        alert('Test email sent successfully!');
    };

    return (
        <div>
            <PageMeta title="Notification Settings" description="Configure email and system notifications" />
            <PageBreadCrumb pageTitle="Notification Settings" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-orange-100 rounded-lg dark:bg-orange-950">
                            <Bell className="w-6 h-6 text-orange-600" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                Notification Settings
                            </h1>
                            <p className="text-sm text-gray-500">
                                Configure Mailjet, email templates, alert frequency
                            </p>
                        </div>
                    </div>

                    <button
                        onClick={handleSave}
                        disabled={saving}
                        className="flex items-center gap-2 px-4 py-2 text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50"
                    >
                        <Save className="w-4 h-4" />
                        {saving ? 'Saving...' : 'Save Changes'}
                    </button>
                </div>

                <div className="grid gap-6">
                    {/* Notification Channels */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Mail className="w-5 h-5 text-blue-500" />
                                Notification Channels
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-6">
                            {channels.map(channel => (
                                <div key={channel.id} className="p-4 border border-gray-200 rounded-lg dark:border-gray-700">
                                    <div className="flex items-center justify-between mb-4">
                                        <div className="flex items-center gap-3">
                                            {channel.id === 'email' ? (
                                                <Mail className="w-5 h-5 text-gray-600" />
                                            ) : (
                                                <MessageSquare className="w-5 h-5 text-gray-600" />
                                            )}
                                            <span className="font-medium text-gray-900 dark:text-white">
                                                {channel.name}
                                            </span>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            {channel.id === 'email' && channel.enabled && (
                                                <button
                                                    onClick={handleTestEmail}
                                                    disabled={testingEmail}
                                                    className="flex items-center gap-1 px-3 py-1 text-sm text-blue-600 border border-blue-600 rounded hover:bg-blue-50 disabled:opacity-50"
                                                >
                                                    <TestTube className="w-4 h-4" />
                                                    {testingEmail ? 'Sending...' : 'Test Email'}
                                                </button>
                                            )}
                                            <button
                                                onClick={() => toggleChannel(channel.id)}
                                                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${channel.enabled ? 'bg-green-500' : 'bg-gray-300 dark:bg-gray-600'
                                                    }`}
                                            >
                                                <span
                                                    className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${channel.enabled ? 'translate-x-6' : 'translate-x-1'
                                                        }`}
                                                />
                                            </button>
                                        </div>
                                    </div>

                                    {channel.enabled && (
                                        <div className="grid gap-3 pt-3 border-t border-gray-100 dark:border-gray-800">
                                            {Object.entries(channel.config).map(([key, value]) => (
                                                <div key={key} className="flex items-center gap-4">
                                                    <label className="w-32 text-sm text-gray-600 capitalize">
                                                        {key.replace(/_/g, ' ')}:
                                                    </label>
                                                    <input
                                                        type={key.includes('secret') || key.includes('key') ? 'password' : 'text'}
                                                        value={value}
                                                        className="flex-1 px-3 py-1 text-sm border border-gray-300 rounded dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                                                        readOnly
                                                    />
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </CardContent>
                    </Card>

                    {/* Alert Types */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <AlertTriangle className="w-5 h-5 text-orange-500" />
                                Alert Types
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-4">
                                {alerts.map(alert => (
                                    <div key={alert.id} className="flex items-center justify-between py-3 border-b border-gray-100 dark:border-gray-800 last:border-0">
                                        <div className="flex-1">
                                            <div className="flex items-center gap-2">
                                                <span className="font-medium text-gray-900 dark:text-white">
                                                    {alert.name}
                                                </span>
                                                {alert.channels.map(ch => (
                                                    <span key={ch} className="px-2 py-0.5 text-xs bg-gray-100 rounded dark:bg-gray-800 text-gray-600 dark:text-gray-400">
                                                        {ch}
                                                    </span>
                                                ))}
                                            </div>
                                            <p className="text-sm text-gray-500">{alert.description}</p>
                                        </div>
                                        <button
                                            onClick={() => toggleAlert(alert.id)}
                                            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${alert.enabled ? 'bg-blue-600' : 'bg-gray-300 dark:bg-gray-600'
                                                }`}
                                        >
                                            <span
                                                className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${alert.enabled ? 'translate-x-6' : 'translate-x-1'
                                                    }`}
                                            />
                                        </button>
                                    </div>
                                ))}
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
