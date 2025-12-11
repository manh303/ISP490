import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Settings, Globe, Palette, Flag, Save, RotateCcw } from 'lucide-react';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface SettingItem {
    id: string;
    label: string;
    description: string;
    type: 'text' | 'select' | 'toggle';
    value: string | boolean;
    options?: { value: string; label: string }[];
}

const defaultSettings: SettingItem[] = [
    {
        id: 'timezone',
        label: 'System Timezone',
        description: 'Timezone used for all reports and logs',
        type: 'select',
        value: 'Asia/Ho_Chi_Minh',
        options: [
            { value: 'Asia/Ho_Chi_Minh', label: 'Asia/Ho_Chi_Minh (UTC+7)' },
            { value: 'Asia/Bangkok', label: 'Asia/Bangkok (UTC+7)' },
            { value: 'UTC', label: 'UTC (UTC+0)' },
        ],
    },
    {
        id: 'brand_name',
        label: 'Brand Name',
        description: 'Name displayed in header and emails',
        type: 'text',
        value: 'E-Commerce DSS',
    },
    {
        id: 'language',
        label: 'Default Language',
        description: 'Default display language for new users',
        type: 'select',
        value: 'en',
        options: [
            { value: 'en', label: 'English' },
            { value: 'vi', label: 'Vietnamese' },
        ],
    },
    {
        id: 'dark_mode',
        label: 'Default Dark Mode',
        description: 'Enable dark mode as default for new users',
        type: 'toggle',
        value: false,
    },
    {
        id: 'maintenance_mode',
        label: 'Maintenance Mode',
        description: 'Put system in maintenance mode (Admin access only)',
        type: 'toggle',
        value: false,
    },
    {
        id: 'enable_ai',
        label: 'Enable AI Features',
        description: 'Allow AI features (Gemini) in DSS',
        type: 'toggle',
        value: true,
    },
    {
        id: 'enable_signup',
        label: 'Allow Registration',
        description: 'Allow new users to self-register accounts',
        type: 'toggle',
        value: false,
    },
];

export default function GeneralSettingsPage() {
    const [settings, setSettings] = useState<SettingItem[]>(defaultSettings);
    const [hasChanges, setHasChanges] = useState(false);
    const [saving, setSaving] = useState(false);

    const handleChange = (id: string, newValue: string | boolean) => {
        setSettings(prev =>
            prev.map(s => (s.id === id ? { ...s, value: newValue } : s))
        );
        setHasChanges(true);
    };

    const handleSave = async () => {
        setSaving(true);
        await new Promise(resolve => setTimeout(resolve, 1000));
        setSaving(false);
        setHasChanges(false);
        alert('Settings saved successfully!');
    };

    const handleReset = () => {
        setSettings(defaultSettings);
        setHasChanges(false);
    };

    return (
        <div>
            <PageMeta title="General Settings" description="System general configuration" />
            <PageBreadCrumb pageTitle="General Settings" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-blue-100 rounded-lg dark:bg-blue-950">
                            <Settings className="w-6 h-6 text-blue-600" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                General Settings
                            </h1>
                            <p className="text-sm text-gray-500">
                                Timezone, brand name, language, feature flags
                            </p>
                        </div>
                    </div>

                    {hasChanges && (
                        <div className="flex gap-3">
                            <button
                                onClick={handleReset}
                                className="flex items-center gap-2 px-4 py-2 text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300"
                            >
                                <RotateCcw className="w-4 h-4" />
                                Reset
                            </button>
                            <button
                                onClick={handleSave}
                                disabled={saving}
                                className="flex items-center gap-2 px-4 py-2 text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50"
                            >
                                <Save className="w-4 h-4" />
                                {saving ? 'Saving...' : 'Save Changes'}
                            </button>
                        </div>
                    )}
                </div>

                {/* Settings Grid */}
                <div className="grid gap-6">
                    {/* System Settings */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Globe className="w-5 h-5 text-blue-500" />
                                System Settings
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-6">
                            {settings.filter(s => ['timezone', 'brand_name', 'language'].includes(s.id)).map(setting => (
                                <div key={setting.id} className="flex items-center justify-between py-3 border-b border-gray-100 dark:border-gray-800 last:border-0">
                                    <div className="flex-1">
                                        <label className="font-medium text-gray-900 dark:text-white">
                                            {setting.label}
                                        </label>
                                        <p className="text-sm text-gray-500">{setting.description}</p>
                                    </div>
                                    <div className="w-64">
                                        {setting.type === 'text' && (
                                            <input
                                                type="text"
                                                value={setting.value as string}
                                                onChange={e => handleChange(setting.id, e.target.value)}
                                                className="w-full px-3 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                                            />
                                        )}
                                        {setting.type === 'select' && (
                                            <select
                                                value={setting.value as string}
                                                onChange={e => handleChange(setting.id, e.target.value)}
                                                className="w-full px-3 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                                            >
                                                {setting.options?.map(opt => (
                                                    <option key={opt.value} value={opt.value}>
                                                        {opt.label}
                                                    </option>
                                                ))}
                                            </select>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </CardContent>
                    </Card>

                    {/* Feature Flags */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Flag className="w-5 h-5 text-orange-500" />
                                Feature Flags
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            {settings.filter(s => s.type === 'toggle').map(setting => (
                                <div key={setting.id} className="flex items-center justify-between py-3 border-b border-gray-100 dark:border-gray-800 last:border-0">
                                    <div className="flex-1">
                                        <label className="font-medium text-gray-900 dark:text-white">
                                            {setting.label}
                                        </label>
                                        <p className="text-sm text-gray-500">{setting.description}</p>
                                    </div>
                                    <button
                                        onClick={() => handleChange(setting.id, !setting.value)}
                                        className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${setting.value ? 'bg-blue-600' : 'bg-gray-300 dark:bg-gray-600'
                                            }`}
                                    >
                                        <span
                                            className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${setting.value ? 'translate-x-6' : 'translate-x-1'
                                                }`}
                                        />
                                    </button>
                                </div>
                            ))}
                        </CardContent>
                    </Card>

                    {/* Appearance */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Palette className="w-5 h-5 text-purple-500" />
                                Appearance
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="flex items-center justify-between py-3">
                                <div>
                                    <label className="font-medium text-gray-900 dark:text-white">
                                        Custom Logo
                                    </label>
                                    <p className="text-sm text-gray-500">Upload a custom logo for the system</p>
                                </div>
                                <button className="px-4 py-2 text-blue-600 border border-blue-600 rounded-lg hover:bg-blue-50 dark:hover:bg-blue-950">
                                    Upload Logo
                                </button>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
