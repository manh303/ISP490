import { RefreshCw, Settings } from 'lucide-react';
import { KpiCard } from './components/kpi-card';
import { EtlJobCard } from './components/etl-job-card';
import { DataQuality } from './components/data-quality';
import { PerformanceChart } from './components/performance-chart';
import { TableHealth } from './components/table-health';

export default function App() {
  const handleRefresh = () => {
    console.log('Refreshing dashboard...');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-4 mb-6">
          <div className="flex items-center justify-between">
            <h1 className="flex items-center gap-2">
              📊 Data Engineer Dashboard
            </h1>
            <div className="flex items-center gap-3">
              <button
                onClick={handleRefresh}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
              >
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
              <button className="p-2 hover:bg-slate-100 rounded-md transition-colors">
                <Settings className="w-5 h-5 text-slate-600" />
              </button>
            </div>
          </div>
        </div>

        {/* KPI Cards */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
          <KpiCard icon="📦" label="Jobs" value="4" />
          <KpiCard icon="⏱️" label="Avg Duration" value="14.2min" />
          <KpiCard icon="✅" label="Success Rate" value="95.8%" />
          <KpiCard icon="⚠️" label="Alerts" value="4" variant="warning" />
          <KpiCard icon="📊" label="Tables" value="23" />
          <KpiCard icon="💾" label="Data Size" value="2.5GB" />
        </div>

        {/* Running Jobs Badge */}
        <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-6">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
            <span className="text-green-800">🔄 0 Jobs Running</span>
          </div>
        </div>

        {/* ETL Jobs */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <EtlJobCard
            name="DWH Pipeline"
            status="SUCCESS"
            successRate={95.8}
          />
          <EtlJobCard
            name="ML Training"
            status="SUCCESS"
            successRate={88.9}
          />
          <EtlJobCard
            name="Crawlers"
            status="DEGRADED"
            successRate={85.0}
          />
        </div>

        {/* Data Quality & Performance Chart */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          <div className="lg:col-span-1">
            <DataQuality />
          </div>
          <div className="lg:col-span-2">
            <PerformanceChart />
          </div>
        </div>

        {/* Table Health */}
        <TableHealth />
      </div>
    </div>
  );
}
