import React, { useState, useEffect } from 'react';
import {
  Play,
  Pause,
  RefreshCw,
  Activity,
  Clock,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Eye
} from 'lucide-react';

const PipelineMonitor = () => {
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [recentRuns, setRecentRuns] = useState([]);
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Fetch pipeline status
  const fetchPipelineStatus = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/v1/pipeline/status');
      const data = await response.json();
      setPipelineStatus(data);
    } catch (err) {
      setError('Failed to fetch pipeline status');
    } finally {
      setLoading(false);
    }
  };

  // Fetch recent runs
  const fetchRecentRuns = async () => {
    try {
      const response = await fetch('/api/v1/pipeline/runs?limit=10');
      const data = await response.json();
      setRecentRuns(data.runs || []);
    } catch (err) {
      console.error('Failed to fetch recent runs:', err);
    }
  };

  // Fetch metrics
  const fetchMetrics = async () => {
    try {
      const response = await fetch('/api/v1/pipeline/metrics');
      const data = await response.json();
      setMetrics(data);
    } catch (err) {
      console.error('Failed to fetch metrics:', err);
    }
  };

  // Trigger pipeline
  const triggerPipeline = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/v1/pipeline/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ force_refresh: true })
      });

      if (response.ok) {
        alert('Pipeline triggered successfully!');
        fetchRecentRuns();
      } else {
        throw new Error('Failed to trigger pipeline');
      }
    } catch (err) {
      alert('Error triggering pipeline: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  // Pause/Resume pipeline
  const togglePipeline = async (action) => {
    try {
      setLoading(true);
      const response = await fetch(`/api/v1/pipeline/${action}`, {
        method: 'POST'
      });

      if (response.ok) {
        alert(`Pipeline ${action}d successfully!`);
        fetchPipelineStatus();
      }
    } catch (err) {
      alert(`Error ${action}ing pipeline: ` + err.message);
    } finally {
      setLoading(false);
    }
  };

  // Get status color
  const getStatusColor = (state) => {
    switch (state) {
      case 'success': return 'text-green-600';
      case 'failed': return 'text-red-600';
      case 'running': return 'text-blue-600';
      default: return 'text-gray-600';
    }
  };

  // Get status icon
  const getStatusIcon = (state) => {
    switch (state) {
      case 'success': return <CheckCircle className="w-5 h-5" />;
      case 'failed': return <XCircle className="w-5 h-5" />;
      case 'running': return <Activity className="w-5 h-5 animate-pulse" />;
      default: return <Clock className="w-5 h-5" />;
    }
  };

  // Format duration
  const formatDuration = (seconds) => {
    if (!seconds) return 'N/A';
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.floor(seconds % 60);
    return `${minutes}m ${remainingSeconds}s`;
  };

  useEffect(() => {
    fetchPipelineStatus();
    fetchRecentRuns();
    fetchMetrics();

    // Refresh data every 30 seconds
    const interval = setInterval(() => {
      fetchPipelineStatus();
      fetchRecentRuns();
      fetchMetrics();
    }, 30000);

    return () => clearInterval(interval);
  }, []);

  if (loading && !pipelineStatus) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-8 h-8 animate-spin text-blue-500" />
        <span className="ml-2 text-gray-600">Loading pipeline status...</span>
      </div>
    );
  }

  return (
    <div className="p-6 bg-white rounded-lg shadow-lg">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-gray-800">Data Pipeline Monitor</h2>
        <button
          onClick={fetchPipelineStatus}
          className="flex items-center px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
          disabled={loading}
        >
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {error && (
        <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
          <div className="flex items-center">
            <AlertTriangle className="w-5 h-5 text-red-500 mr-2" />
            <span className="text-red-700">{error}</span>
          </div>
        </div>
      )}

      {/* Pipeline Controls */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <button
          onClick={triggerPipeline}
          className="flex items-center justify-center px-4 py-3 bg-green-500 text-white rounded-lg hover:bg-green-600 transition-colors"
          disabled={loading}
        >
          <Play className="w-4 h-4 mr-2" />
          Trigger Run
        </button>

        <button
          onClick={() => togglePipeline('pause')}
          className="flex items-center justify-center px-4 py-3 bg-yellow-500 text-white rounded-lg hover:bg-yellow-600 transition-colors"
          disabled={loading}
        >
          <Pause className="w-4 h-4 mr-2" />
          Pause
        </button>

        <button
          onClick={() => togglePipeline('resume')}
          className="flex items-center justify-center px-4 py-3 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
          disabled={loading}
        >
          <Play className="w-4 h-4 mr-2" />
          Resume
        </button>

        <a
          href={pipelineStatus?.airflow_url || '#'}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center justify-center px-4 py-3 bg-gray-500 text-white rounded-lg hover:bg-gray-600 transition-colors"
        >
          <Eye className="w-4 h-4 mr-2" />
          View Airflow
        </a>
      </div>

      {/* Pipeline Status */}
      {pipelineStatus && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
          <div className="bg-gray-50 p-4 rounded-lg">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">Pipeline Status</h3>
            <div className="flex items-center">
              <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${
                pipelineStatus.status === 'running'
                  ? 'bg-green-100 text-green-800'
                  : 'bg-red-100 text-red-800'
              }`}>
                {pipelineStatus.status === 'running' ? 'Active' : 'Paused'}
              </span>
            </div>
            <p className="text-sm text-gray-600 mt-2">
              Last Updated: {new Date(pipelineStatus.last_update).toLocaleString()}
            </p>
          </div>

          {metrics && (
            <>
              <div className="bg-gray-50 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-700 mb-2">Success Rate</h3>
                <div className="text-3xl font-bold text-green-600">
                  {metrics.success_rate}%
                </div>
                <p className="text-sm text-gray-600">
                  {metrics.successful_runs}/{metrics.total_runs} successful runs
                </p>
              </div>

              <div className="bg-gray-50 p-4 rounded-lg">
                <h3 className="text-lg font-semibold text-gray-700 mb-2">Avg Duration</h3>
                <div className="text-3xl font-bold text-blue-600">
                  {formatDuration(metrics.average_duration_seconds)}
                </div>
                <p className="text-sm text-gray-600">
                  Pipeline health: {metrics.pipeline_health}
                </p>
              </div>
            </>
          )}
        </div>
      )}

      {/* Recent Runs */}
      <div className="bg-gray-50 p-4 rounded-lg">
        <h3 className="text-lg font-semibold text-gray-700 mb-4">Recent Runs</h3>

        {recentRuns.length === 0 ? (
          <p className="text-gray-600">No recent runs found</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full table-auto">
              <thead>
                <tr className="bg-gray-100">
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-700">Status</th>
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-700">Run ID</th>
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-700">Start Time</th>
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-700">Duration</th>
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-700">Trigger</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {recentRuns.map((run, index) => (
                  <tr key={index} className="hover:bg-gray-50">
                    <td className="px-4 py-2">
                      <div className={`flex items-center ${getStatusColor(run.state)}`}>
                        {getStatusIcon(run.state)}
                        <span className="ml-2 capitalize text-sm">{run.state}</span>
                      </div>
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-900 font-mono">
                      {run.run_id.substring(0, 16)}...
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-600">
                      {new Date(run.start_date).toLocaleString()}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-600">
                      {run.end_date && run.start_date
                        ? formatDuration((new Date(run.end_date) - new Date(run.start_date)) / 1000)
                        : 'Running...'
                      }
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-600">
                      {run.external_trigger ? 'Manual' : 'Scheduled'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default PipelineMonitor;