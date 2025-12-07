import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import {
  Play,
  Pause,
  RotateCcw,
  FileText,
  Clock,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Eye,
  Download
} from 'lucide-react';
import {
  getETLJobs,
  getETLRunHistory,
  getETLRunLogs,
  ETLJob,
  ETLRun
} from '../../services/dataEngineerApi';

const DataPipeline: React.FC = () => {
  const { jobCode } = useParams<{ jobCode?: string }>();
  const [jobs, setJobs] = useState<ETLJob[]>([]);
  const [selectedJob, setSelectedJob] = useState<ETLJob | null>(null);
  const [runHistory, setRunHistory] = useState<ETLRun[]>([]);
  const [selectedRun, setSelectedRun] = useState<ETLRun | null>(null);
  const [runLogs, setRunLogs] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [logsLoading, setLogsLoading] = useState(false);

  useEffect(() => {
    fetchJobs();
  }, []);

  useEffect(() => {
    if (jobCode) {
      const job = jobs.find(j => j.job_code === jobCode);
      if (job) {
        setSelectedJob(job);
        fetchRunHistory(job.job_code);
      }
    }
  }, [jobCode, jobs]);

  const fetchJobs = async () => {
    try {
      const data = await getETLJobs();
      setJobs(data);
      if (!selectedJob && data.length > 0) {
        setSelectedJob(data[0]);
        fetchRunHistory(data[0].job_code);
      }
    } catch (error) {
      console.error('Error fetching ETL jobs:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchRunHistory = async (jobCode: string) => {
    try {
      const data = await getETLRunHistory(jobCode, 20);
      setRunHistory(data);
    } catch (error) {
      console.error('Error fetching run history:', error);
    }
  };

  const fetchRunLogs = async (runId: number) => {
    setLogsLoading(true);
    try {
      const logs: any = await getETLRunLogs(runId);
      // Assuming logs might be an array of objects or a string; format accordingly
      let formattedLogs: string;
      if (typeof logs === 'string') {
        formattedLogs = logs;
      } else if (Array.isArray(logs)) {
        formattedLogs = logs.map((log: any) => 
          `[${log.created_at}] ${log.log_level}: ${log.log_message}${log.error_message ? ` - Error: ${log.error_message}` : ''}`
        ).join('\n');
      } else {
        formattedLogs = JSON.stringify(logs, null, 2);
      }
      setRunLogs(formattedLogs);
    } catch (error) {
      console.error('Error fetching run logs:', error);
      setRunLogs('Error loading logs');
    } finally {
      setLogsLoading(false);
    }
  };

  const handleJobSelect = (job: ETLJob) => {
    setSelectedJob(job);
    fetchRunHistory(job.job_code);
    setSelectedRun(null);
    setRunLogs('');
  };

  const handleRunSelect = (run: ETLRun) => {
    setSelectedRun(run);
    fetchRunLogs(run.run_id);
  };

  const runJob = async () => {
    if (!selectedJob) return;
    alert(`Run Job "${selectedJob.job_name}" - Feature under development, API not ready.`);
    // TODO: Call API to run job
  };

  const pauseJob = async () => {
    if (!selectedJob) return;
    alert(`Pause Job "${selectedJob.job_name}" - Feature under development, API not ready.`);
    // TODO: Call API to pause job
  };

  const restartJob = async () => {
    if (!selectedJob) return;
    alert(`Restart Job "${selectedJob.job_name}" - Feature under development, API not ready.`);
    // TODO: Call API to restart job
  };

  const getStatusIcon = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'success':
        return <CheckCircle className="w-4 h-4 text-green-600" />;
      case 'failed':
      case 'error':
        return <XCircle className="w-4 h-4 text-red-600" />;
      case 'running':
        return <div className="w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full animate-spin" />;
      default:
        return <Clock className="w-4 h-4 text-gray-600" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'success':
        return 'text-green-600 bg-green-100';
      case 'failed':
      case 'error':
        return 'text-red-600 bg-red-100';
      case 'running':
        return 'text-blue-600 bg-blue-100';
      default:
        return 'text-gray-600 bg-gray-100';
    }
  };

  const downloadLogs = () => {
    if (!selectedRun) return;
    if (!runLogs || runLogs.trim() === '' || runLogs === 'Error loading logs') {
      alert('No logs available to download');
      return;
    }
    const blob = new Blob([runLogs], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `logs_run_${selectedRun.run_id}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Data Pipeline Management
        </h1>
        <p className="text-gray-600 dark:text-gray-300">
          Monitor and manage ETL jobs and data pipelines
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* ETL Jobs List */}
        <div className="lg:col-span-1">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
            <div className="p-4 border-b">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                ETL Jobs
              </h2>
            </div>
            <div className="max-h-96 overflow-y-auto">
              {jobs.map((job) => (
                <div
                  key={job.job_code}
                  onClick={() => handleJobSelect(job)}
                  className={`p-4 border-b cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 ${
                    selectedJob?.job_code === job.job_code ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-200' : ''
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="font-medium text-gray-900 dark:text-white truncate">
                      {job.job_name}
                    </h3>
                    <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(job.last_run_status)}`}>
                      {job.last_run_status}
                    </span>
                  </div>
                  <p className="text-sm text-gray-500 mb-1">{job.job_code}</p>
                  <div className="flex justify-between text-xs text-gray-500">
                    <span>Success: {job.success_rate.toFixed(1)}%</span>
                    <span>Last Run: {job.last_run_date}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Run History */}
        <div className="lg:col-span-1">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
            <div className="p-4 border-b">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                Run History
              </h2>
              {selectedJob && (
                <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                  {selectedJob.job_name}
                </p>
              )}
            </div>
            <div className="max-h-96 overflow-y-auto">
              {runHistory.length === 0 ? (
                <div className="p-4 text-center text-gray-500">
                  No runs found
                </div>
              ) : (
                runHistory.map((run) => (
                  <div
                    key={run.run_id}
                    onClick={() => handleRunSelect(run)}
                    className={`p-4 border-b cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 ${
                      selectedRun?.run_id === run.run_id ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-200' : ''
                    }`}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        {getStatusIcon(run.status)}
                        <span className="font-medium text-gray-900 dark:text-white">
                          Run #{run.run_id}
                        </span>
                      </div>
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(run.status)}`}>
                        {run.status}
                      </span>
                    </div>
                    <div className="text-xs text-gray-500 space-y-1">
                      <div>Started: {new Date(run.started_at).toLocaleString()}</div>
                      {run.finished_at && (
                        <div>Finished: {new Date(run.finished_at).toLocaleString()}</div>
                      )}
                      <div>Duration: {run.duration_minutes?.toFixed(1) || 'N/A'} minutes</div>
                      <div>Rows: {run.rows_read?.toLocaleString()} read, {run.rows_written?.toLocaleString()} written</div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Run Details & Logs */}
        <div className="lg:col-span-1">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
            <div className="p-4 border-b">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                Run Details & Logs
              </h2>
            </div>
            <div className="max-h-96 overflow-y-auto">
              {selectedRun ? (
                <div className="p-4">
                  <div className="mb-4">
                    <h3 className="font-medium text-gray-900 dark:text-white mb-2">
                      Run #{selectedRun.run_id}
                    </h3>
                    <div className="space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-gray-500">Status:</span>
                        <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(selectedRun.status)}`}>
                          {selectedRun.status}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-500">Started:</span>
                        <span>{new Date(selectedRun.started_at).toLocaleString()}</span>
                      </div>
                      {selectedRun.finished_at && (
                        <div className="flex justify-between">
                          <span className="text-gray-500">Finished:</span>
                          <span>{new Date(selectedRun.finished_at).toLocaleString()}</span>
                        </div>
                      )}
                      <div className="flex justify-between">
                        <span className="text-gray-500">Duration:</span>
                        <span>{selectedRun.duration_minutes?.toFixed(1) || 'N/A'} minutes</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-500">Rows Read:</span>
                        <span>{selectedRun.rows_read?.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-500">Rows Written:</span>
                        <span>{selectedRun.rows_written?.toLocaleString()}</span>
                      </div>
                      {selectedRun.error_message && (
                        <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-red-700 dark:text-red-300 text-xs">
                          <strong>Error:</strong> {selectedRun.error_message}
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="border-t pt-4">
                    <div className="flex items-center justify-between mb-2">
                      <h4 className="font-medium text-gray-900 dark:text-white">Logs</h4>
                      <button onClick={downloadLogs} disabled={logsLoading} className="text-blue-600 hover:text-blue-800 text-sm disabled:opacity-50 disabled:cursor-not-allowed">
                        <Download className="w-4 h-4 inline mr-1" />
                        Download
                      </button>
                    </div>
                    {logsLoading ? (
                      <div className="flex items-center justify-center py-4">
                        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
                      </div>
                    ) : (
                      <pre className="text-xs bg-gray-50 dark:bg-gray-900 p-2 rounded max-h-64 overflow-y-auto whitespace-pre-wrap">
                        {runLogs || 'No logs available'}
                      </pre>
                    )}
                  </div>
                </div>
              ) : (
                <div className="p-4 text-center text-gray-500">
                  Select a run to view details
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Action Buttons */}
      {selectedJob && (
        <div className="mt-6 flex gap-4">
          <button
            onClick={runJob}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <Play className="w-4 h-4" />
            Run Job
          </button>
          <button
            onClick={pauseJob}
            className="flex items-center gap-2 bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <Pause className="w-4 h-4" />
            Pause Job
          </button>
          <button
            onClick={restartJob}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <RotateCcw className="w-4 h-4" />
            Restart Job
          </button>
        </div>
      )}
    </div>
  );
};

export default DataPipeline;