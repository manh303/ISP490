import React, { useState, useEffect } from 'react';
import { getAllExpectations, createExpectation, getExpectationResults, getAllJobs, getJobDetails, Expectation, ExpectationResult, Job, JobDetail } from '../../services/businessMetadataApi';

const ExpectationsJobsPage: React.FC = () => {
  const [expectations, setExpectations] = useState<Expectation[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [selectedExpectation, setSelectedExpectation] = useState<Expectation | null>(null);
  const [expectationResults, setExpectationResults] = useState<ExpectationResult[]>([]);
  const [selectedJob, setSelectedJob] = useState<JobDetail | null>(null);
  const [showCreateExpectationForm, setShowCreateExpectationForm] = useState(false);
  const [newExpectation, setNewExpectation] = useState({
    dataset_id: '',
    name: '',
    severity: 'error',
    check_sql: '',
    owner: '',
    tags: ''
  });
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadExpectations();
    loadJobs();
  }, []);

  const loadExpectations = async () => {
    setLoading(true);
    try {
      const data = await getAllExpectations();
      setExpectations(data);
    } catch (error) {
      console.error('Error loading expectations:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadJobs = async () => {
    try {
      const data = await getAllJobs();
      setJobs(data);
    } catch (error) {
      console.error('Error loading jobs:', error);
    }
  };

  const loadExpectationResults = async (exp_id: number) => {
    setLoading(true);
    try {
      const data = await getExpectationResults(exp_id);
      setExpectationResults(data);
    } catch (error) {
      console.error('Error loading expectation results:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadJobDetails = async (job_id: number) => {
    setLoading(true);
    try {
      const data = await getJobDetails(job_id);
      setSelectedJob(data);
    } catch (error) {
      console.error('Error loading job details:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateExpectation = async () => {
    try {
      await createExpectation({
        ...newExpectation,
        dataset_id: parseInt(newExpectation.dataset_id)
      });
      setNewExpectation({
        dataset_id: '',
        name: '',
        severity: 'error',
        check_sql: '',
        owner: '',
        tags: ''
      });
      setShowCreateExpectationForm(false);
      loadExpectations();
    } catch (error) {
      console.error('Error creating expectation:', error);
    }
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Expectations & Jobs</h1>

      {/* Create Expectation Button */}
      <div className="mb-6">
        <button
          onClick={() => setShowCreateExpectationForm(!showCreateExpectationForm)}
          className="px-4 py-2 bg-green-500 text-white rounded"
        >
          Create New Expectation
        </button>
      </div>

      {/* Create Expectation Form */}
      {showCreateExpectationForm && (
        <div className="mb-6 p-4 bg-gray-50 rounded-lg">
          <h3 className="font-medium mb-4">Create New Expectation</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <input
              type="number"
              placeholder="Dataset ID"
              value={newExpectation.dataset_id}
              onChange={(e) => setNewExpectation({ ...newExpectation, dataset_id: e.target.value })}
              className="p-2 border rounded"
            />
            <input
              type="text"
              placeholder="Name"
              value={newExpectation.name}
              onChange={(e) => setNewExpectation({ ...newExpectation, name: e.target.value })}
              className="p-2 border rounded"
            />
            <select
              value={newExpectation.severity}
              onChange={(e) => setNewExpectation({ ...newExpectation, severity: e.target.value })}
              className="p-2 border rounded"
            >
              <option value="error">Error</option>
              <option value="warning">Warning</option>
            </select>
            <input
              type="text"
              placeholder="Owner"
              value={newExpectation.owner}
              onChange={(e) => setNewExpectation({ ...newExpectation, owner: e.target.value })}
              className="p-2 border rounded"
            />
          </div>
          <textarea
            placeholder="Check SQL"
            value={newExpectation.check_sql}
            onChange={(e) => setNewExpectation({ ...newExpectation, check_sql: e.target.value })}
            className="w-full p-2 border rounded mt-4"
            rows={3}
          />
          <input
            type="text"
            placeholder="Tags"
            value={newExpectation.tags}
            onChange={(e) => setNewExpectation({ ...newExpectation, tags: e.target.value })}
            className="w-full p-2 border rounded mt-2"
          />
          <div className="mt-4 flex gap-2">
            <button onClick={handleCreateExpectation} className="px-4 py-2 bg-green-500 text-white rounded">
              Create
            </button>
            <button onClick={() => setShowCreateExpectationForm(false)} className="px-4 py-2 bg-gray-500 text-white rounded">
              Cancel
            </button>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Expectations */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Data Expectations</h2>
          {loading && <div>Loading...</div>}
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {expectations.map((exp) => (
              <div
                key={exp.exp_id}
                className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                onClick={() => {
                  setSelectedExpectation(exp);
                  loadExpectationResults(exp.exp_id);
                }}
              >
                <div className="font-medium">{exp.name}</div>
                <div className="text-sm text-gray-600">Table: {exp.schema_name}.{exp.table_name}</div>
                <div className="text-sm text-gray-600">Severity: {exp.severity}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Jobs */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Jobs</h2>
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {jobs.map((job) => (
              <div
                key={job.job_id}
                className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                onClick={() => loadJobDetails(job.job_id)}
              >
                <div className="font-medium">{job.job_name}</div>
                <div className="text-sm text-gray-600">Owner: {job.owner}</div>
                <div className="text-sm text-gray-600">Active: {job.active ? 'Yes' : 'No'}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Details Section */}
      <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Expectation Details */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Expectation Details</h2>
          {selectedExpectation ? (
            <div>
              <h3 className="font-medium text-lg">{selectedExpectation.name}</h3>
              <p className="text-gray-600 mb-2">Table: {selectedExpectation.schema_name}.{selectedExpectation.table_name}</p>
              <p className="text-gray-600 mb-2">Severity: {selectedExpectation.severity}</p>
              <p className="text-gray-600 mb-4">Owner: {selectedExpectation.owner || 'N/A'}</p>
              <div className="mb-4">
                <strong>Check SQL:</strong>
                <pre className="mt-1 p-2 bg-gray-50 rounded text-sm overflow-x-auto">
                  {selectedExpectation.check_sql}
                </pre>
              </div>
              <h4 className="font-medium mb-2">Results ({expectationResults.length})</h4>
              <div className="space-y-1 max-h-48 overflow-y-auto">
                {expectationResults.map((result) => (
                  <div key={result.check_id} className="text-sm p-2 bg-gray-50 rounded">
                    {result.check_date}: {result.passed ? 'Passed' : 'Failed'} ({result.failed_count}/{result.total_count})
                    {result.error_message && <span className="text-red-600"> - {result.error_message}</span>}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="text-gray-500">Select an expectation to view details</p>
          )}
        </div>

        {/* Job Details */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Job Details</h2>
          {selectedJob ? (
            <div>
              <h3 className="font-medium text-lg">{selectedJob.job_name}</h3>
              <p className="text-gray-600 mb-2">Owner: {selectedJob.owner}</p>
              <p className="text-gray-600 mb-2">Active: {selectedJob.active ? 'Yes' : 'No'}</p>
              <p className="text-gray-600 mb-4">Schedule: {selectedJob.schedule}</p>
            </div>
          ) : (
            <p className="text-gray-500">Select a job to view details</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default ExpectationsJobsPage;