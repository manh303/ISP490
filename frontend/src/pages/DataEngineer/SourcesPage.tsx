import React, { useState, useEffect } from 'react';
import { getAllSourceSystems, getSourceSystemDetails, SourceSystem, SourceSystemDetail } from '../../services/businessMetadataApi';

const SourcesPage: React.FC = () => {
  const [sources, setSources] = useState<SourceSystem[]>([]);
  const [selectedSource, setSelectedSource] = useState<SourceSystemDetail | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadSources();
  }, []);

  const loadSources = async () => {
    setLoading(true);
    try {
      const data = await getAllSourceSystems();
      setSources(data);
    } catch (error) {
      console.error('Error loading sources:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadSourceDetails = async (code: string) => {
    setLoading(true);
    try {
      const data = await getSourceSystemDetails(code);
      setSelectedSource(data);
    } catch (error) {
      console.error('Error loading source details:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Data Sources & Systems</h1>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Sources List */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Data Sources List</h2>
          {loading && <div>Loading...</div>}
          <div className="space-y-2">
            {sources.map((source) => (
              <div
                key={source.source_id}
                className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                onClick={() => loadSourceDetails(source.code)}
              >
                <div className="font-medium">{source.name}</div>
                <div className="text-sm text-gray-600">Code: {source.code}</div>
                <div className="text-sm text-gray-600">Datasets: {source.dataset_count}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Source Details */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Data Source Details</h2>
          {selectedSource ? (
            <div>
              <h3 className="font-medium text-lg">{selectedSource.name}</h3>
              <p className="text-gray-600 mb-4">Code: {selectedSource.code}</p>
              <p className="text-gray-600 mb-4">Owner: {selectedSource.owner_contact}</p>
              <h4 className="font-medium mb-2">Datasets ({selectedSource.datasets.length})</h4>
              <div className="space-y-1 max-h-96 overflow-y-auto">
                {selectedSource.datasets.map((dataset) => (
                  <div key={dataset.dataset_id} className="text-sm p-2 bg-gray-50 rounded">
                    {dataset.schema_name}.{dataset.table_name} ({dataset.dataset_type})
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="text-gray-500">Select a source to view details</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default SourcesPage;