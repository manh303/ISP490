import React, { useState, useEffect } from 'react';
import { getAllDatasets, getDatasetDetails, searchDataCatalog, getAllSchemas, getTablesInSchema, DatasetDetail, Schema, TableInSchema } from '../../services/businessMetadataApi';

const CatalogPage: React.FC = () => {
  const [datasets, setDatasets] = useState<DatasetDetail[]>([]);
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [selectedDataset, setSelectedDataset] = useState<DatasetDetail | null>(null);
  const [selectedSchemaTables, setSelectedSchemaTables] = useState<TableInSchema[]>([]);
  const [selectedSchemaName, setSelectedSchemaName] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadSchemas();
    loadDatasets();
  }, []);

  const loadDatasets = async (params?: any) => {
    setLoading(true);
    try {
      const data = await getAllDatasets(params);
      setDatasets(data);
    } catch (error) {
      console.error('Error loading datasets:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadSchemas = async () => {
    try {
      const data = await getAllSchemas();
      setSchemas(data);
    } catch (error) {
      console.error('Error loading schemas:', error);
    }
  };

  const loadDatasetDetails = async (dataset_id: number) => {
    setLoading(true);
    try {
      const data = await getDatasetDetails(dataset_id);
      setSelectedDataset(data);
    } catch (error) {
      console.error('Error loading dataset details:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadTablesInSchema = async (schema_name: string) => {
    setLoading(true);
    setSelectedSchemaName(schema_name);
    try {
      const data = await getTablesInSchema(schema_name);
      setSelectedSchemaTables(data);
    } catch (error) {
      console.error('Error loading tables in schema:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = async () => {
    if (searchQuery.trim()) {
      setLoading(true);
      try {
        const data = await searchDataCatalog(searchQuery);
        setDatasets(data);
      } catch (error) {
        console.error('Error searching catalog:', error);
      } finally {
        setLoading(false);
      }
    } else {
      loadDatasets();
    }
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Danh mục dữ liệu</h1>

      {/* Search */}
      <div className="mb-6">
        <div className="flex gap-2">
          <input
            type="text"
            placeholder="Tìm kiếm datasets..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="flex-1 p-2 border rounded"
          />
          <button onClick={handleSearch} className="px-4 py-2 bg-blue-500 text-white rounded">
            Tìm kiếm
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Schemas */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Schemas</h2>
          <div className="space-y-2">
            {schemas.map((schema) => (
              <div
                key={schema.schema_name}
                className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                onClick={() => loadTablesInSchema(schema.schema_name)}
              >
                <div className="font-medium">{schema.schema_name}</div>
                <div className="text-sm text-gray-600">Tables: {schema.table_count}</div>
                <div className="text-sm text-gray-600">Rows: {schema.total_rows || 'N/A'}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Datasets or Tables in Schema */}
        <div className="bg-white p-4 rounded-lg shadow">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-lg font-semibold">
              {selectedSchemaTables.length > 0 ? `Tables in ${selectedSchemaName}` : 'Datasets'}
            </h2>
            {selectedSchemaTables.length > 0 && (
              <button
                onClick={() => {
                  setSelectedSchemaTables([]);
                  setSelectedSchemaName('');
                }}
                className="px-3 py-1 bg-gray-500 text-white rounded text-sm"
              >
                Back to Datasets
              </button>
            )}
          </div>
          {loading && <div>Loading...</div>}
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {selectedSchemaTables.length > 0
              ? selectedSchemaTables.map((item) => (
                  <div
                    key={item.dataset_id}
                    className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                    onClick={() => loadDatasetDetails(item.dataset_id)}
                  >
                    <div className="font-medium">{item.table_name}</div>
                    <div className="text-sm text-gray-600">Schema: {selectedSchemaName}</div>
                    <div className="text-sm text-gray-600">Type: {item.dataset_type}</div>
                  </div>
                ))
              : datasets.map((item) => (
                  <div
                    key={item.dataset_id}
                    className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                    onClick={() => loadDatasetDetails(item.dataset_id)}
                  >
                    <div className="font-medium">{item.table_name}</div>
                    <div className="text-sm text-gray-600">Schema: {item.schema_name}</div>
                    <div className="text-sm text-gray-600">Type: {item.dataset_type}</div>
                  </div>
                ))}
          </div>
        </div>

        {/* Dataset Details */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Chi tiết Dataset</h2>
          {selectedDataset ? (
            <div className="space-y-2">
              <div><strong>Name:</strong> {selectedDataset.table_name}</div>
              <div><strong>Schema:</strong> {selectedDataset.schema_name}</div>
              <div><strong>Source:</strong> {selectedDataset.source_name}</div>
              <div><strong>Type:</strong> {selectedDataset.dataset_type}</div>
              <div><strong>Layer:</strong> {selectedDataset.layer}</div>
              <div><strong>Rows:</strong> {selectedDataset.row_count || 'N/A'}</div>
              <div><strong>Size:</strong> {selectedDataset.size_mb ? `${selectedDataset.size_mb} MB` : 'N/A'}</div>
              <div><strong>Expectations:</strong> {selectedDataset.expectations_count}</div>
              <div><strong>Quality Issues:</strong> {selectedDataset.quality_issues_count}</div>
            </div>
          ) : (
            <p className="text-gray-500">Chọn một dataset để xem chi tiết</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default CatalogPage;