import React, { useState, useEffect } from 'react';
import { GitBranch, ArrowRight, Database, RefreshCw, Network } from 'lucide-react';
import { getTableLineage } from '../../services/dataEngineerApi';

interface TableLineageItem {
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  transformation_type: string;
  job_code: string;
}

const TableLineagePage: React.FC = () => {
  const [lineageData, setLineageData] = useState<TableLineageItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [schemaName, setSchemaName] = useState('dwh');
  const [tableName, setTableName] = useState('');
  const [direction, setDirection] = useState<'upstream' | 'downstream' | 'both'>('both');

  const schemas = ['dwh', 'ml'];
  const tablesBySchema: { [key: string]: string[] } = {
    dwh: [
      'dim_brand',
      'dim_category',
      'dim_date',
      'dim_platform',
      'dim_product',
      'dim_reviewer',
      'fact_product_daily',
      'fact_product_daily_agg',
      'fact_review',
      'fact_review_daily',
      'fact_review_daily_agg',
      'fact_reviews_detail'
    ],
    ml: [
      'dim_ml_model',
      'fact_price_prediction',
      'fact_product_recommen',
      'fact_review_sentiment'
    ]
  };

  const availableTables = tablesBySchema[schemaName] || [];

  const fetchTableLineage = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getTableLineage(schemaName, tableName, direction);
      setLineageData(data);
    } catch (err) {
      console.error('Error fetching table lineage:', err);
      setError('Failed to load table lineage data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (schemaName && tableName) {
      fetchTableLineage();
    }
  }, [schemaName, tableName, direction]);

  const getTransformationColor = (type: string) => {
    switch (type.toLowerCase()) {
      case 'etl': return 'bg-blue-100 text-blue-800';
      case 'view': return 'bg-green-100 text-green-800';
      case 'materialized_view': return 'bg-purple-100 text-purple-800';
      case 'procedure': return 'bg-orange-100 text-orange-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const groupByTarget = (data: TableLineageItem[]) => {
    const grouped: { [key: string]: TableLineageItem[] } = {};
    data.forEach(item => {
      const key = `${item.target_schema}.${item.target_table}`;
      if (!grouped[key]) {
        grouped[key] = [];
      }
      grouped[key].push(item);
    });
    return grouped;
  };

  const groupedData = groupByTarget(lineageData);

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Table Lineage
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Visualize data flow and dependencies between tables
          </p>
        </div>
        <button
          onClick={fetchTableLineage}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Schema Name
            </label>
            <select
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              {schemas.map(schema => (
                <option key={schema} value={schema}>{schema}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Table Name
            </label>
            <input
              type="text"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              list="table-options"
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
              placeholder="Select or enter table name"
            />
            <datalist id="table-options">
              {availableTables.map(table => (
                <option key={table} value={table} />
              ))}
            </datalist>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Direction
            </label>
            <select
              value={direction}
              onChange={(e) => setDirection(e.target.value as 'upstream' | 'downstream' | 'both')}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="upstream">Upstream (Source)</option>
              <option value="downstream">Downstream (Target)</option>
              <option value="both">Both Directions</option>
            </select>
          </div>
          <div className="flex items-end">
            <button
              onClick={fetchTableLineage}
              className="w-full bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
            >
              Load Lineage
            </button>
          </div>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      )}

      {/* Loading State */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        </div>
      )}

      {/* Lineage Visualization */}
      {!loading && !error && Object.keys(groupedData).length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <Network className="w-5 h-5 mr-2" />
              Data Lineage Flow
            </h2>
            <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
              Showing lineage for table: <strong>{schemaName}.{tableName}</strong>
            </p>
          </div>
          <div className="p-6">
            <div className="space-y-6">
              {Object.entries(groupedData).map(([targetKey, items]) => (
                <div key={targetKey} className="border border-gray-200 dark:border-gray-600 rounded-lg p-4">
                  <div className="flex items-center mb-4">
                    <Database className="w-5 h-5 mr-2 text-blue-600" />
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                      {targetKey}
                    </h3>
                  </div>

                  <div className="space-y-3">
                    {items.map((item, index) => (
                      <div key={index} className="flex items-center bg-gray-50 dark:bg-gray-700 p-3 rounded">
                        <div className="flex items-center flex-1">
                          <span className="font-medium text-gray-900 dark:text-white">
                            {item.source_schema}.{item.source_table}
                          </span>
                          <ArrowRight className="w-4 h-4 mx-2 text-gray-400" />
                          <span className="font-medium text-gray-900 dark:text-white">
                            {item.target_schema}.{item.target_table}
                          </span>
                        </div>

                        <div className="flex items-center gap-2">
                          <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getTransformationColor(item.transformation_type)}`}>
                            {item.transformation_type}
                          </span>
                          <span className="text-sm text-gray-600 dark:text-gray-300">
                            {item.job_code}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Detailed Table */}
      {!loading && !error && lineageData.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <GitBranch className="w-5 h-5 mr-2" />
              Detailed Lineage Information
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Source Table</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Target Table</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Transformation</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Job ID</th>
                  </tr>
                </thead>
                <tbody>
                  {lineageData.map((item, index) => (
                    <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="py-3 px-4">
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {item.source_table}
                          </p>
                          <p className="text-sm text-gray-500">{item.source_schema}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {item.target_table}
                          </p>
                          <p className="text-sm text-gray-500">{item.target_schema}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getTransformationColor(item.transformation_type)}`}>
                          {item.transformation_type}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {item.job_code}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Empty State */}
      {!loading && !error && lineageData.length === 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border p-12">
          <div className="text-center">
            <GitBranch className="w-16 h-16 mx-auto mb-4 text-gray-400" />
            <h3 className="text-xl font-medium text-gray-900 dark:text-white mb-2">
              No Lineage Data Available
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              No lineage information found for the selected table.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default TableLineagePage;