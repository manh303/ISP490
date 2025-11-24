import React, { useState } from 'react';
import { exportOverviewReport, exportProductsReport } from '../../services/reportApi';

const ReportsComponent: React.FC = () => {
  const [overviewParams, setOverviewParams] = useState({
    from_date: '',
    to_date: '',
    platform_code: '',
  });

  const [productsParams, setProductsParams] = useState({
    from_date: '',
    to_date: '',
    platform_code: '',
    metric: 'revenue' as 'revenue' | 'reviews' | 'rating' | 'price',
    limit: 100,
  });

  const downloadBlob = (blob: Blob, filename: string) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleExportOverview = async () => {
    try {
      const blob = await exportOverviewReport({
        from_date: overviewParams.from_date,
        to_date: overviewParams.to_date,
        platform_code: overviewParams.platform_code || undefined,
      });
      downloadBlob(blob, 'overview_report.csv');
    } catch (error) {
      console.error('Error exporting overview report:', error);
      alert('Failed to export overview report');
    }
  };

  const handleExportProducts = async () => {
    try {
      const blob = await exportProductsReport({
        from_date: productsParams.from_date,
        to_date: productsParams.to_date,
        platform_code: productsParams.platform_code || undefined,
        metric: productsParams.metric,
        limit: productsParams.limit,
      });
      downloadBlob(blob, 'products_report.csv');
    } catch (error) {
      console.error('Error exporting products report:', error);
      alert('Failed to export products report');
    }
  };

  return (
    <div className="reports-component">
      <h2>Reports</h2>

      <div className="report-section">
        <h3>Overview Report</h3>
        <div>
          <label>
            From Date:
            <input
              type="date"
              value={overviewParams.from_date}
              onChange={(e) => setOverviewParams({ ...overviewParams, from_date: e.target.value })}
            />
          </label>
          <label>
            To Date:
            <input
              type="date"
              value={overviewParams.to_date}
              onChange={(e) => setOverviewParams({ ...overviewParams, to_date: e.target.value })}
            />
          </label>
          <label>
            Platform Code:
            <select
              value={overviewParams.platform_code}
              onChange={(e) => setOverviewParams({ ...overviewParams, platform_code: e.target.value })}
            >
              <option value="">All</option>
              <option value="tiki">Tiki</option>
              <option value="lazada">Lazada</option>
            </select>
          </label>
          <button onClick={handleExportOverview}>Export Overview Report</button>
        </div>
      </div>

      <div className="report-section">
        <h3>Products Report</h3>
        <div>
          <label>
            From Date:
            <input
              type="date"
              value={productsParams.from_date}
              onChange={(e) => setProductsParams({ ...productsParams, from_date: e.target.value })}
            />
          </label>
          <label>
            To Date:
            <input
              type="date"
              value={productsParams.to_date}
              onChange={(e) => setProductsParams({ ...productsParams, to_date: e.target.value })}
            />
          </label>
          <label>
            Platform Code:
            <select
              value={productsParams.platform_code}
              onChange={(e) => setProductsParams({ ...productsParams, platform_code: e.target.value })}
            >
              <option value="">All</option>
              <option value="tiki">Tiki</option>
              <option value="lazada">Lazada</option>
            </select>
          </label>
          <label>
            Metric:
            <select
              value={productsParams.metric}
              onChange={(e) => setProductsParams({ ...productsParams, metric: e.target.value as any })}
            >
              <option value="revenue">Revenue</option>
              <option value="reviews">Reviews</option>
              <option value="rating">Rating</option>
              <option value="price">Price</option>
            </select>
          </label>
          <label>
            Limit:
            <input
              type="number"
              min="1"
              max="1000"
              value={productsParams.limit}
              onChange={(e) => setProductsParams({ ...productsParams, limit: parseInt(e.target.value) })}
            />
          </label>
          <button onClick={handleExportProducts}>Export Products Report</button>
        </div>
      </div>
    </div>
  );
};

export default ReportsComponent;