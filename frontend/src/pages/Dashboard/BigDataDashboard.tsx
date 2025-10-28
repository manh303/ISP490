import React from 'react';
import { FaUserCircle, FaMoon, FaHome, FaChartLine, FaUsers, FaCogs, FaBoxOpen, FaTable } from 'react-icons/fa';
import './BigDataDashboard.css';

// Dummy data
const kpis = [
  { label: 'Accuracy Model', value: '>75%', icon: <FaChartLine /> },
  { label: 'Records Processed', value: '4M+', icon: <FaTable /> },
  { label: 'Processing Time', value: '<10 mins', icon: <FaCogs /> },
  { label: 'Data Quality', value: '>95%', icon: <FaBoxOpen /> },
];

const transactions = [
  { order_id: 'ORD001', price: 120, quantity: 2, order_date: '2025-10-10' },
  { order_id: 'ORD002', price: 80, quantity: 1, order_date: '2025-10-11' },
  { order_id: 'ORD003', price: 200, quantity: 3, order_date: '2025-10-12' },
  { order_id: 'ORD004', price: 150, quantity: 2, order_date: '2025-10-12' },
];

export default function BigDataDashboard() {
  return (
    <div className="dss-dashboard-root">
      {/* Sidebar */}
      <aside className="dss-sidebar">
        <div className="dss-sidebar-title">DSS Big Data Ecommerce</div>
        <nav>
          <ul>
            <li><FaHome /> Home</li>
            <li><FaChartLine /> Sales Forecast</li>
            <li><FaUsers /> Customer Segmentation</li>
            <li><FaBoxOpen /> Product Recommendation</li>
            <li><FaTable /> Data Quality</li>
            <li><FaCogs /> Settings</li>
          </ul>
        </nav>
      </aside>
      {/* Main Content */}
      <div className="dss-main">
        {/* Top Navbar */}
        <header className="dss-navbar">
          <div className="dss-navbar-title">DSS Big Data Ecommerce</div>
          <div className="dss-navbar-actions">
            <button className="dss-darkmode-btn"><FaMoon /></button>
            <FaUserCircle className="dss-avatar" />
          </div>
        </header>
        {/* KPI Cards */}
        <section className="dss-kpi-grid">
          {kpis.map((kpi, idx) => (
            <div className="dss-kpi-card" key={idx}>
              <div className="dss-kpi-icon">{kpi.icon}</div>
              <div className="dss-kpi-value">{kpi.value}</div>
              <div className="dss-kpi-label">{kpi.label}</div>
            </div>
          ))}
        </section>
        {/* Charts */}
        <section className="dss-charts-grid">
          <div className="dss-chart-card">[Line Chart: Monthly Sales Forecast]</div>
          <div className="dss-chart-card">[Pie Chart: Customer Segments]</div>
          <div className="dss-chart-card">[Bar Chart: Top 5 Product Recommendations]</div>
        </section>
        {/* Latest Transactions Table */}
        <section className="dss-table-section">
          <h3>Latest Transactions</h3>
          <table className="dss-table">
            <thead>
              <tr>
                <th>Order ID</th>
                <th>Price</th>
                <th>Quantity</th>
                <th>Order Date</th>
              </tr>
            </thead>
            <tbody>
              {transactions.map((tx, idx) => (
                <tr key={idx}>
                  <td>{tx.order_id}</td>
                  <td>${tx.price}</td>
                  <td>{tx.quantity}</td>
                  <td>{tx.order_date}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}
