import { useState } from 'react';
import { Sidebar } from './components/Sidebar';
import { Header } from './components/Header';
import { DashboardHome } from './components/DashboardHome';
import { ProductRecommendations } from './components/ProductRecommendations';
import { PricePredictions } from './components/PricePredictions';
import { DemandForecast } from './components/DemandForecast';
import { CustomerSegments } from './components/CustomerSegments';
import { Dimensions } from './components/Dimensions';
import { Facts } from './components/Facts';

export default function MLIPage() {
  const [currentPage, setCurrentPage] = useState('dashboard');
  const [darkMode, setDarkMode] = useState(false);

  const renderPage = () => {
    switch (currentPage) {
      case 'dashboard':
        return <DashboardHome />;
      case 'product-recommendation':
        return <ProductRecommendations />;
      case 'price-prediction':
        return <PricePredictions />;
      case 'demand-forecast':
        return <DemandForecast />;
      case 'customer-segments':
        return <CustomerSegments />;
      case 'dimensions':
        return <Dimensions />;
      case 'facts':
        return <Facts />;
      default:
        return <DashboardHome />;
    }
  };

  const getPageTitle = () => {
    switch (currentPage) {
      case 'dashboard':
        return 'ML Insights Dashboard';
      case 'product-recommendation':
        return 'Product Recommendations';
      case 'price-prediction':
        return 'Price Predictions';
      case 'demand-forecast':
        return 'Demand Forecast';
      case 'customer-segments':
        return 'Customer Segments';
      case 'dimensions':
        return 'Data Warehouse - Dimensions';
      case 'facts':
        return 'Data Warehouse - Facts';
      default:
        return 'ML Insights Dashboard';
    }
  };

  return (
    <div className="flex h-screen">
      {/* <Sidebar currentPage={currentPage} onNavigate={setCurrentPage} /> */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* <Header 
          pageTitle={getPageTitle()} 
          darkMode={darkMode} 
        /> */}
        <main className="flex-1 overflow-y-auto p-6">
          <div className="bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 rounded-lg shadow-sm p-6">
            {renderPage()}
          </div>
        </main>
      </div>
    </div>
  );
}
