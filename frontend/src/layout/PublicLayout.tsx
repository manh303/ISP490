import React from 'react';
import HomeHeader from '../components/home/HomeHeader';
import HomeFooter from '../components/home/HomeFooter';
import { Outlet } from 'react-router';

const CustomerLayoutContent: React.FC = () => {
  return (
    <div className="customer-layout min-h-screen flex flex-col bg-[#f7f8fa]">
      <HomeHeader />
      <main className="flex-1">
        <Outlet />
      </main>
      <HomeFooter />
    </div>
  );
};

const CustomerLayout: React.FC = () => {
  return <CustomerLayoutContent />;
};

export default CustomerLayout;
