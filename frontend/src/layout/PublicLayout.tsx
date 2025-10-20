import React from 'react';

import { Outlet } from 'react-router';
import { Header } from '../pages/Publics/Header';
import { Footer } from '../pages/Publics/Footer';

const CustomerLayoutContent: React.FC = () => {
  return (
    <div className="customer-layout min-h-screen flex flex-col bg-[#f7f8fa]">
      <Header />
      <main className="flex-1">
        <Outlet />
      </main>
      <Footer />
    </div>
  );
};

const CustomerLayout: React.FC = () => {
  return <CustomerLayoutContent />;
};

export default CustomerLayout;
