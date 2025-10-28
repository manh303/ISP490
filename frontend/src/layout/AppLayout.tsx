import React from "react";

const AppLayout: React.FC = () => {
  return (
    <div className="min-h-screen flex">
      {/* TODO: thay bằng Sidebar/Header thật của bạn nếu đã có */}
      <aside className="w-64 border-r hidden md:block">Sidebar</aside>
      <main className="flex-1 p-4">
      </main>
    </div>
  );
};

export default AppLayout;
