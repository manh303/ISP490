import React from 'react';

const HomeHeader: React.FC = () => {
  return (
    <header style={{
      display: 'flex', 
      alignItems: 'center', 
      justifyContent: 'space-between', 
      background: '#fff',
      padding: '16px 48px',
      boxShadow: '0 2px 4px rgba(0,0,0,0.05)'
    }}>
      <div style={{display: 'flex', alignItems: 'center', gap: 64}}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          fontSize: '20px',
          fontWeight: 600,
          color: '#0066ff'
        }}>
          <img 
            src="/dss-logo.png" 
            alt="DSS Analytics" 
            style={{height: '32px'}}
          />
          DSS Analytics
        </div>
        <nav style={{display: 'flex', gap: 32}}>
          <a href="#" style={{color: '#666', textDecoration: 'none', fontSize: '15px'}}>Trang Chủ</a>
          <a href="#" style={{color: '#666', textDecoration: 'none', fontSize: '15px'}}>Giải Pháp</a>
          <a href="#" style={{color: '#666', textDecoration: 'none', fontSize: '15px'}}>Về Chúng Tôi</a>
          <a href="#" style={{color: '#666', textDecoration: 'none', fontSize: '15px'}}>Liên Hệ</a>
        </nav>
      </div>
      <div style={{display: 'flex', gap: 12}}>
        <button style={{
          background: 'transparent',
          color: '#0066ff',
          border: '1.5px solid #0066ff',
          borderRadius: 4,
          fontWeight: 500,
          fontSize: 14,
          padding: '8px 24px',
          cursor: 'pointer',
          transition: 'all 0.2s'
        }}>
          Đăng Nhập
        </button>
        <button style={{
          background: '#0066ff',
          color: '#fff',
          border: 'none',
          borderRadius: 4,
          fontWeight: 500,
          fontSize: 14,
          padding: '8px 24px',
          cursor: 'pointer',
          transition: 'all 0.2s'
        }}>
          Đăng Ký
        </button>
      </div>
    </header>
  );
};

export default HomeHeader;
