import React from 'react';

const HomeHeader: React.FC = () => {
  return (
    <header style={{display: 'flex', alignItems: 'center', justifyContent: 'space-between', background: '#f7f8fa', padding: '32px 48px 24px 48px'}}>
      <div style={{display: 'flex', alignItems: 'center', gap: 32, flex: 1}}>
        <div style={{background: '#8d96a6', color: '#fff', fontWeight: 600, fontSize: 22, borderRadius: 4, width: 90, height: 48, display: 'flex', alignItems: 'center', justifyContent: 'center'}}>DSS</div>
        <div style={{display: 'flex', gap: 24, flex: 1}}>
          <div style={{background: '#d3d7dd', borderRadius: 4, width: 110, height: 32}} />
          <div style={{background: '#d3d7dd', borderRadius: 4, width: 110, height: 32}} />
          <div style={{background: '#d3d7dd', borderRadius: 4, width: 110, height: 32}} />
          <div style={{background: '#d3d7dd', borderRadius: 4, width: 90, height: 32}} />
        </div>
      </div>
      <div style={{display: 'flex', gap: 12}}>
        <button style={{background: '#fff', color: '#222', border: '1.5px solid #434a56', borderRadius: 3, fontWeight: 500, fontSize: 16, padding: '8px 24px', marginRight: 4, cursor: 'pointer'}}>Đăng Nhập</button>
        <button style={{background: '#434a56', color: '#fff', border: 'none', borderRadius: 3, fontWeight: 500, fontSize: 16, padding: '8px 24px', cursor: 'pointer'}}>Đăng Ký</button>
      </div>
    </header>
  );
};

export default HomeHeader;
