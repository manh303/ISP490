import React from 'react';

const HomeFooter: React.FC = () => {
  return (
    <footer style={{background: '#3a4250', color: '#fff', padding: '48px 0 0 0', marginTop: 48}}>
      <div style={{display: 'flex', justifyContent: 'center', gap: 64, maxWidth: 1400, margin: '0 auto', padding: '0 32px'}}>
        {/* Logo + cột 1 */}
        <div>
          <div style={{background: '#fff', width: 80, height: 32, borderRadius: 4, marginBottom: 24}} />
          <div style={{background: '#6c7684', height: 20, width: 260, borderRadius: 4, marginBottom: 12}} />
          <div style={{background: '#6c7684', height: 16, width: 200, borderRadius: 4, marginBottom: 8}} />
          <div style={{background: '#6c7684', height: 16, width: 180, borderRadius: 4, marginBottom: 8}} />
        </div>
        {/* Cột 2 */}
        <div>
          <div style={{background: '#99a0ac', height: 32, width: 140, borderRadius: 4, marginBottom: 16}} />
          <div style={{background: '#6c7684', height: 16, width: 120, borderRadius: 4, marginBottom: 8}} />
          <div style={{background: '#6c7684', height: 16, width: 100, borderRadius: 4, marginBottom: 8}} />
          <div style={{background: '#6c7684', height: 16, width: 90, borderRadius: 4, marginBottom: 8}} />
        </div>
        {/* Cột 3 */}
        <div>
          <div style={{background: '#99a0ac', height: 32, width: 180, borderRadius: 4, marginBottom: 16}} />
          <div style={{background: '#6c7684', height: 16, width: 260, borderRadius: 4, marginBottom: 8}} />
          <div style={{background: '#6c7684', height: 16, width: 220, borderRadius: 4, marginBottom: 8}} />
          <div style={{background: '#6c7684', height: 16, width: 180, borderRadius: 4, marginBottom: 8}} />
          <div style={{background: '#6c7684', height: 16, width: 240, borderRadius: 4, marginBottom: 8}} />
        </div>
        {/* Cột 4 */}
        <div>
          <div style={{background: '#99a0ac', height: 32, width: 120, borderRadius: 4, marginBottom: 16}} />
          <div style={{display: 'flex', gap: 8, marginBottom: 16}}>
            <div style={{background: '#2979ff', width: 32, height: 32, borderRadius: 4}} />
            <div style={{background: '#2979ff', width: 32, height: 32, borderRadius: 4}} />
            <div style={{background: '#4fa3ff', width: 32, height: 32, borderRadius: 4}} />
          </div>
        </div>
      </div>
      <hr style={{border: 'none', borderTop: '1px solid #5a6170', margin: '40px 0 32px 0'}} />
      <div style={{textAlign: 'center', paddingBottom: 32}}>
        <div style={{background: '#6c7684', height: 24, width: 340, borderRadius: 4, margin: '0 auto'}} />
      </div>
    </footer>
  );
};

export default HomeFooter;
