import React from 'react';

const FeaturesSection: React.FC = () => (
  <section style={{padding: '60px 0 40px 0', maxWidth: '1200px', margin: '0 auto'}}>
    <div style={{textAlign: 'center', marginBottom: '48px'}}>
      <h2 style={{
        fontSize: '28px',
        fontWeight: '600',
        color: '#1a1a1a',
        marginBottom: '16px'
      }}>Các Tính Năng Nổi Bật</h2>
      <p style={{
        fontSize: '16px',
        color: '#666',
        maxWidth: '800px',
        margin: '0 auto',
        lineHeight: '1.6'
      }}>
        Giải pháp toàn diện giúp doanh nghiệp đưa ra quyết định thông minh dựa trên dữ liệu
      </p>
    </div>

    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(2, 1fr)',
      gap: '24px',
      padding: '0 20px'
    }}>
    </div>
  </section>
);

export default FeaturesSection;
