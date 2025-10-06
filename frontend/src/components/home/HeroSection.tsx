import React from 'react';

const HeroSection: React.FC = () => (
  <section className="hero-section" style={{background: '#e9ebee', padding: '48px 0', textAlign: 'center'}}>
    <div style={{maxWidth: 800, margin: '0 auto'}}>
      <div style={{height: 100, background: '#99a0ac', borderRadius: 8, margin: '0 auto 24px', width: '70%'}} />
      <div style={{height: 16, background: '#7b8594', borderRadius: 4, margin: '0 auto 8px', width: '50%'}} />
      <div style={{height: 12, background: '#7b8594', borderRadius: 4, margin: '0 auto 24px', width: '35%'}} />
      <button style={{background: '#2979ff', color: '#fff', border: 'none', borderRadius: 4, padding: '16px 40px', fontSize: 18, cursor: 'pointer'}}>Khám Phá Giải Pháp</button>
    </div>
  </section>
);

export default HeroSection;
