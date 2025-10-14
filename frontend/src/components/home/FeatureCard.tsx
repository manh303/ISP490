import React from 'react';

interface FeatureCardProps {
  title: string;
  description: string;
  icon: React.ReactNode;
  iconBg: string;
  linkText?: string;
}

const FeatureCard: React.FC<FeatureCardProps> = ({ title, description, icon, iconBg, linkText = "Tìm hiểu thêm" }) => (
  <div style={{
    background: '#fff',
    borderRadius: '12px',
    padding: '32px',
    display: 'flex',
    flexDirection: 'column',
    gap: '16px',
    boxShadow: '0 2px 8px rgba(0,0,0,0.05)',
    transition: 'transform 0.2s',
    cursor: 'pointer',
    border: '1px solid #eee'
  }}>
    <div style={{
      width: '48px',
      height: '48px',
      borderRadius: '12px',
      background: iconBg,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      marginBottom: '8px'
    }}>
      {icon}
    </div>
    
    <h3 style={{
      fontSize: '20px',
      fontWeight: '600',
      color: '#1a1a1a',
      margin: '0'
    }}>
      {title}
    </h3>
    
    <p style={{
      fontSize: '15px',
      color: '#666',
      lineHeight: '1.6',
      margin: '0',
      flexGrow: 1
    }}>
      {description}
    </p>
    
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: '8px',
      color: '#0066ff',
      fontSize: '15px',
      fontWeight: '500'
    }}>
      {linkText}
      <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8z"/>
      </svg>
    </div>
  </div>
);

export default FeatureCard;
