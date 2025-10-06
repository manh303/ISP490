import React from 'react';

interface FeatureCardProps {
  color: string;
  accent: string;
}

const FeatureCard: React.FC<FeatureCardProps> = ({ color, accent }) => (
  <div style={{border: '1px solid #e0e0e0', borderRadius: 8, padding: 24, background: '#fff', minWidth: 280, flex: 1, margin: 12}}>
    <div style={{width: 32, height: 32, background: color, borderRadius: 4, marginBottom: 12}} />
    <div style={{height: 16, background: '#434a56', borderRadius: 4, marginBottom: 8, width: '70%'}} />
    <div style={{height: 12, background: '#cfd4db', borderRadius: 4, marginBottom: 6, width: '90%'}} />
    <div style={{height: 12, background: '#cfd4db', borderRadius: 4, marginBottom: 6, width: '80%'}} />
    <div style={{height: 12, background: '#cfd4db', borderRadius: 4, marginBottom: 12, width: '60%'}} />
    <div style={{height: 16, background: accent, borderRadius: 4, width: '30%'}} />
  </div>
);

export default FeatureCard;
