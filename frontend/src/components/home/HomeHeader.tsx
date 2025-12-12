import React from 'react';
import { Link } from 'react-router';

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
      <div style={{ display: 'flex', alignItems: 'center', gap: 64 }}>
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
            style={{ height: '32px' }}
          />
          DSS Analytics
        </div>
        <nav style={{ display: 'flex', gap: 32 }}>
          <Link to="/" style={{ color: '#666', textDecoration: 'none', fontSize: '15px' }}>Home</Link>
          <Link to="/solutions" style={{ color: '#666', textDecoration: 'none', fontSize: '15px' }}>Solutions</Link>
          <Link to="/about" style={{ color: '#666', textDecoration: 'none', fontSize: '15px' }}>About Us</Link>
          <Link to="/contact" style={{ color: '#666', textDecoration: 'none', fontSize: '15px' }}>Contact</Link>
        </nav>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Link to="/signin" style={{ textDecoration: 'none' }}>
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
            Sign In
          </button>
        </Link>
        <Link to="/signup" style={{ textDecoration: 'none' }}>
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
            Sign Up
          </button>
        </Link>
      </div>
    </header>
  );
};

export default HomeHeader;
