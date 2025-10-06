import React from 'react';

import HeroSection from '../components/home/HeroSection';
import FeaturesSection from '../components/home/FeaturesSection';
import FeatureCard from '../components/home/FeatureCard';


const Home: React.FC = () => {
  return (
    <div className="home-page" style={{background: '#f7f8fa', minHeight: '100vh'}}>
     
      <HeroSection />
      <FeaturesSection />
      <div style={{display: 'flex', flexWrap: 'wrap', justifyContent: 'center', maxWidth: 1100, margin: '0 auto', padding: '32px 0'}}>
        <FeatureCard color="#409eff" accent="#b3d4fc" />
        <FeatureCard color="#00e676" accent="#b2f5d6" />
        <FeatureCard color="#a259ff" accent="#e0c7fa" />
        <FeatureCard color="#ff9100" accent="#ffe0b2" />
      </div>
  
    </div>
  );
};

export default Home;
