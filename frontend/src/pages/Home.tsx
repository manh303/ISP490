import React from 'react';

import HeroSection from '../components/home/HeroSection';
import FeaturesSection from '../components/home/FeaturesSection';
import FeatureCard from '../components/home/FeatureCard';

const Home: React.FC = () => {
  return (
    <div className="home-page" style={{background: '#fff', minHeight: '100vh'}}>
      <HeroSection />
      <div style={{maxWidth: '1200px', margin: '0 auto', padding: '60px 20px'}}>
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
          <FeatureCard
            title="Phân Tích Kịch Bản"
            description="Mô phỏng và đánh giá nhiều kịch bản kinh doanh khác nhau để tìm ra giải pháp tối ưu nhất cho doanh nghiệp của bạn."
            iconBg="#e6f0ff"
            icon={<svg width="24" height="24" viewBox="0 0 24 24" fill="#0066ff"><path d="M9 21c0 .55.45 1 1 1h4c.55 0 1-.45 1-1v-1H9v1zm3-19C8.14 2 5 5.14 5 9c0 2.38 1.19 4.47 3 5.74V17c0 .55.45 1 1 1h6c.55 0 1-.45 1-1v-2.26c1.81-1.27 3-3.36 3-5.74 0-3.86-3.14-7-7-7zm2.85 11.1l-.85.6V16h-4v-2.3l-.85-.6C7.8 12.16 7 10.63 7 9c0-2.76 2.24-5 5-5s5 2.24 5 5c0 1.63-.8 3.16-2.15 4.1z"/></svg>}
          />
          <FeatureCard
            title="Báo Cáo Doanh Thu Thời Gian Thực"
            description="Theo dõi và phân tích doanh thu trực tiếp với các biểu đồ tương tác, giúp bạn nắm bắt tình hình kinh doanh ngay lập tức."
            iconBg="#ffe6e6"
            icon={<svg width="24" height="24" viewBox="0 0 24 24" fill="#ff3333"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H5V5h14v14z"/><path d="M7 12h2v5H7zm8-5h2v10h-2zm-4 7h2v3h-2zm0-4h2v2h-2z"/></svg>}
          />
          <FeatureCard
            title="Dự Báo Xu Hướng Thị Trường"
            description="Sử dụng AI và machine learning để dự đoán xu hướng thị trường, giúp doanh nghiệp chủ động trong chiến lược."
            iconBg="#fff2e6"
            icon={<svg width="24" height="24" viewBox="0 0 24 24" fill="#ff9933"><path d="M3.5 18.49l6-6.01 4 4L22 6.92l-1.41-1.41-7.09 7.97-4-4L2 16.99z"/></svg>}
          />
          <FeatureCard
            title="Tối Ưu Hóa Vận Hành"
            description="Phân tích hiệu suất vận hành và đưa ra các khuyến nghị để cải thiện quy trình, tối thiểu chi phí và nâng cao năng suất."
            iconBg="#e6ffe6"
            icon={<svg width="24" height="24" viewBox="0 0 24 24" fill="#00cc00"><path d="M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54c-.04-.24-.24-.41-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.05.3-.07.63-.07.94s.02.64.07.94l-2.03 1.58c-.18.14-.23.41-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z"/></svg>}
          />
        </div>
      </div>
    </div>
  );
};

export default Home;
