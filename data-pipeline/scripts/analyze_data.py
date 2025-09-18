# File: scripts/analyze_data.py
#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from loguru import logger
import warnings
warnings.filterwarnings('ignore')

# Set Vietnamese font for matplotlib (optional)
plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial Unicode MS']
plt.rcParams['figure.figsize'] = (12, 8)
sns.set_style("whitegrid")

DATA_DIR = Path("data")
CHARTS_DIR = Path("charts")
CHARTS_DIR.mkdir(exist_ok=True)

class DataAnalyzer:
    def __init__(self):
        self.data_dir = DATA_DIR
        self.charts_dir = CHARTS_DIR
        self.customers_df = None
        self.products_df = None
        self.transactions_df = None
        
    def load_data(self):
        """Load tất cả dữ liệu từ CSV"""
        logger.info("📖 Đọc dữ liệu từ CSV files...")
        
        try:
            self.customers_df = pd.read_csv(self.data_dir / 'customers.csv')
            self.products_df = pd.read_csv(self.data_dir / 'products.csv')
            self.transactions_df = pd.read_csv(self.data_dir / 'transactions.csv')
            
            # Convert date columns
            self.customers_df['registration_date'] = pd.to_datetime(self.customers_df['registration_date'])
            self.products_df['created_date'] = pd.to_datetime(self.products_df['created_date'])
            self.transactions_df['order_date'] = pd.to_datetime(self.transactions_df['order_date'])
            
            logger.success("✅ Đã load dữ liệu thành công!")
            logger.info(f"  👥 Customers: {len(self.customers_df):,}")
            logger.info(f"  📦 Products: {len(self.products_df):,}")
            logger.info(f"  💰 Transactions: {len(self.transactions_df):,}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi đọc dữ liệu: {e}")
            return False
        return True
    
    def basic_statistics(self):
        """Thống kê cơ bản"""
        logger.info("📊 PHÂN TÍCH THỐNG KÊ CƠ BẢN")
        
        print("\n" + "="*60)
        print("📈 TỔNG QUAN DOANH SỐ")
        print("="*60)
        
        # Doanh số tổng
        total_revenue = self.transactions_df['item_total'].sum()
        total_orders = self.transactions_df['order_id'].nunique()
        avg_order_value = self.transactions_df.groupby('order_id')['item_total'].sum().mean()
        
        print(f"💰 Tổng doanh thu: {total_revenue:,.0f} VND")
        print(f"📦 Tổng số đơn hàng: {total_orders:,}")
        print(f"💵 Giá trị đơn hàng trung bình: {avg_order_value:.0f} VND")
        
        # Top sản phẩm
        print(f"\n🏆 TOP 10 SẢN PHẨM BÁN CHẠY:")
        top_products = (self.transactions_df.groupby('product_id')
                       .agg({'quantity': 'sum', 'item_total': 'sum'})
                       .merge(self.products_df[['product_id', 'product_name']], on='product_id')
                       .sort_values('quantity', ascending=False)
                       .head(10))
        
        for idx, row in top_products.iterrows():
            print(f"  {row['product_name'][:30]:<30} | Số lượng: {row['quantity']:>4} | Doanh thu: {row['item_total']:>10,.0f}")
        
        # Top khách hàng
        print(f"\n👑 TOP 10 KHÁCH HÀNG VIP:")
        top_customers = (self.transactions_df.groupby('customer_id')
                        .agg({'item_total': 'sum', 'order_id': 'nunique'})
                        .merge(self.customers_df[['customer_id', 'customer_name']], on='customer_id')
                        .sort_values('item_total', ascending=False)
                        .head(10))
        
        for idx, row in top_customers.iterrows():
            print(f"  {row['customer_name']:<25} | Tổng chi: {row['item_total']:>10,.0f} | Số đơn: {row['order_id']:>3}")
    
    def create_revenue_chart(self):
        """Biểu đồ doanh thu theo thời gian"""
        logger.info("📊 Tạo biểu đồ doanh thu...")
        
        # Doanh thu theo tháng
        monthly_revenue = (self.transactions_df
                          .assign(month=self.transactions_df['order_date'].dt.to_period('M'))
                          .groupby('month')['item_total'].sum())
        
        plt.figure(figsize=(15, 6))
        plt.subplot(1, 2, 1)
        monthly_revenue.plot(kind='line', marker='o', color='blue', linewidth=2)
        plt.title('📈 Doanh Thu Theo Tháng', fontsize=14, fontweight='bold')
        plt.xlabel('Tháng')
        plt.ylabel('Doanh Thu (VND)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Doanh thu theo danh mục
        category_revenue = (self.transactions_df
                           .merge(self.products_df[['product_id', 'category']], on='product_id')
                           .groupby('category')['item_total'].sum()
                           .sort_values(ascending=True))
        
        plt.subplot(1, 2, 2)
        category_revenue.plot(kind='barh', color='green', alpha=0.7)
        plt.title('📊 Doanh Thu Theo Danh Mục', fontsize=14, fontweight='bold')
        plt.xlabel('Doanh Thu (VND)')
        
        plt.tight_layout()
        plt.savefig(self.charts_dir / 'revenue_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
    def create_customer_analysis(self):
        """Phân tích khách hàng"""
        logger.info("👥 Phân tích khách hàng...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Phân bố theo segment
        segment_counts = self.customers_df['customer_segment'].value_counts()
        axes[0, 0].pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%', 
                      colors=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99'])
        axes[0, 0].set_title('🎯 Phân Bố Khách Hàng Theo Segment', fontweight='bold')
        
        # Phân bố tuổi
        axes[0, 1].hist(self.customers_df['age'], bins=20, color='skyblue', edgecolor='black', alpha=0.7)
        axes[0, 1].set_title('👶 Phân Bố Tuổi Khách Hàng', fontweight='bold')
        axes[0, 1].set_xlabel('Tuổi')
        axes[0, 1].set_ylabel('Số lượng')
        
        # Lifetime value
        axes[1, 0].hist(self.customers_df['lifetime_value'], bins=30, color='gold', alpha=0.7)
        axes[1, 0].set_title('💎 Phân Bố Lifetime Value', fontweight='bold')
        axes[1, 0].set_xlabel('Lifetime Value (VND)')
        axes[1, 0].set_ylabel('Số lượng')
        
        # Đăng ký theo thời gian
        registration_monthly = (self.customers_df
                               .assign(month=self.customers_df['registration_date'].dt.to_period('M'))
                               .groupby('month').size())
        
        axes[1, 1].plot(range(len(registration_monthly)), registration_monthly.values, 
                       marker='o', color='purple', linewidth=2)
        axes[1, 1].set_title('📅 Đăng Ký Khách Hàng Theo Tháng', fontweight='bold')
        axes[1, 1].set_xlabel('Tháng')
        axes[1, 1].set_ylabel('Số KH đăng ký')
        
        plt.tight_layout()
        plt.savefig(self.charts_dir / 'customer_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
    def create_product_analysis(self):
        """Phân tích sản phẩm"""
        logger.info("📦 Phân tích sản phẩm...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Phân bố giá
        axes[0, 0].hist(self.products_df['price'], bins=50, color='lightcoral', alpha=0.7)
        axes[0, 0].set_title('💰 Phân Bố Giá Sản Phẩm', fontweight='bold')
        axes[0, 0].set_xlabel('Giá (VND)')
        axes[0, 0].set_ylabel('Số lượng')
        
        # Rating distribution
        axes[0, 1].hist(self.products_df['rating'], bins=20, color='lightgreen', alpha=0.7)
        axes[0, 1].set_title('⭐ Phân Bố Rating Sản Phẩm', fontweight='bold')
        axes[0, 1].set_xlabel('Rating')
        axes[0, 1].set_ylabel('Số lượng')
        
        # Sản phẩm theo category
        category_counts = self.products_df['category'].value_counts()
        axes[1, 0].bar(category_counts.index, category_counts.values, color='orange', alpha=0.7)
        axes[1, 0].set_title('📊 Sản Phẩm Theo Danh Mục', fontweight='bold')
        axes[1, 0].tick_params(axis='x', rotation=45)
        
        # Stock levels
        axes[1, 1].hist(self.products_df['stock_quantity'], bins=30, color='mediumpurple', alpha=0.7)
        axes[1, 1].set_title('📦 Phân Bố Tồn Kho', fontweight='bold')
        axes[1, 1].set_xlabel('Số lượng tồn kho')
        axes[1, 1].set_ylabel('Số sản phẩm')
        
        plt.tight_layout()
        plt.savefig(self.charts_dir / 'product_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def advanced_analysis(self):
        """Phân tích nâng cao - RFM, Cohort, etc."""
        logger.info("🧮 Phân tích nâng cao...")
        
        # RFM Analysis
        current_date = self.transactions_df['order_date'].max()
        
        rfm_data = (self.transactions_df.groupby('customer_id')
                   .agg({
                       'order_date': lambda x: (current_date - x.max()).days,  # Recency
                       'order_id': 'nunique',  # Frequency
                       'item_total': 'sum'  # Monetary
                   }))
        
        rfm_data.columns = ['Recency', 'Frequency', 'Monetary']
        
        # RFM Score
        rfm_data['R_Score'] = pd.qcut(rfm_data['Recency'], 5, labels=[5, 4, 3, 2, 1])
        rfm_data['F_Score'] = pd.qcut(rfm_data['Frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
        rfm_data['M_Score'] = pd.qcut(rfm_data['Monetary'], 5, labels=[1, 2, 3, 4, 5])
        
        rfm_data['RFM_Score'] = (rfm_data['R_Score'].astype(str) + 
                                rfm_data['F_Score'].astype(str) + 
                                rfm_data['M_Score'].astype(str))
        
        # Customer Segments
        def segment_customers(row):
            if row['RFM_Score'] in ['555', '554', '544', '545', '454', '455', '445']:
                return 'Champions'
            elif row['RFM_Score'] in ['543', '444', '435', '355', '354', '345', '344', '335']:
                return 'Loyal Customers'
            elif row['RFM_Score'] in ['512', '511', '422', '421', '412', '411', '311']:
                return 'New Customers'
            elif row['RFM_Score'] in ['533', '532', '531', '523', '522', '521', '515', '514']:
                return 'Potential Loyalists'
            elif row['RFM_Score'] in ['155', '154', '144', '214', '215', '115', '114']:
                return 'At Risk'
            elif row['RFM_Score'] in ['155', '154', '144', '214', '215', '115']:
                return 'Cannot Lose Them'
            else:
                return 'Others'
        
        rfm_data['Segment'] = rfm_data.apply(segment_customers, axis=1)
        
        # Visualize RFM
        plt.figure(figsize=(15, 5))
        
        plt.subplot(1, 3, 1)
        rfm_data['Segment'].value_counts().plot(kind='bar', color='steelblue', alpha=0.8)
        plt.title('🎯 Phân Khúc Khách Hàng (RFM)', fontweight='bold')
        plt.xticks(rotation=45)
        
        plt.subplot(1, 3, 2)
        plt.scatter(rfm_data['Frequency'], rfm_data['Monetary'], alpha=0.6, color='red')
        plt.xlabel('Frequency')
        plt.ylabel('Monetary')
        plt.title('💰 Frequency vs Monetary', fontweight='bold')
        
        plt.subplot(1, 3, 3)
        payment_methods = self.transactions_df['payment_method'].value_counts()
        plt.pie(payment_methods.values, labels=payment_methods.index, autopct='%1.1f%%')
        plt.title('💳 Phương Thức Thanh Toán', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.charts_dir / 'advanced_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Save RFM results
        rfm_summary = rfm_data['Segment'].value_counts().to_dict()
        
        print(f"\n🎯 PHÂN KHÚC KHÁCH HÀNG RFM:")
        for segment, count in rfm_summary.items():
            percentage = count / len(rfm_data) * 100
            print(f"  {segment:<20}: {count:>6,} ({percentage:>5.1f}%)")
        
        return rfm_data
    
    def create_business_insights(self):
        """Tạo insights kinh doanh"""
        logger.info("💡 Tạo Business Insights...")
        
        insights = []
        
        # Revenue insights
        total_revenue = self.transactions_df['item_total'].sum()
        monthly_growth = (self.transactions_df
                         .assign(month=self.transactions_df['order_date'].dt.to_period('M'))
                         .groupby('month')['item_total'].sum()
                         .pct_change().mean() * 100)
        
        insights.append(f"📈 Tổng doanh thu: {total_revenue:,.0f} VND")
        insights.append(f"📊 Tăng trưởng TB hàng tháng: {monthly_growth:.1f}%")
        
        # Customer insights
        avg_customer_value = (self.transactions_df.groupby('customer_id')['item_total']
                             .sum().mean())
        repeat_customers = (self.transactions_df.groupby('customer_id')['order_id']
                           .nunique().gt(1).sum())
        repeat_rate = repeat_customers / len(self.customers_df) * 100
        
        insights.append(f"💰 Giá trị TB mỗi khách hàng: {avg_customer_value:,.0f} VND")
        insights.append(f"🔄 Tỷ lệ khách hàng quay lại: {repeat_rate:.1f}%")
        
        # Product insights
        bestseller_category = (self.transactions_df
                              .merge(self.products_df[['product_id', 'category']], on='product_id')
                              .groupby('category')['quantity'].sum()
                              .idxmax())
        
        low_stock_products = len(self.products_df[self.products_df['stock_quantity'] < 10])
        
        insights.append(f"🏆 Danh mục bán chạy nhất: {bestseller_category}")
        insights.append(f"⚠️ Sản phẩm sắp hết hàng: {low_stock_products}")
        
        # Seasonal insights
        seasonal_sales = (self.transactions_df
                         .assign(month=self.transactions_df['order_date'].dt.month)
                         .groupby('month')['item_total'].sum())
        best_month = seasonal_sales.idxmax()
        worst_month = seasonal_sales.idxmin()
        
        insights.append(f"🌟 Tháng bán chạy nhất: Tháng {best_month}")
        insights.append(f"📉 Tháng bán ít nhất: Tháng {worst_month}")
        
        # Save insights
        insights_data = {
            'generated_at': pd.Timestamp.now().isoformat(),
            'insights': insights,
            'metrics': {
                'total_revenue': float(total_revenue),
                'monthly_growth_rate': float(monthly_growth),
                'avg_customer_value': float(avg_customer_value),
                'repeat_customer_rate': float(repeat_rate),
                'bestseller_category': bestseller_category,
                'low_stock_count': int(low_stock_products)
            }
        }
        
        with open(self.data_dir / 'business_insights.json', 'w', encoding='utf-8') as f:
            json.dump(insights_data, f, indent=2, ensure_ascii=False)
        
        print("\n" + "="*60)
        print("💡 BUSINESS INSIGHTS")
        print("="*60)
        for insight in insights:
            print(f"  {insight}")
        
        return insights_data
    
    def generate_report(self):
        """Tạo báo cáo HTML tổng hợp"""
        logger.info("📋 Tạo báo cáo HTML...")
        
        html_content = f"""
        <!DOCTYPE html>
        <html lang="vi">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>📊 Báo Cáo Phân Tích Big Data</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                    color: #333;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 10px;
                    box-shadow: 0 0 20px rgba(0,0,0,0.1);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                }}
                .content {{
                    padding: 30px;
                }}
                .metric-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin: 30px 0;
                }}
                .metric-card {{
                    background: #f8f9ff;
                    border-left: 4px solid #667eea;
                    padding: 20px;
                    border-radius: 8px;
                }}
                .metric-value {{
                    font-size: 2em;
                    font-weight: bold;
                    color: #667eea;
                }}
                .metric-label {{
                    color: #666;
                    margin-top: 10px;
                }}
                .chart-gallery {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                    gap: 20px;
                    margin: 30px 0;
                }}
                .chart-item {{
                    text-align: center;
                    padding: 20px;
                    background: #fafafa;
                    border-radius: 8px;
                }}
                .chart-item img {{
                    max-width: 100%;
                    height: auto;
                    border-radius: 8px;
                    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                }}
                .insights {{
                    background: #fff3cd;
                    border: 1px solid #ffeaa7;
                    border-radius: 8px;
                    padding: 20px;
                    margin: 30px 0;
                }}
                .insights h3 {{
                    color: #856404;
                    margin-top: 0;
                }}
                .insights ul {{
                    list-style: none;
                    padding: 0;
                }}
                .insights li {{
                    padding: 8px 0;
                    border-bottom: 1px dotted #ddd;
                }}
                .footer {{
                    background: #667eea;
                    color: white;
                    text-align: center;
                    padding: 20px;
                    margin-top: 30px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 BÁO CÁO PHÂN TÍCH BIG DATA</h1>
                    <p>Phân tích dữ liệu E-commerce - {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}</p>
                </div>
                
                <div class="content">
                    <div class="metric-grid">
                        <div class="metric-card">
                            <div class="metric-value">{len(self.customers_df):,}</div>
                            <div class="metric-label">👥 Tổng số khách hàng</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{len(self.products_df):,}</div>
                            <div class="metric-label">📦 Tổng số sản phẩm</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{len(self.transactions_df):,}</div>
                            <div class="metric-label">💰 Tổng số giao dịch</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value">{self.transactions_df['item_total'].sum():,.0f}</div>
                            <div class="metric-label">💵 Tổng doanh thu (VND)</div>
                        </div>
                    </div>
                    
                    <div class="chart-gallery">
                        <div class="chart-item">
                            <h3>📈 Phân Tích Doanh Thu</h3>
                            <img src="charts/revenue_analysis.png" alt="Revenue Analysis">
                        </div>
                        <div class="chart-item">
                            <h3>👥 Phân Tích Khách Hàng</h3>
                            <img src="charts/customer_analysis.png" alt="Customer Analysis">
                        </div>
                        <div class="chart-item">
                            <h3>📦 Phân Tích Sản Phẩm</h3>
                            <img src="charts/product_analysis.png" alt="Product Analysis">
                        </div>
                        <div class="chart-item">
                            <h3>🧮 Phân Tích Nâng Cao</h3>
                            <img src="charts/advanced_analysis.png" alt="Advanced Analysis">
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>🚀 Powered by Big Data Analytics Pipeline</p>
                    <p>📊 Dữ liệu được tạo và phân tích tự động</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        with open('data_analysis_report.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.success("✅ Đã tạo báo cáo HTML: data_analysis_report.html")
        
    def run_complete_analysis(self):
        """Chạy phân tích hoàn chỉnh"""
        logger.info("🎯 Bắt đầu phân tích dữ liệu hoàn chỉnh")
        
        if not self.load_data():
            return False
        
        # Các bước phân tích
        self.basic_statistics()
        self.create_revenue_chart()
        self.create_customer_analysis()
        self.create_product_analysis()
        rfm_data = self.advanced_analysis()
        self.create_business_insights()
        self.generate_report()
        
        logger.success("🎉 HOÀN THÀNH PHÂN TÍCH!")
        logger.info("📄 Các file kết quả đã tạo:")
        logger.info("  📊 Charts: ./charts/")
        logger.info("  📋 Báo cáo HTML: ./data_analysis_report.html")
        logger.info("  💡 Business insights: ./data/business_insights.json")
        
        return True

def main():
    """Chạy phân tích dữ liệu"""
    analyzer = DataAnalyzer()
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()