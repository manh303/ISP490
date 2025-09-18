# File: dashboard.py
#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import joblib
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Page config
st.set_page_config(
    page_title="🚀 Big Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        text-align: center;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: bold;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load dữ liệu từ CSV files"""
    try:
        customers_df = pd.read_csv('data/customers.csv')
        products_df = pd.read_csv('data/products.csv')
        transactions_df = pd.read_csv('data/transactions.csv')
        
        # Convert dates
        customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])
        products_df['created_date'] = pd.to_datetime(products_df['created_date'])
        transactions_df['order_date'] = pd.to_datetime(transactions_df['order_date'])
        
        return customers_df, products_df, transactions_df
    except Exception as e:
        st.error(f"❌ Không thể load dữ liệu: {e}")
        return None, None, None

@st.cache_data
def load_business_insights():
    """Load business insights"""
    try:
        with open('data/business_insights.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def main():
    # Header
    st.markdown('<h1 class="main-header">🚀 BIG DATA ANALYTICS DASHBOARD</h1>', unsafe_allow_html=True)
    
    # Load data
    customers_df, products_df, transactions_df = load_data()
    
    if customers_df is None:
        st.error("⚠️ Vui lòng chạy `python scripts/local_data_pipeline.py` trước để tạo dữ liệu!")
        st.stop()
    
    # Sidebar
    st.sidebar.image("https://via.placeholder.com/200x100/667eea/white?text=Big+Data", width=200)
    st.sidebar.markdown("## 📊 Điều Khiển Dashboard")
    
    # Page selection
    page = st.sidebar.selectbox(
        "🎯 Chọn Trang Phân Tích:",
        ["📈 Tổng Quan", "👥 Khách Hàng", "📦 Sản Phẩm", "💰 Doanh Thu", "🤖 ML Insights", "🔍 Data Explorer"]
    )
    
    # Date filter
    date_range = st.sidebar.date_input(
        "📅 Khoảng Thời Gian:",
        value=[transactions_df['order_date'].min(), transactions_df['order_date'].max()],
        min_value=transactions_df['order_date'].min(),
        max_value=transactions_df['order_date'].max()
    )
    
    # Filter transactions by date
    if len(date_range) == 2:
        filtered_transactions = transactions_df[
            (transactions_df['order_date'] >= pd.Timestamp(date_range[0])) &
            (transactions_df['order_date'] <= pd.Timestamp(date_range[1]))
        ]
    else:
        filtered_transactions = transactions_df
    
    # Main content based on page selection
    if page == "📈 Tổng Quan":
        show_overview(customers_df, products_df, filtered_transactions)
    elif page == "👥 Khách Hàng":
        show_customer_analysis(customers_df, filtered_transactions)
    elif page == "📦 Sản Phẩm":
        show_product_analysis(products_df, filtered_transactions)
    elif page == "💰 Doanh Thu":
        show_revenue_analysis(filtered_transactions)
    elif page == "🤖 ML Insights":
        show_ml_insights()
    elif page == "🔍 Data Explorer":
        show_data_explorer(customers_df, products_df, filtered_transactions)

def show_overview(customers_df, products_df, transactions_df):
    """Trang tổng quan"""
    st.markdown("## 📊 Tổng Quan Hệ Thống")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("👥 Khách Hàng", f"{len(customers_df):,}")
        
    with col2:
        st.metric("📦 Sản Phẩm", f"{len(products_df):,}")
        
    with col3:
        st.metric("💰 Giao Dịch", f"{len(transactions_df):,}")
        
    with col4:
        total_revenue = transactions_df['item_total'].sum()
        st.metric("💵 Tổng Doanh Thu", f"{total_revenue:,.0f} VND")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Doanh Thu Theo Thời Gian")
        daily_revenue = transactions_df.groupby(transactions_df['order_date'].dt.date)['item_total'].sum()
        fig = px.line(x=daily_revenue.index, y=daily_revenue.values, 
                     title="Doanh Thu Hàng Ngày")
        fig.update_layout(xaxis_title="Ngày", yaxis_title="Doanh Thu (VND)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Phân Khúc Khách Hàng")
        segment_counts = customers_df['customer_segment'].value_counts()
        fig = px.pie(values=segment_counts.values, names=segment_counts.index,
                    title="Phân Bố Customer Segments")
        st.plotly_chart(fig, use_container_width=True)

def show_customer_analysis(customers_df, transactions_df):
    """Phân tích khách hàng"""
    st.markdown("## 👥 Phân Tích Khách Hàng")
    
    # Customer metrics
    customer_stats = transactions_df.groupby('customer_id').agg({
        'item_total': ['sum', 'mean', 'count'],
        'order_id': 'nunique'
    }).round(2)
    
    customer_stats.columns = ['total_spent', 'avg_order_value', 'total_transactions', 'unique_orders']
    customer_stats = customer_stats.merge(customers_df[['customer_id', 'customer_name', 'customer_segment', 'age', 'country']], 
                                        on='customer_id', how='left')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏆 Top Khách Hàng VIP")
        top_customers = customer_stats.nlargest(10, 'total_spent')[['customer_name', 'total_spent', 'unique_orders']]
        st.dataframe(top_customers, use_container_width=True)
    
    with col2:
        st.markdown("### 🌍 Khách Hàng Theo Quốc Gia")
        country_counts = customers_df['country'].value_counts().head(10)
        fig = px.bar(x=country_counts.index, y=country_counts.values,
                    title="Top 10 Quốc Gia")
        st.plotly_chart(fig, use_container_width=True)
    
    # Age analysis
    st.markdown("### 👶 Phân Tích Độ Tuổi")
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.histogram(customers_df, x='age', nbins=20, 
                          title="Phân Bố Tuổi Khách Hàng")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        age_spending = customer_stats.groupby(pd.cut(customer_stats.merge(customers_df[['customer_id', 'age']], on='customer_id')['age'], 
                                                   bins=5))['total_spent'].mean()
        fig = px.bar(x=[str(x) for x in age_spending.index], y=age_spending.values,
                    title="Chi Tiêu Trung Bình Theo Nhóm Tuổi")
        st.plotly_chart(fig, use_container_width=True)

def show_product_analysis(products_df, transactions_df):
    """Phân tích sản phẩm"""
    st.markdown("## 📦 Phân Tích Sản Phẩm")
    
    # Product performance
    product_stats = transactions_df.groupby('product_id').agg({
        'quantity': 'sum',
        'item_total': 'sum'
    }).merge(products_df[['product_id', 'product_name', 'category', 'price']], 
             on='product_id', how='left')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏆 Sản Phẩm Bán Chạy")
        top_products = product_stats.nlargest(10, 'quantity')[['product_name', 'quantity', 'item_total']]
        st.dataframe(top_products, use_container_width=True)
    
    with col2:
        st.markdown("### 📊 Doanh Thu Theo Danh Mục")
        category_revenue = (transactions_df.merge(products_df[['product_id', 'category']], on='product_id')
                          .groupby('category')['item_total'].sum().sort_values(ascending=False))
        fig = px.bar(x=category_revenue.values, y=category_revenue.index, 
                    orientation='h', title="Revenue by Category")
        st.plotly_chart(fig, use_container_width=True)
    
    # Price analysis
    st.markdown("### 💰 Phân Tích Giá")
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.histogram(products_df, x='price', nbins=30,
                          title="Phân Bố Giá Sản Phẩm")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.box(products_df, x='category', y='price',
                    title="Giá Theo Danh Mục")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

def show_revenue_analysis(transactions_df):
    """Phân tích doanh thu"""
    st.markdown("## 💰 Phân Tích Doanh Thu")
    
    # Revenue trends
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Xu Hướng Doanh Thu")
        monthly_revenue = transactions_df.groupby(transactions_df['order_date'].dt.to_period('M'))['item_total'].sum()
        fig = px.line(x=monthly_revenue.index.astype(str), y=monthly_revenue.values,
                     title="Doanh Thu Theo Tháng")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 📅 Doanh Thu Theo Ngày Trong Tuần")
        dow_revenue = transactions_df.groupby(transactions_df['order_date'].dt.dayofweek)['item_total'].sum()
        dow_names = ['Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7', 'Chủ Nhật']
        fig = px.bar(x=dow_names, y=dow_revenue.values,
                    title="Doanh Thu Theo Ngày Trong Tuần")
        st.plotly_chart(fig, use_container_width=True)
    
    # Payment method analysis
    st.markdown("### 💳 Phân Tích Phương Thức Thanh Toán")
    col1, col2 = st.columns(2)
    
    with col1:
        payment_counts = transactions_df['payment_method'].value_counts()
        fig = px.pie(values=payment_counts.values, names=payment_counts.index,
                    title="Phân Bố Phương Thức Thanh Toán")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        payment_revenue = transactions_df.groupby('payment_method')['item_total'].sum().sort_values(ascending=False)
        fig = px.bar(x=payment_revenue.index, y=payment_revenue.values,
                    title="Doanh Thu Theo Phương Thức Thanh Toán")
        st.plotly_chart(fig, use_container_width=True)

def show_ml_insights():
    """Hiển thị ML insights"""
    st.markdown("## 🤖 Machine Learning Insights")
    
    # Check if ML models exist
    models_dir = Path("models")
    if not models_dir.exists():
        st.warning("⚠️ Chưa có ML models. Chạy `python scripts/ml_analysis.py` để tạo models.")
        return
    
    # Load ML summary if available
    try:
        with open('models/ml_summary.json', 'r', encoding='utf-8') as f:
            ml_summary = json.load(f)
    except:
        ml_summary = {}
    
    if ml_summary:
        st.markdown("### 📊 Model Performance")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            churn_rate = ml_summary.get('ml_insights', {}).get('churn_rate', 'N/A')
            st.metric("⚠️ Churn Rate", churn_rate)
        
        with col2:
            high_value = ml_summary.get('ml_insights', {}).get('high_value_customers', 'N/A')
            st.metric("💎 High-Value Customers", high_value)
        
        with col3:
            avg_clv = ml_summary.get('ml_insights', {}).get('avg_clv', 'N/A')
            st.metric("💰 Average CLV", f"{avg_clv} VND" if avg_clv != 'N/A' else 'N/A')
        
        with col4:
            models_count = len(ml_summary.get('models_created', []))
            st.metric("🤖 Models Created", models_count)
    
    # Model files info
    st.markdown("### 📁 Available Models")
    model_files = list(models_dir.glob("*.pkl"))
    if model_files:
        for model_file in model_files:
            size_mb = model_file.stat().st_size / 1024 / 1024
            st.write(f"🧠 **{model_file.stem}**: {size_mb:.2f} MB")
    else:
        st.info("Chưa có model files. Chạy ML analysis để tạo models.")
    
    # Load and display feature importance if available
    try:
        clv_features = pd.read_csv('models/clv_feature_importance.csv')
        st.markdown("### 🔍 CLV Feature Importance")
        fig = px.bar(clv_features.head(10), x='importance', y='feature',
                    orientation='h', title="Top 10 Features for CLV Prediction")
        st.plotly_chart(fig, use_container_width=True)
    except:
        pass

def show_data_explorer(customers_df, products_df, transactions_df):
    """Data explorer"""
    st.markdown("## 🔍 Data Explorer")
    
    # Dataset selection
    dataset_option = st.selectbox("Chọn Dataset:", ["Customers", "Products", "Transactions"])
    
    if dataset_option == "Customers":
        df = customers_df
    elif dataset_option == "Products":
        df = products_df
    else:
        df = transactions_df
    
    # Dataset info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Số Dòng", f"{len(df):,}")
    with col2:
        st.metric("📋 Số Cột", len(df.columns))
    with col3:
        st.metric("💾 Kích Thước", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    
    # Column selection
    columns_to_show = st.multiselect("Chọn Cột Hiển Thị:", df.columns.tolist(), default=df.columns.tolist()[:5])
    
    # Filters
    if columns_to_show:
        st.markdown("### 🔧 Filters")
        filters = {}
        for col in columns_to_show:
            if df[col].dtype in ['object']:
                unique_vals = df[col].unique()
                if len(unique_vals) <= 50:  # Only show filter if reasonable number of unique values
                    selected_vals = st.multiselect(f"Filter {col}:", unique_vals, default=unique_vals[:5])
                    if selected_vals:
                        filters[col] = selected_vals
        
        # Apply filters
        filtered_df = df.copy()
        for col, vals in filters.items():
            filtered_df = filtered_df[filtered_df[col].isin(vals)]
        
        # Display data
        st.markdown(f"### 📊 Data Preview ({len(filtered_df):,} rows)")
        st.dataframe(filtered_df[columns_to_show].head(1000), use_container_width=True)
        
        # Summary statistics
        if st.checkbox("Hiện Thống Kê Tóm Tắt"):
            st.markdown("### 📈 Summary Statistics")
            st.dataframe(filtered_df[columns_to_show].describe(), use_container_width=True)

if __name__ == "__main__":
    main()