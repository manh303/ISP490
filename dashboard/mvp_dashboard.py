#!/usr/bin/env python3
"""
MVP E-commerce DSS Dashboard
Simple dashboard for demo with basic DSS insights using existing data
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import json
import os
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="E-commerce DSS Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def init_connection():
    """Initialize database connection"""
    try:
        return psycopg2.connect(
            host='localhost',
            port=5433,
            database='ecommerce_dss',
            user='dss_user',
            password='dss_password_123'
        )
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Data loading functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_overview_data():
    """Load overview metrics"""
    conn = init_connection()
    if not conn:
        return None

    query = """
    WITH recent_data AS (
        SELECT
            source_platform,
            COUNT(*) as total_products,
            AVG(CAST(raw_data->>'price' AS DECIMAL)) as avg_price,
            COUNT(CASE WHEN raw_data->>'rating' IS NOT NULL THEN 1 END) as rated_products
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
        GROUP BY source_platform
    )
    SELECT * FROM recent_data
    """

    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading overview data: {e}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_price_comparison():
    """Load price comparison data between platforms"""
    conn = init_connection()
    if not conn:
        return None

    query = """
    WITH price_data AS (
        SELECT
            source_platform,
            raw_data->>'product_name' as product_name,
            raw_data->>'brand' as brand,
            CAST(raw_data->>'price' AS DECIMAL) as price,
            CAST(raw_data->>'price_current' AS DECIMAL) as price_current,
            CAST(raw_data->>'discount_percent' AS DECIMAL) as discount
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
            AND (raw_data->>'price' IS NOT NULL OR raw_data->>'price_current' IS NOT NULL)
            AND CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) > 0
        LIMIT 100
    )
    SELECT
        source_platform,
        brand,
        COALESCE(price, price_current) as price,
        discount,
        product_name
    FROM price_data
    WHERE COALESCE(price, price_current) IS NOT NULL
    """

    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading price data: {e}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_brand_analysis():
    """Load brand performance data"""
    conn = init_connection()
    if not conn:
        return None

    query = """
    WITH brand_data AS (
        SELECT
            COALESCE(raw_data->>'brand', raw_data->>'brand_name', 'Unknown') as brand,
            source_platform,
            COUNT(*) as product_count,
            AVG(CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL)) as avg_price,
            AVG(CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL)) as avg_rating
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
        GROUP BY brand, source_platform
        HAVING COUNT(*) > 1
        ORDER BY product_count DESC
        LIMIT 50
    )
    SELECT * FROM brand_data
    WHERE brand != 'Unknown' AND brand IS NOT NULL
    """

    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading brand data: {e}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_trending_products():
    """Load trending products data"""
    conn = init_connection()
    if not conn:
        return None

    query = """
    WITH trending AS (
        SELECT
            raw_data->>'product_name' as product_name,
            COALESCE(raw_data->>'brand', raw_data->>'brand_name') as brand,
            source_platform,
            CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) as price,
            CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL) as rating,
            CAST(COALESCE(raw_data->>'review_count', '0') AS INTEGER) as review_count,
            CAST(COALESCE(raw_data->>'sold_count', '0') AS INTEGER) as sold_count,
            created_at
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
            AND raw_data->>'product_name' IS NOT NULL
            AND LENGTH(raw_data->>'product_name') > 5
        ORDER BY
            CAST(COALESCE(raw_data->>'review_count', '0') AS INTEGER) DESC,
            CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL) DESC
        LIMIT 20
    )
    SELECT * FROM trending
    """

    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading trending data: {e}")
        conn.close()
        return pd.DataFrame()

# Dashboard layout
def main():
    # Header
    st.title("🛒 E-commerce DSS Dashboard MVP")
    st.markdown("**Decision Support System for Tiki & Lazada Price Intelligence**")

    # Sidebar
    st.sidebar.title("📊 Dashboard Controls")

    # Refresh data button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.experimental_rerun()

    # Load data
    overview_df = load_overview_data()
    price_df = load_price_comparison()
    brand_df = load_brand_analysis()
    trending_df = load_trending_products()

    # Check if data is available
    if overview_df is None or overview_df.empty:
        st.error("⚠️ No data available. Please run the ETL pipeline first.")
        st.info("💡 Tip: Run `airflow dags trigger tiki_lazada_etl_pipeline` to generate sample data.")
        return

    # Main metrics
    st.header("📈 Key Metrics Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_products = overview_df['total_products'].sum()
        st.metric("Total Products", f"{total_products:,}")

    with col2:
        avg_price = overview_df['avg_price'].mean()
        st.metric("Average Price", f"₫{avg_price:,.0f}" if pd.notna(avg_price) else "N/A")

    with col3:
        platforms = overview_df['source_platform'].nunique()
        st.metric("Platforms", platforms)

    with col4:
        rated_products = overview_df['rated_products'].sum()
        st.metric("Products with Ratings", f"{rated_products:,}")

    st.divider()

    # Platform comparison
    st.header("🏪 Platform Performance Comparison")

    col1, col2 = st.columns(2)

    with col1:
        if not overview_df.empty:
            # Products by platform
            fig_products = px.bar(
                overview_df,
                x='source_platform',
                y='total_products',
                title="Product Count by Platform",
                color='source_platform',
                color_discrete_map={'tiki': '#1f77b4', 'lazada': '#ff7f0e'}
            )
            fig_products.update_layout(showlegend=False)
            st.plotly_chart(fig_products, use_container_width=True)

    with col2:
        if not overview_df.empty and not overview_df['avg_price'].isna().all():
            # Average price by platform
            fig_price = px.bar(
                overview_df.dropna(subset=['avg_price']),
                x='source_platform',
                y='avg_price',
                title="Average Price by Platform",
                color='source_platform',
                color_discrete_map={'tiki': '#1f77b4', 'lazada': '#ff7f0e'}
            )
            fig_price.update_yaxis(title="Price (VND)")
            fig_price.update_layout(showlegend=False)
            st.plotly_chart(fig_price, use_container_width=True)

    st.divider()

    # Price analysis
    st.header("💰 Price Intelligence Analysis")

    col1, col2 = st.columns([2, 1])

    with col1:
        if not price_df.empty:
            # Price distribution
            fig_dist = px.histogram(
                price_df,
                x='price',
                color='source_platform',
                title="Price Distribution Comparison",
                nbins=30,
                color_discrete_map={'tiki': '#1f77b4', 'lazada': '#ff7f0e'}
            )
            fig_dist.update_xaxis(title="Price (VND)")
            fig_dist.update_yaxis(title="Number of Products")
            st.plotly_chart(fig_dist, use_container_width=True)

    with col2:
        if not price_df.empty:
            # Price insights
            st.subheader("💡 Key Insights")

            tiki_avg = price_df[price_df['source_platform'] == 'tiki']['price'].mean()
            lazada_avg = price_df[price_df['source_platform'] == 'lazada']['price'].mean()

            if pd.notna(tiki_avg) and pd.notna(lazada_avg):
                if tiki_avg > lazada_avg:
                    st.info(f"📊 Tiki products are {((tiki_avg/lazada_avg-1)*100):.1f}% more expensive on average")
                else:
                    st.info(f"📊 Lazada products are {((lazada_avg/tiki_avg-1)*100):.1f}% more expensive on average")

            # Discount analysis
            avg_discount = price_df['discount'].mean()
            if pd.notna(avg_discount) and avg_discount > 0:
                st.success(f"💸 Average discount: {avg_discount:.1f}%")

    st.divider()

    # Brand analysis
    st.header("🏷️ Brand Performance Analysis")

    if not brand_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Top brands by product count
            top_brands = brand_df.groupby('brand')['product_count'].sum().sort_values(ascending=False).head(10)
            fig_brands = px.bar(
                x=top_brands.values,
                y=top_brands.index,
                orientation='h',
                title="Top 10 Brands by Product Count",
                color=top_brands.values,
                color_continuous_scale='Blues'
            )
            fig_brands.update_layout(
                xaxis_title="Number of Products",
                yaxis_title="Brand",
                showlegend=False,
                coloraxis_showscale=False
            )
            st.plotly_chart(fig_brands, use_container_width=True)

        with col2:
            # Brand price comparison
            brand_price = brand_df.groupby(['brand', 'source_platform'])['avg_price'].mean().reset_index()
            brand_price = brand_price.dropna().head(20)

            if not brand_price.empty:
                fig_brand_price = px.scatter(
                    brand_price,
                    x='avg_price',
                    y='brand',
                    color='source_platform',
                    title="Brand Price Comparison",
                    color_discrete_map={'tiki': '#1f77b4', 'lazada': '#ff7f0e'}
                )
                fig_brand_price.update_xaxis(title="Average Price (VND)")
                st.plotly_chart(fig_brand_price, use_container_width=True)

    st.divider()

    # Trending products
    st.header("🔥 Trending Products")

    if not trending_df.empty:
        # Display trending products table
        display_df = trending_df[['product_name', 'brand', 'source_platform', 'price', 'rating', 'review_count']].copy()
        display_df['price'] = display_df['price'].apply(lambda x: f"₫{x:,.0f}" if pd.notna(x) and x > 0 else "N/A")
        display_df['rating'] = display_df['rating'].apply(lambda x: f"⭐ {x:.1f}" if pd.notna(x) and x > 0 else "N/A")

        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "product_name": "Product Name",
                "brand": "Brand",
                "source_platform": "Platform",
                "price": "Price",
                "rating": "Rating",
                "review_count": "Reviews"
            }
        )

    st.divider()

    # DSS Recommendations
    st.header("🎯 DSS Recommendations")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💼 Business Intelligence")

        recommendations = []

        # Price strategy recommendations
        if not price_df.empty:
            tiki_count = len(price_df[price_df['source_platform'] == 'tiki'])
            lazada_count = len(price_df[price_df['source_platform'] == 'lazada'])

            if tiki_count > lazada_count:
                recommendations.append("📈 **Market Share**: Tiki has more product variety in current dataset")
            else:
                recommendations.append("📈 **Market Share**: Lazada has more product variety in current dataset")

        # Brand recommendations
        if not brand_df.empty:
            top_brand = brand_df.groupby('brand')['product_count'].sum().idxmax()
            recommendations.append(f"🏷️ **Top Brand**: {top_brand} has the most products")

        # Price recommendations
        if not price_df.empty and not price_df['discount'].isna().all():
            avg_discount = price_df['discount'].mean()
            if avg_discount > 15:
                recommendations.append("💰 **Pricing**: High discount rates suggest competitive market")
            else:
                recommendations.append("💰 **Pricing**: Low discount rates suggest premium positioning")

        for rec in recommendations:
            st.markdown(rec)

    with col2:
        st.subheader("🔮 Next Steps")

        next_steps = [
            "🔄 **Data Pipeline**: Set up automated daily data collection",
            "📊 **Advanced Analytics**: Implement price trend analysis",
            "🤖 **ML Models**: Build demand forecasting models",
            "📱 **Real-time**: Add real-time price monitoring alerts",
            "🎯 **Segmentation**: Customer segment analysis",
            "🏆 **Competitive**: Competitor strategy insights"
        ]

        for step in next_steps:
            st.markdown(step)

    # Footer
    st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Data Freshness", "Real-time")

    with col2:
        st.metric("Coverage", "Tiki + Lazada")

    with col3:
        last_update = datetime.now().strftime("%H:%M:%S")
        st.metric("Last Update", last_update)

    # Technical info in sidebar
    with st.sidebar:
        st.divider()
        st.subheader("🔧 Technical Info")
        st.text("Database: PostgreSQL")
        st.text("ETL: Airflow + Python")
        st.text("Visualization: Streamlit + Plotly")
        st.text("Architecture: Data Warehouse")

        if st.button("📋 View Raw Data Stats"):
            if overview_df is not None and not overview_df.empty:
                st.json(overview_df.to_dict('records'))

if __name__ == "__main__":
    main()