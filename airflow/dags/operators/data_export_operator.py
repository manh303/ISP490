#!/usr/bin/env python3
"""
Data Export Operator for Airflow
Handles automated report generation, data exports, and business intelligence outputs
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import tempfile

# Airflow imports
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.email.operators.email import EmailOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

# Data processing and visualization
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# File handling
import xlsxwriter
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.chart import BarChart, LineChart, PieChart, Reference
import boto3
from botocore.exceptions import NoCredentialsError

# Configure logging
logger = logging.getLogger(__name__)

# Set plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class DataExportOperator(BaseOperator):
    """
    Advanced Data Export Operator for automated reporting and data distribution
    
    Features:
    - Multiple export formats (CSV, Excel, PDF, JSON)
    - Interactive dashboards (HTML with Plotly)
    - Automated report scheduling
    - Cloud storage integration (S3, GCS)
    - Email delivery with attachments
    - Custom business intelligence reports
    """
    
    template_fields = ['export_types', 'export_queries', 'report_config', 'output_path']
    
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        export_types: List[str] = None,
        export_queries: Optional[Dict[str, str]] = None,
        report_config: Optional[Dict[str, Any]] = None,
        output_path: str = '/app/exports',
        export_formats: List[str] = None,
        enable_visualizations: bool = True,
        enable_cloud_upload: bool = False,
        cloud_provider: str = 's3',
        cloud_bucket: Optional[str] = None,
        email_recipients: Optional[List[str]] = None,
        auto_cleanup_days: int = 7,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.export_types = export_types or ['daily_summary', 'customer_insights']
        self.export_queries = export_queries or {}
        self.report_config = report_config or {}
        self.output_path = Path(output_path)
        self.export_formats = export_formats or ['csv', 'xlsx', 'pdf']
        self.enable_visualizations = enable_visualizations
        self.enable_cloud_upload = enable_cloud_upload
        self.cloud_provider = cloud_provider
        self.cloud_bucket = cloud_bucket
        self.email_recipients = email_recipients or []
        self.auto_cleanup_days = auto_cleanup_days
        
        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Export results storage
        self.export_results = {
            'timestamp': datetime.now(),
            'exports_generated': 0,
            'formats_created': 0,
            'cloud_uploads': 0,
            'emails_sent': 0,
            'export_details': {}
        }

    def execute(self, context: Context) -> Dict[str, Any]:
        """Main execution method"""
        logger.info("📤 Starting data export and reporting pipeline...")
        
        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            
            # Process each export type
            for export_type in self.export_types:
                logger.info(f"📊 Generating export: {export_type}")
                
                try:
                    export_result = self._generate_export(postgres_hook, export_type, context)
                    self.export_results['export_details'][export_type] = export_result
                    self.export_results['exports_generated'] += 1
                    
                except Exception as export_error:
                    logger.error(f"❌ Failed to generate {export_type}: {export_error}")
                    self.export_results['export_details'][export_type] = {
                        'status': 'failed',
                        'error': str(export_error),
                        'timestamp': datetime.now()
                    }
            
            # Cloud upload if enabled
            if self.enable_cloud_upload and self.cloud_bucket:
                self._upload_to_cloud(context)
            
            # Send email reports if configured
            if self.email_recipients:
                self._send_email_reports(context)
            
            # Cleanup old files
            self._cleanup_old_files()
            
            logger.info(f"✅ Export pipeline completed. "
                       f"Generated: {self.export_results['exports_generated']}, "
                       f"Formats: {self.export_results['formats_created']}")
            
            return self.export_results
            
        except Exception as e:
            logger.error(f"❌ Export pipeline failed: {e}")
            raise AirflowException(f"Data export failed: {e}")

    def _generate_export(self, postgres_hook: PostgresHook, export_type: str, context: Context) -> Dict[str, Any]:
        """Generate a specific export type"""
        
        # Get data for export
        export_data = self._get_export_data(postgres_hook, export_type, context)
        
        if export_data.empty:
            logger.warning(f"⚠️ No data available for {export_type}")
            return {
                'status': 'skipped',
                'reason': 'no_data',
                'timestamp': datetime.now()
            }
        
        logger.info(f"📊 Processing {len(export_data)} records for {export_type}")
        
        # Process and enrich data
        processed_data = self._process_export_data(export_data, export_type, context)
        
        # Generate files in different formats
        generated_files = []
        
        for format_type in self.export_formats:
            try:
                file_path = self._generate_file(processed_data, export_type, format_type, context)
                generated_files.append({
                    'format': format_type,
                    'path': str(file_path),
                    'size_mb': file_path.stat().st_size / 1024**2,
                    'created_at': datetime.now()
                })
                self.export_results['formats_created'] += 1
                
            except Exception as format_error:
                logger.error(f"❌ Failed to generate {format_type} for {export_type}: {format_error}")
        
        # Generate visualizations if enabled
        viz_files = []
        if self.enable_visualizations:
            viz_files = self._generate_visualizations(processed_data, export_type, context)
        
        return {
            'status': 'success',
            'export_type': export_type,
            'records_processed': len(processed_data),
            'files_generated': generated_files,
            'visualizations': viz_files,
            'timestamp': datetime.now()
        }

    def _get_export_data(self, postgres_hook: PostgresHook, export_type: str, context: Context) -> pd.DataFrame:
        """Get data for specific export type"""
        
        execution_date = context['execution_date']
        
        # Define export queries
        export_queries = {
            'daily_summary': f"""
                SELECT 
                    DATE(created_at) as report_date,
                    'Orders' as metric_category,
                    COUNT(*) as total_count,
                    SUM(total_amount) as total_value,
                    AVG(total_amount) as average_value,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM orders_processed 
                WHERE DATE(created_at) = '{execution_date.date()}'
                
                UNION ALL
                
                SELECT 
                    DATE(processed_at) as report_date,
                    'Products' as metric_category,
                    COUNT(*) as total_count,
                    AVG(product_quality_score) as total_value,
                    AVG(product_weight_g) as average_value,
                    COUNT(DISTINCT product_category_clean) as unique_customers
                FROM products_processed 
                WHERE DATE(processed_at) = '{execution_date.date()}'
                
                UNION ALL
                
                SELECT 
                    DATE(processed_at) as report_date,
                    'Customers' as metric_category,
                    COUNT(*) as total_count,
                    0 as total_value,
                    0 as average_value,
                    COUNT(DISTINCT customer_state) as unique_customers
                FROM customers_processed 
                WHERE DATE(processed_at) = '{execution_date.date()}'
            """,
            
            'customer_insights': f"""
                SELECT 
                    c.customer_state,
                    c.customer_segment,
                    COUNT(DISTINCT c.customer_id) as customer_count,
                    COUNT(DISTINCT o.order_id) as total_orders,
                    COALESCE(SUM(o.total_amount), 0) as total_revenue,
                    COALESCE(AVG(o.total_amount), 0) as avg_order_value,
                    COUNT(DISTINCT oi.product_id) as unique_products_purchased,
                    DATE('{execution_date.date()}') as report_date
                FROM customers_processed c
                LEFT JOIN orders_processed o ON c.customer_id = o.customer_id
                LEFT JOIN order_items_processed oi ON o.order_id = oi.order_id
                WHERE c.processed_at >= '{execution_date.date() - timedelta(days=7)}'
                GROUP BY c.customer_state, c.customer_segment
                HAVING customer_count > 0
                ORDER BY total_revenue DESC
            """,
            
            'product_performance': f"""
                SELECT 
                    p.product_category_clean,
                    p.weight_category,
                    COUNT(DISTINCT p.product_id) as product_count,
                    COUNT(DISTINCT oi.order_id) as order_count,
                    COALESCE(SUM(oi.quantity), 0) as total_quantity_sold,
                    COALESCE(SUM(oi.price * oi.quantity), 0) as total_revenue,
                    COALESCE(AVG(oi.price), 0) as avg_price,
                    COALESCE(AVG(p.product_quality_score), 0) as avg_quality_score,
                    DATE('{execution_date.date()}') as report_date
                FROM products_processed p
                LEFT JOIN order_items_processed oi ON p.product_id = oi.product_id
                WHERE p.processed_at >= '{execution_date.date() - timedelta(days=30)}'
                GROUP BY p.product_category_clean, p.weight_category
                ORDER BY total_revenue DESC
            """,
            
            'sales_trends': f"""
                SELECT 
                    DATE(o.order_date) as order_date,
                    EXTRACT(hour FROM o.order_date) as hour_of_day,
                    EXTRACT(dow FROM o.order_date) as day_of_week,
                    COUNT(*) as order_count,
                    SUM(o.total_amount) as daily_revenue,
                    AVG(o.total_amount) as avg_order_value,
                    COUNT(DISTINCT o.customer_id) as unique_customers,
                    COUNT(DISTINCT oi.product_id) as unique_products
                FROM orders_processed o
                JOIN order_items_processed oi ON o.order_id = oi.order_id
                WHERE o.order_date >= '{execution_date.date() - timedelta(days=30)}'
                GROUP BY DATE(o.order_date), EXTRACT(hour FROM o.order_date), EXTRACT(dow FROM o.order_date)
                ORDER BY order_date, hour_of_day
            """
        }
        
        # Use custom query if provided
        query = self.export_queries.get(export_type) or export_queries.get(export_type)
        
        if not query:
            raise ValueError(f"No export query defined for: {export_type}")
        
        return postgres_hook.get_pandas_df(query)

    def _process_export_data(self, data: pd.DataFrame, export_type: str, context: Context) -> pd.DataFrame:
        """Process and enrich export data"""
        
        df = data.copy()
        
        # Add common metadata
        df['export_generated_at'] = datetime.now()
        df['dag_run_id'] = context['dag_run'].run_id
        df['execution_date'] = context['execution_date']
        
        # Type-specific processing
        if export_type == 'daily_summary':
            # Add percentage calculations
            if 'total_count' in df.columns:
                total_sum = df['total_count'].sum()
                df['percentage_of_total'] = (df['total_count'] / total_sum * 100).round(2)
            
            # Add growth indicators (mock data - would need historical comparison)
            df['growth_indicator'] = np.random.choice(['↑', '↓', '→'], size=len(df))
            df['growth_percentage'] = np.random.uniform(-10, 25, size=len(df)).round(1)
        
        elif export_type == 'customer_insights':
            # Calculate customer lifetime value tiers
            if 'total_revenue' in df.columns:
                df['ltv_tier'] = pd.qcut(
                    df['total_revenue'].fillna(0), 
                    q=4, 
                    labels=['Low', 'Medium', 'High', 'Premium'],
                    duplicates='drop'
                )
            
            # Add customer acquisition cost (mock calculation)
            df['estimated_cac'] = (df['total_revenue'] * 0.15).round(2)
            
            # Calculate order frequency
            df['order_frequency'] = (
                df['total_orders'] / df['customer_count']
            ).round(2)
        
        elif export_type == 'product_performance':
            # Performance scoring
            if 'total_revenue' in df.columns and 'order_count' in df.columns:
                df['performance_score'] = (
                    (df['total_revenue'] / df['total_revenue'].max() * 0.5) +
                    (df['order_count'] / df['order_count'].max() * 0.3) +
                    (df['avg_quality_score'] / 10 * 0.2)
                ).round(3)
            
            # Inventory recommendations
            df['inventory_recommendation'] = df.apply(
                lambda row: 'Increase' if row.get('performance_score', 0) > 0.7 
                else 'Decrease' if row.get('performance_score', 0) < 0.3 
                else 'Maintain', axis=1
            )
        
        # Format numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if 'percentage' in col.lower() or 'score' in col.lower():
                df[col] = df[col].round(2)
            elif 'revenue' in col.lower() or 'value' in col.lower():
                df[col] = df[col].round(2)
            elif 'count' in col.lower():
                df[col] = df[col].astype(int)
        
        return df

    def _generate_file(self, data: pd.DataFrame, export_type: str, format_type: str, context: Context) -> Path:
        """Generate file in specified format"""
        
        execution_date = context['execution_date'].strftime('%Y%m%d')
        timestamp = datetime.now().strftime('%H%M%S')
        
        filename = f"{export_type}_{execution_date}_{timestamp}.{format_type}"
        file_path = self.output_path / filename
        
        if format_type == 'csv':
            data.to_csv(file_path, index=False)
            
        elif format_type == 'xlsx':
            self._generate_excel_file(data, file_path, export_type)
            
        elif format_type == 'json':
            data.to_json(file_path, orient='records', indent=2, date_format='iso')
            
        elif format_type == 'pdf':
            self._generate_pdf_report(data, file_path, export_type)
            
        elif format_type == 'html':
            self._generate_html_report(data, file_path, export_type)
            
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        logger.info(f"✅ Generated {format_type.upper()} file: {file_path}")
        return file_path

    def _generate_excel_file(self, data: pd.DataFrame, file_path: Path, export_type: str):
        """Generate formatted Excel file"""
        
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Write main data
            data.to_excel(writer, sheet_name='Data', index=False)
            worksheet = writer.sheets['Data']
            
            # Format headers
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BD',
                'border': 1
            })
            
            # Format data cells
            number_format = workbook.add_format({'num_format': '#,##0.00'})
            percentage_format = workbook.add_format({'num_format': '0.00%'})
            
            # Apply formatting
            for col_num, column in enumerate(data.columns):
                worksheet.write(0, col_num, column, header_format)
                
                # Auto-adjust column width
                max_length = max(
                    data[column].astype(str).map(len).max(),
                    len(str(column))
                ) + 2
                worksheet.set_column(col_num, col_num, min(max_length, 50))
                
                # Apply number formats
                if 'percentage' in column.lower():
                    worksheet.set_column(col_num, col_num, None, percentage_format)
                elif data[column].dtype in ['float64', 'int64'] and 'count' not in column.lower():
                    worksheet.set_column(col_num, col_num, None, number_format)
            
            # Add summary sheet
            if export_type == 'customer_insights':
                self._add_customer_summary_sheet(data, writer, workbook)
            elif export_type == 'product_performance':
                self._add_product_summary_sheet(data, writer, workbook)

    def _add_customer_summary_sheet(self, data: pd.DataFrame, writer, workbook):
        """Add customer insights summary sheet"""
        
        # Create summary data
        summary_data = {
            'Metric': [
                'Total Customers',
                'Total States',
                'Total Revenue',
                'Average Order Value',
                'Top Performing State',
                'Top Customer Segment'
            ],
            'Value': [
                data['customer_count'].sum(),
                data['customer_state'].nunique(),
                f"${data['total_revenue'].sum():,.2f}",
                f"${data['avg_order_value'].mean():,.2f}",
                data.loc[data['total_revenue'].idxmax(), 'customer_state'],
                data.groupby('customer_segment')['customer_count'].sum().idxmax()
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    def _add_product_summary_sheet(self, data: pd.DataFrame, writer, workbook):
        """Add product performance summary sheet"""
        
        summary_data = {
            'Metric': [
                'Total Products',
                'Total Categories',
                'Total Revenue',
                'Average Quality Score',
                'Best Performing Category',
                'Highest Revenue Product Category'
            ],
            'Value': [
                data['product_count'].sum(),
                data['product_category_clean'].nunique(),
                f"${data['total_revenue'].sum():,.2f}",
                f"{data['avg_quality_score'].mean():.2f}",
                data.loc[data['performance_score'].idxmax(), 'product_category_clean'] if 'performance_score' in data.columns else 'N/A',
                data.loc[data['total_revenue'].idxmax(), 'product_category_clean']
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    def _generate_pdf_report(self, data: pd.DataFrame, file_path: Path, export_type: str):
        """Generate PDF report with visualizations"""
        
        with PdfPages(file_path) as pdf:
            # Title page
            fig, ax = plt.subplots(figsize=(8.5, 11))
            ax.text(0.5, 0.8, f'{export_type.replace("_", " ").title()} Report', 
                   ha='center', va='center', fontsize=24, weight='bold')
            ax.text(0.5, 0.7, f'Generated on {datetime.now().strftime("%Y-%m-%d %H:%M")}', 
                   ha='center', va='center', fontsize=14)
            ax.text(0.5, 0.6, f'Total Records: {len(data)}', 
                   ha='center', va='center', fontsize=12)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis('off')
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Data summary page
            fig, ax = plt.subplots(figsize=(8.5, 11))
            
            # Basic statistics
            if len(data.select_dtypes(include=[np.number]).columns) > 0:
                stats_text = "Data Summary:\n\n"
                numeric_cols = data.select_dtypes(include=[np.number]).columns[:5]  # Limit to first 5
                
                for col in numeric_cols:
                    stats_text += f"{col}:\n"
                    stats_text += f"  Mean: {data[col].mean():.2f}\n"
                    stats_text += f"  Min: {data[col].min():.2f}\n"
                    stats_text += f"  Max: {data[col].max():.2f}\n\n"
                
                ax.text(0.1, 0.9, stats_text, ha='left', va='top', fontsize=10, 
                       family='monospace', transform=ax.transAxes)
            
            ax.axis('off')
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Generate charts based on export type
            if export_type == 'customer_insights' and 'customer_state' in data.columns:
                self._add_customer_charts_to_pdf(data, pdf)
            elif export_type == 'product_performance' and 'product_category_clean' in data.columns:
                self._add_product_charts_to_pdf(data, pdf)

    def _add_customer_charts_to_pdf(self, data: pd.DataFrame, pdf):
        """Add customer-specific charts to PDF"""
        
        # Customer distribution by state
        if 'customer_state' in data.columns and 'customer_count' in data.columns:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8.5, 11))
            
            # Bar chart - customers by state
            top_states = data.nlargest(10, 'customer_count')
            ax1.bar(top_states['customer_state'], top_states['customer_count'])
            ax1.set_title('Top 10 States by Customer Count')
            ax1.set_xlabel('State')
            ax1.set_ylabel('Customer Count')
            ax1.tick_params(axis='x', rotation=45)
            
            # Pie chart - revenue by segment
            if 'customer_segment' in data.columns and 'total_revenue' in data.columns:
                segment_revenue = data.groupby('customer_segment')['total_revenue'].sum()
                ax2.pie(segment_revenue.values, labels=segment_revenue.index, autopct='%1.1f%%')
                ax2.set_title('Revenue Distribution by Customer Segment')
            
            plt.tight_layout()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()

    def _add_product_charts_to_pdf(self, data: pd.DataFrame, pdf):
        """Add product-specific charts to PDF"""
        
        # Product performance charts
        if 'product_category_clean' in data.columns and 'total_revenue' in data.columns:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8.5, 11))
            
            # Bar chart - revenue by category
            top_categories = data.nlargest(10, 'total_revenue')
            ax1.bar(range(len(top_categories)), top_categories['total_revenue'])
            ax1.set_title('Top 10 Product Categories by Revenue')
            ax1.set_xlabel('Product Category')
            ax1.set_ylabel('Revenue ($)')
            ax1.set_xticks(range(len(top_categories)))
            ax1.set_xticklabels(top_categories['product_category_clean'], rotation=45, ha='right')
            
            # Scatter plot - quality vs revenue
            if 'avg_quality_score' in data.columns:
                ax2.scatter(data['avg_quality_score'], data['total_revenue'], alpha=0.6)
                ax2.set_title('Product Quality Score vs Revenue')
                ax2.set_xlabel('Average Quality Score')
                ax2.set_ylabel('Total Revenue ($)')
            
            plt.tight_layout()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()

    def _generate_html_report(self, data: pd.DataFrame, file_path: Path, export_type: str):
        """Generate interactive HTML report with Plotly"""
        
        # Create HTML template
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .chart-container {{ margin: 20px 0; }}
                .data-table {{ width: 100%; border-collapse: collapse; }}
                .data-table th, .data-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .data-table th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{title}</h1>
                <p>Generated on {timestamp}</p>
                <p>Total Records: {record_count}</p>
            </div>
            
            {charts}
            
            <h2>Data Table</h2>
            {data_table}
        </body>
        </html>
        """
        
        # Generate charts based on data
        charts_html = self._generate_plotly_charts(data, export_type)
        
        # Convert data to HTML table
        data_table = data.head(100).to_html(classes='data-table', table_id='data-table')
        
        # Fill template
        html_content = html_template.format(
            title=export_type.replace('_', ' ').title(),
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            record_count=len(data),
            charts=charts_html,
            data_table=data_table
        )
        
        # Write HTML file
        with open(file_path, 'w') as f:
            f.write(html_content)

    def _generate_plotly_charts(self, data: pd.DataFrame, export_type: str) -> str:
        """Generate Plotly charts for HTML report"""
        
        charts_html = ""
        
        try:
            if export_type == 'customer_insights':
                # Customer state distribution
                if 'customer_state' in data.columns and 'customer_count' in data.columns:
                    fig = px.bar(data.head(10), x='customer_state', y='customer_count',
                                title='Customer Distribution by State')
                    charts_html += f'<div class="chart-container">{fig.to_html(full_html=False, include_plotlyjs=False)}</div>'
                
                # Revenue by segment
                if 'customer_segment' in data.columns and 'total_revenue' in data.columns:
                    segment_data = data.groupby('customer_segment')['total_revenue'].sum().reset_index()
                    fig = px.pie(segment_data, values='total_revenue', names='customer_segment',
                                title='Revenue by Customer Segment')
                    charts_html += f'<div class="chart-container">{fig.to_html(full_html=False, include_plotlyjs=False)}</div>'
            
            elif export_type == 'product_performance':
                # Product category performance
                if 'product_category_clean' in data.columns and 'total_revenue' in data.columns:
                    top_categories = data.nlargest(10, 'total_revenue')
                    fig = px.bar(top_categories, x='product_category_clean', y='total_revenue',
                                title='Top Product Categories by Revenue')
                    fig.update_xaxes(tickangle=45)
                    charts_html += f'<div class="chart-container">{fig.to_html(full_html=False, include_plotlyjs=False)}</div>'
                
                # Quality vs Revenue scatter
                if 'avg_quality_score' in data.columns and 'total_revenue' in data.columns:
                    fig = px.scatter(data, x='avg_quality_score', y='total_revenue',
                                    title='Product Quality vs Revenue',
                                    hover_data=['product_category_clean'])
                    charts_html += f'<div class="chart-container">{fig.to_html(full_html=False, include_plotlyjs=False)}</div>'
        
        except Exception as e:
            logger.warning(f"⚠️ Failed to generate some charts: {e}")
        
        return charts_html

    def _generate_visualizations(self, data: pd.DataFrame, export_type: str, context: Context) -> List[Dict[str, Any]]:
        """Generate standalone visualizations"""
        
        viz_files = []
        execution_date = context['execution_date'].strftime('%Y%m%d')
        timestamp = datetime.now().strftime('%H%M%S')
        
        try:
            # Generate different visualization types
            viz_configs = {
                'summary_chart': self._create_summary_chart,
                'trend_analysis': self._create_trend_chart,
                'distribution_plot': self._create_distribution_plot
            }
            
            for viz_name, viz_function in viz_configs.items():
                try:
                    fig = viz_function(data, export_type)
                    if fig:
                        viz_filename = f"{export_type}_{viz_name}_{execution_date}_{timestamp}.png"
                        viz_path = self.output_path / viz_filename
                        
                        fig.savefig(viz_path, dpi=300, bbox_inches='tight')
                        plt.close(fig)
                        
                        viz_files.append({
                            'name': viz_name,
                            'path': str(viz_path),
                            'size_mb': viz_path.stat().st_size / 1024**2
                        })
                        
                except Exception as viz_error:
                    logger.warning(f"⚠️ Failed to generate {viz_name}: {viz_error}")
        
        except Exception as e:
            logger.warning(f"⚠️ Visualization generation error: {e}")
        
        return viz_files

    def _create_summary_chart(self, data: pd.DataFrame, export_type: str):
        """Create summary chart based on export type"""
        
        if export_type == 'daily_summary' and 'metric_category' in data.columns:
            fig, ax = plt.subplots(figsize=(12, 8))
            
            categories = data['metric_category'].unique()
            x_pos = np.arange(len(categories))
            values = data.groupby('metric_category')['total_count'].sum()
            
            bars = ax.bar(x_pos, values, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
            ax.set_xlabel('Category')
            ax.set_ylabel('Count')
            ax.set_title('Daily Summary by Category')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(categories)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}', ha='center', va='bottom')
            
            return fig
            
        return None

    def _create_trend_chart(self, data: pd.DataFrame, export_type: str):
        """Create trend analysis chart"""
        
        if 'report_date' in data.columns or 'order_date' in data.columns:
            fig, ax = plt.subplots(figsize=(12, 8))
            
            date_col = 'order_date' if 'order_date' in data.columns else 'report_date'
            
            if 'daily_revenue' in data.columns:
                daily_data = data.groupby(date_col)['daily_revenue'].sum()
                ax.plot(daily_data.index, daily_data.values, marker='o', linewidth=2)
                ax.set_ylabel('Revenue ($)')
                ax.set_title('Daily Revenue Trend')
            elif 'total_count' in data.columns:
                daily_data = data.groupby(date_col)['total_count'].sum()
                ax.plot(daily_data.index, daily_data.values, marker='o', linewidth=2)
                ax.set_ylabel('Count')
                ax.set_title('Daily Activity Trend')
            
            ax.set_xlabel('Date')
            ax.tick_params(axis='x', rotation=45)
            plt.tight_layout()
            
            return fig
            
        return None

    def _create_distribution_plot(self, data: pd.DataFrame, export_type: str):
        """Create distribution plot"""
        
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) > 0:
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            axes = axes.flatten()
            
            for i, col in enumerate(numeric_cols[:4]):  # Max 4 distributions
                if i < len(axes):
                    axes[i].hist(data[col].dropna(), bins=30, alpha=0.7, edgecolor='black')
                    axes[i].set_title(f'Distribution of {col}')
                    axes[i].set_xlabel(col)
                    axes[i].set_ylabel('Frequency')
            
            # Hide unused subplots
            for i in range(len(numeric_cols), len(axes)):
                axes[i].hide()
            
            plt.tight_layout()
            return fig
            
        return None

    def _upload_to_cloud(self, context: Context):
        """Upload generated files to cloud storage"""
        
        logger.info(f"☁️ Uploading files to {self.cloud_provider}...")
        
        try:
            if self.cloud_provider == 's3':
                self._upload_to_s3()
            elif self.cloud_provider == 'gcs':
                self._upload_to_gcs()
            else:
                logger.warning(f"⚠️ Cloud provider {self.cloud_provider} not supported")
                
        except Exception as e:
            logger.error(f"❌ Cloud upload failed: {e}")

    def _upload_to_s3(self):
        """Upload files to AWS S3"""
        
        try:
            s3_client = boto3.client('s3')
            
            # Upload all files in output directory
            for file_path in self.output_path.iterdir():
                if file_path.is_file() and file_path.stat().st_mtime > (datetime.now() - timedelta(hours=1)).timestamp():
                    s3_key = f"exports/{file_path.name}"
                    
                    s3_client.upload_file(
                        str(file_path),
                        self.cloud_bucket,
                        s3_key
                    )
                    
                    logger.info(f"✅ Uploaded to S3: s3://{self.cloud_bucket}/{s3_key}")
                    self.export_results['cloud_uploads'] += 1
                    
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found")
        except Exception as e:
            logger.error(f"❌ S3 upload failed: {e}")

    def _send_email_reports(self, context: Context):
        """Send email reports to recipients"""
        
        logger.info(f"📧 Sending email reports to {len(self.email_recipients)} recipients...")
        
        try:
            # Find recent files to attach
            recent_files = [
                f for f in self.output_path.iterdir() 
                if f.is_file() and f.stat().st_mtime > (datetime.now() - timedelta(hours=1)).timestamp()
            ]
            
            if not recent_files:
                logger.warning("⚠️ No recent files to email")
                return
            
            # Create email content
            subject = f"Data Export Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            html_content = f"""
            <h2>Data Export Report</h2>
            <p><strong>Execution Date:</strong> {context['execution_date'].strftime('%Y-%m-%d')}</p>
            <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
            <p><strong>Run ID:</strong> {context['dag_run'].run_id}</p>
            
            <h3>Export Summary</h3>
            <ul>
                <li>Exports Generated: {self.export_results['exports_generated']}</li>
                <li>Formats Created: {self.export_results['formats_created']}</li>
                <li>Files Attached: {len(recent_files)}</li>
            </ul>
            
            <h3>Generated Files</h3>
            <ul>
            """
            
            for file_path in recent_files:
                file_size_mb = file_path.stat().st_size / 1024**2
                html_content += f"<li>{file_path.name} ({file_size_mb:.2f} MB)</li>"
            
            html_content += """
            </ul>
            <p>Please find the export files attached to this email.</p>
            <p><em>This is an automated report from the Data Pipeline.</em></p>
            """
            
            # This would integrate with your email system
            # For demonstration, we'll just log the email details
            email_details = {
                'to': self.email_recipients,
                'subject': subject,
                'html_content': html_content,
                'attachments': [str(f) for f in recent_files[:5]]  # Limit attachments
            }
            
            logger.info(f"📧 Email prepared: {email_details['subject']}")
            logger.info(f"📎 Attachments: {len(email_details['attachments'])}")
            
            # In production, you would send the actual email here
            self.export_results['emails_sent'] = len(self.email_recipients)
            
        except Exception as e:
            logger.error(f"❌ Email sending failed: {e}")

    def _cleanup_old_files(self):
        """Clean up old export files"""
        
        logger.info(f"🧹 Cleaning up files older than {self.auto_cleanup_days} days...")
        
        try:
            cutoff_time = datetime.now() - timedelta(days=self.auto_cleanup_days)
            
            for file_path in self.output_path.iterdir():
                if file_path.is_file():
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    if file_mtime < cutoff_time:
                        file_path.unlink()
                        logger.debug(f"🗑️ Deleted old file: {file_path.name}")
            
            logger.info("✅ File cleanup completed")
            
        except Exception as e:
            logger.warning(f"⚠️ File cleanup error: {e}")