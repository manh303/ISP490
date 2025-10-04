#!/usr/bin/env python3
"""
E-commerce DSS Data Flow Model
Mô hình thể hiện đầu vào, xử lí và đầu ra của dữ liệu
"""

import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, ConnectionPatch
import numpy as np
from datetime import datetime
import json
import pandas as pd
from pathlib import Path

class EcommerceDataFlowModel:
    def __init__(self):
        self.fig, self.ax = plt.subplots(1, 1, figsize=(16, 12))
        self.ax.set_xlim(0, 16)
        self.ax.set_ylim(0, 12)
        self.ax.axis('off')

        # Colors for different stages
        self.colors = {
            'input': '#E8F4FD',      # Light blue
            'processing': '#FFF2CC',  # Light yellow
            'output': '#E1F5FE',     # Light cyan
            'storage': '#F3E5F5',    # Light purple
            'analytics': '#E8F5E8'   # Light green
        }

    def create_data_sources_section(self):
        """Tạo phần Data Sources (Đầu vào)"""
        # Title
        self.ax.text(2, 11.5, 'NGUỒN DỮ LIỆU (INPUT)', fontsize=14, fontweight='bold',
                    ha='center', color='#1976D2')

        # API Sources
        api_box = FancyBboxPatch((0.5, 9.5), 3, 1.5, boxstyle="round,pad=0.1",
                                facecolor=self.colors['input'], edgecolor='#1976D2', linewidth=2)
        self.ax.add_patch(api_box)
        self.ax.text(2, 10.6, 'API SOURCES', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(2, 10.3, '• FakeStore API', fontsize=8, ha='center')
        self.ax.text(2, 10.1, '• DummyJSON Store', fontsize=8, ha='center')
        self.ax.text(2, 9.9, '• Platzi API', fontsize=8, ha='center')
        self.ax.text(2, 9.7, '~600 products/day', fontsize=7, ha='center', style='italic')

        # Web Scraping
        scraping_box = FancyBboxPatch((0.5, 7.8), 3, 1.5, boxstyle="round,pad=0.1",
                                     facecolor=self.colors['input'], edgecolor='#1976D2', linewidth=2)
        self.ax.add_patch(scraping_box)
        self.ax.text(2, 8.9, 'WEB SCRAPING', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(2, 8.6, '• Sendo.vn', fontsize=8, ha='center')
        self.ax.text(2, 8.4, '• FPTShop.com.vn', fontsize=8, ha='center')
        self.ax.text(2, 8.2, '• ChotOt.com', fontsize=8, ha='center')
        self.ax.text(2, 8.0, '~450 products/day', fontsize=7, ha='center', style='italic')

        # Synthetic Data
        synthetic_box = FancyBboxPatch((0.5, 6.1), 3, 1.5, boxstyle="round,pad=0.1",
                                      facecolor=self.colors['input'], edgecolor='#1976D2', linewidth=2)
        self.ax.add_patch(synthetic_box)
        self.ax.text(2, 7.2, 'SYNTHETIC DATA', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(2, 6.9, '• User Behavior Sim', fontsize=8, ha='center')
        self.ax.text(2, 6.7, '• Transaction Sim', fontsize=8, ha='center')
        self.ax.text(2, 6.5, '• Market Changes', fontsize=8, ha='center')
        self.ax.text(2, 6.3, '~1000 events/min', fontsize=7, ha='center', style='italic')

        # Shopee (Limited)
        shopee_box = FancyBboxPatch((0.5, 4.4), 3, 1.5, boxstyle="round,pad=0.1",
                                   facecolor='#FFEBEE', edgecolor='#D32F2F', linewidth=2)
        self.ax.add_patch(shopee_box)
        self.ax.text(2, 5.5, 'SHOPEE (LIMITED)', fontsize=10, fontweight='bold', ha='center', color='#D32F2F')
        self.ax.text(2, 5.2, '• Anti-bot protection', fontsize=8, ha='center')
        self.ax.text(2, 5.0, '• SPA architecture', fontsize=8, ha='center')
        self.ax.text(2, 4.8, '• Synthetic fallback', fontsize=8, ha='center')
        self.ax.text(2, 4.6, '~100 synthetic/day', fontsize=7, ha='center', style='italic')

    def create_processing_section(self):
        """Tạo phần Data Processing (Xử lí)"""
        # Title
        self.ax.text(8, 11.5, 'XỬ LÍ DỮ LIỆU (PROCESSING)', fontsize=14, fontweight='bold',
                    ha='center', color='#F57C00')

        # Real-time Processing
        realtime_box = FancyBboxPatch((6, 9.5), 4, 1.8, boxstyle="round,pad=0.1",
                                     facecolor=self.colors['processing'], edgecolor='#F57C00', linewidth=2)
        self.ax.add_patch(realtime_box)
        self.ax.text(8, 10.8, 'REAL-TIME PROCESSING', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(8, 10.5, '• Kafka Streaming', fontsize=8, ha='center')
        self.ax.text(8, 10.3, '• Apache Spark', fontsize=8, ha='center')
        self.ax.text(8, 10.1, '• Data Validation', fontsize=8, ha='center')
        self.ax.text(8, 9.9, '• Quality Checks', fontsize=8, ha='center')
        self.ax.text(8, 9.7, '<5 min latency', fontsize=7, ha='center', style='italic')

        # Batch Processing
        batch_box = FancyBboxPatch((6, 7.4), 4, 1.8, boxstyle="round,pad=0.1",
                                  facecolor=self.colors['processing'], edgecolor='#F57C00', linewidth=2)
        self.ax.add_patch(batch_box)
        self.ax.text(8, 8.7, 'BATCH PROCESSING', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(8, 8.4, '• Airflow DAGs', fontsize=8, ha='center')
        self.ax.text(8, 8.2, '• ETL Pipelines', fontsize=8, ha='center')
        self.ax.text(8, 8.0, '• Data Transformation', fontsize=8, ha='center')
        self.ax.text(8, 7.8, '• Aggregation', fontsize=8, ha='center')
        self.ax.text(8, 7.6, 'Daily/Hourly jobs', fontsize=7, ha='center', style='italic')

        # ML Processing
        ml_box = FancyBboxPatch((6, 5.3), 4, 1.8, boxstyle="round,pad=0.1",
                               facecolor=self.colors['processing'], edgecolor='#F57C00', linewidth=2)
        self.ax.add_patch(ml_box)
        self.ax.text(8, 6.6, 'ML PROCESSING', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(8, 6.3, '• Feature Engineering', fontsize=8, ha='center')
        self.ax.text(8, 6.1, '• Model Training', fontsize=8, ha='center')
        self.ax.text(8, 5.9, '• Prediction Pipeline', fontsize=8, ha='center')
        self.ax.text(8, 5.7, '• Model Validation', fontsize=8, ha='center')
        self.ax.text(8, 5.5, 'Weekly retrain', fontsize=7, ha='center', style='italic')

    def create_storage_section(self):
        """Tạo phần Data Storage (Lưu trữ)"""
        # Raw Data Storage
        raw_box = FancyBboxPatch((4.5, 3.2), 3, 1.5, boxstyle="round,pad=0.1",
                                facecolor=self.colors['storage'], edgecolor='#7B1FA2', linewidth=2)
        self.ax.add_patch(raw_box)
        self.ax.text(6, 4.3, 'RAW DATA', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(6, 4.0, '• MongoDB', fontsize=8, ha='center')
        self.ax.text(6, 3.8, '• Data Lake', fontsize=8, ha='center')
        self.ax.text(6, 3.6, '• JSON/CSV Files', fontsize=8, ha='center')
        self.ax.text(6, 3.4, '2.76M+ records', fontsize=7, ha='center', style='italic')

        # Processed Data Storage
        processed_box = FancyBboxPatch((8.5, 3.2), 3, 1.5, boxstyle="round,pad=0.1",
                                      facecolor=self.colors['storage'], edgecolor='#7B1FA2', linewidth=2)
        self.ax.add_patch(processed_box)
        self.ax.text(10, 4.3, 'PROCESSED DATA', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(10, 4.0, '• PostgreSQL', fontsize=8, ha='center')
        self.ax.text(10, 3.8, '• Data Warehouse', fontsize=8, ha='center')
        self.ax.text(10, 3.6, '• Feature Store', fontsize=8, ha='center')
        self.ax.text(10, 3.4, 'Aggregated metrics', fontsize=7, ha='center', style='italic')

    def create_output_section(self):
        """Tạo phần Data Output (Đầu ra)"""
        # Title
        self.ax.text(14, 11.5, 'ĐẦU RA (OUTPUT)', fontsize=14, fontweight='bold',
                    ha='center', color='#388E3C')

        # Dashboards
        dashboard_box = FancyBboxPatch((12.5, 9.5), 3, 1.5, boxstyle="round,pad=0.1",
                                      facecolor=self.colors['analytics'], edgecolor='#388E3C', linewidth=2)
        self.ax.add_patch(dashboard_box)
        self.ax.text(14, 10.6, 'DASHBOARDS', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(14, 10.3, '• Grafana Monitoring', fontsize=8, ha='center')
        self.ax.text(14, 10.1, '• Sales Analytics', fontsize=8, ha='center')
        self.ax.text(14, 9.9, '• Market Insights', fontsize=8, ha='center')
        self.ax.text(14, 9.7, 'Real-time updates', fontsize=7, ha='center', style='italic')

        # Reports
        reports_box = FancyBboxPatch((12.5, 7.8), 3, 1.5, boxstyle="round,pad=0.1",
                                    facecolor=self.colors['analytics'], edgecolor='#388E3C', linewidth=2)
        self.ax.add_patch(reports_box)
        self.ax.text(14, 8.9, 'REPORTS', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(14, 8.6, '• Business Reports', fontsize=8, ha='center')
        self.ax.text(14, 8.4, '• Performance KPIs', fontsize=8, ha='center')
        self.ax.text(14, 8.2, '• Trend Analysis', fontsize=8, ha='center')
        self.ax.text(14, 8.0, 'Daily/Weekly', fontsize=7, ha='center', style='italic')

        # ML Models Output
        ml_output_box = FancyBboxPatch((12.5, 6.1), 3, 1.5, boxstyle="round,pad=0.1",
                                      facecolor=self.colors['analytics'], edgecolor='#388E3C', linewidth=2)
        self.ax.add_patch(ml_output_box)
        self.ax.text(14, 7.2, 'ML PREDICTIONS', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(14, 6.9, '• Price Predictions', fontsize=8, ha='center')
        self.ax.text(14, 6.7, '• Demand Forecasting', fontsize=8, ha='center')
        self.ax.text(14, 6.5, '• Customer Insights', fontsize=8, ha='center')
        self.ax.text(14, 6.3, 'Real-time inference', fontsize=7, ha='center', style='italic')

        # Alerts & Notifications
        alerts_box = FancyBboxPatch((12.5, 4.4), 3, 1.5, boxstyle="round,pad=0.1",
                                   facecolor=self.colors['analytics'], edgecolor='#388E3C', linewidth=2)
        self.ax.add_patch(alerts_box)
        self.ax.text(14, 5.5, 'ALERTS & API', fontsize=10, fontweight='bold', ha='center')
        self.ax.text(14, 5.2, '• System Alerts', fontsize=8, ha='center')
        self.ax.text(14, 5.0, '• REST API Endpoints', fontsize=8, ha='center')
        self.ax.text(14, 4.8, '• WebSocket Streams', fontsize=8, ha='center')
        self.ax.text(14, 4.6, '24/7 monitoring', fontsize=7, ha='center', style='italic')

    def create_data_flow_arrows(self):
        """Tạo các mũi tên thể hiện luồng dữ liệu"""
        # Input to Processing arrows
        arrow1 = ConnectionPatch((3.5, 8.5), (6, 8.5), "data", "data",
                               arrowstyle="->", shrinkA=5, shrinkB=5,
                               mutation_scale=20, fc="red", color='#FF6B35')
        self.ax.add_patch(arrow1)

        # Processing to Storage arrows
        arrow2 = ConnectionPatch((8, 7.4), (6.5, 4.7), "data", "data",
                               arrowstyle="->", shrinkA=5, shrinkB=5,
                               mutation_scale=20, fc="blue", color='#1976D2')

        arrow3 = ConnectionPatch((8, 7.4), (9.5, 4.7), "data", "data",
                               arrowstyle="->", shrinkA=5, shrinkB=5,
                               mutation_scale=20, fc="blue", color='#1976D2')

        self.ax.add_patch(arrow2)
        self.ax.add_patch(arrow3)

        # Storage to Output arrows
        arrow4 = ConnectionPatch((10.5, 4), (12.5, 8.5), "data", "data",
                               arrowstyle="->", shrinkA=5, shrinkB=5,
                               mutation_scale=20, fc="green", color='#388E3C')
        self.ax.add_patch(arrow4)

    def add_data_volume_indicators(self):
        """Thêm các chỉ số về khối lượng dữ liệu"""
        # Volume indicators
        self.ax.text(4.5, 2.5, 'DATA VOLUMES & METRICS', fontsize=12, fontweight='bold',
                    ha='center', color='#333')

        # Metrics table
        metrics = [
            'Daily Collection: ~2,000 products',
            'Real-time Events: ~1,000/minute',
            'Storage: 2.76M+ records',
            'Processing Latency: <5 minutes',
            'Success Rate: 87.5%',
            'Data Quality: 95%+ accuracy'
        ]

        for i, metric in enumerate(metrics):
            self.ax.text(1, 2.2 - i*0.25, f'• {metric}', fontsize=9, ha='left',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='#F5F5F5', alpha=0.8))

    def add_title_and_legend(self):
        """Thêm title và legend"""
        # Main title
        self.ax.text(8, 0.8, 'E-COMMERCE DSS DATA FLOW MODEL',
                    fontsize=16, fontweight='bold', ha='center')
        self.ax.text(8, 0.5, 'Input → Processing → Storage → Output',
                    fontsize=12, ha='center', style='italic')

        # Legend
        legend_elements = [
            mpatches.Patch(color=self.colors['input'], label='Data Input Sources'),
            mpatches.Patch(color=self.colors['processing'], label='Data Processing'),
            mpatches.Patch(color=self.colors['storage'], label='Data Storage'),
            mpatches.Patch(color=self.colors['analytics'], label='Data Output & Analytics'),
            mpatches.Patch(color='#FFEBEE', label='Limited/Restricted Sources')
        ]

        self.ax.legend(handles=legend_elements, loc='upper right',
                      bbox_to_anchor=(0.98, 0.98), fontsize=8)

    def generate_model(self):
        """Tạo toàn bộ model"""
        print("Generating E-commerce DSS Data Flow Model...")

        # Create all sections
        self.create_data_sources_section()
        self.create_processing_section()
        self.create_storage_section()
        self.create_output_section()

        # Add arrows and indicators
        self.create_data_flow_arrows()
        self.add_data_volume_indicators()
        self.add_title_and_legend()

        # Save the model
        output_dir = Path(__file__).parent.parent / 'docs' / 'architecture'
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = output_dir / f'data_flow_model_{timestamp}.png'

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight',
                   facecolor='white', edgecolor='none')

        print(f"Data Flow Model saved: {output_file}")

        # Also save as PDF
        pdf_file = output_dir / f'data_flow_model_{timestamp}.pdf'
        plt.savefig(pdf_file, dpi=300, bbox_inches='tight',
                   facecolor='white', edgecolor='none')

        print(f"PDF version saved: {pdf_file}")

        return output_file, pdf_file

    def generate_detailed_specs(self):
        """Tạo specifications chi tiết"""
        specs = {
            "data_flow_model": {
                "input_sources": {
                    "api_sources": {
                        "fakestore_api": {
                            "url": "https://fakestoreapi.com",
                            "daily_volume": "~200 products",
                            "data_types": ["products", "categories", "users", "carts"],
                            "status": "active"
                        },
                        "dummyjson_store": {
                            "url": "https://dummyjson.com",
                            "daily_volume": "~300 products",
                            "data_types": ["products", "reviews", "users"],
                            "status": "active"
                        },
                        "platzi_api": {
                            "url": "https://api.escuelajs.co/api/v1",
                            "daily_volume": "~100 products",
                            "data_types": ["products", "categories"],
                            "status": "active"
                        }
                    },
                    "web_scraping": {
                        "sendo_vn": {
                            "url": "https://www.sendo.vn",
                            "daily_volume": "~150 products",
                            "target_pages": ["/danh-muc", "/tim-kiem", "/san-pham"],
                            "status": "active"
                        },
                        "fptshop_vn": {
                            "url": "https://fptshop.com.vn",
                            "daily_volume": "~150 products",
                            "target_pages": ["/may-tinh", "/dien-thoai", "/may-anh"],
                            "status": "active"
                        },
                        "chotot_com": {
                            "url": "https://www.chotot.com",
                            "daily_volume": "~50 products",
                            "target_pages": ["/mua-ban", "/tim-kiem"],
                            "status": "active"
                        },
                        "shopee_vn": {
                            "url": "https://shopee.vn",
                            "daily_volume": "~100 synthetic products",
                            "limitations": [
                                "SPA architecture requires JavaScript",
                                "Anti-bot protection active",
                                "Robots.txt blocks product pages",
                                "Dynamic content loading"
                            ],
                            "status": "limited"
                        }
                    },
                    "synthetic_data": {
                        "user_behavior": {
                            "volume": "1000 events/minute",
                            "types": ["page_views", "clicks", "searches", "purchases"]
                        },
                        "transactions": {
                            "volume": "500 transactions/hour",
                            "types": ["orders", "payments", "shipments", "returns"]
                        },
                        "market_changes": {
                            "volume": "200 updates/minute",
                            "types": ["price_updates", "inventory_changes", "promotions"]
                        }
                    }
                },
                "processing_pipeline": {
                    "real_time": {
                        "kafka_topics": ["user_events", "transaction_stream", "price_changes"],
                        "spark_streaming": "Apache Spark 3.x",
                        "latency": "<5 minutes",
                        "throughput": "1000+ events/minute"
                    },
                    "batch_processing": {
                        "airflow_dags": ["dss_etl_dag", "comprehensive_ecommerce_dss_dag"],
                        "schedule": "Daily/Hourly",
                        "etl_jobs": ["data_validation", "transformation", "aggregation"]
                    },
                    "ml_pipeline": {
                        "feature_engineering": "Automated",
                        "model_training": "Weekly retraining",
                        "inference": "Real-time predictions",
                        "validation": "Cross-validation + holdout"
                    }
                },
                "storage_systems": {
                    "raw_data": {
                        "mongodb": {
                            "collections": ["data_lake", "product_catalog", "customer_profiles"],
                            "volume": "2.76M+ records"
                        },
                        "file_storage": {
                            "formats": ["CSV", "JSON", "Parquet"],
                            "location": "/data/raw/"
                        }
                    },
                    "processed_data": {
                        "postgresql": {
                            "tables": ["analytics_warehouse", "metrics_summary", "feature_store"],
                            "optimization": "Indexed for analytics queries"
                        }
                    }
                },
                "output_systems": {
                    "dashboards": {
                        "grafana": {
                            "update_frequency": "Real-time",
                            "dashboard_types": ["Sales Analytics", "Market Insights", "System Monitoring"]
                        }
                    },
                    "reports": {
                        "business_reports": "Daily/Weekly/Monthly",
                        "performance_kpis": "Real-time metrics",
                        "trend_analysis": "Historical patterns"
                    },
                    "ml_outputs": {
                        "price_predictions": "Hourly updates",
                        "demand_forecasting": "Daily forecasts",
                        "customer_insights": "Real-time scoring"
                    },
                    "apis": {
                        "rest_endpoints": "FastAPI backend",
                        "websocket_streams": "Real-time data feeds",
                        "alerts": "24/7 system monitoring"
                    }
                }
            },
            "quality_metrics": {
                "data_completeness": "95%",
                "data_accuracy": "98%",
                "data_consistency": "97%",
                "processing_success_rate": "87.5%",
                "system_uptime": "99.5%"
            }
        }

        # Save specifications
        specs_dir = Path(__file__).parent.parent / 'docs' / 'architecture'
        specs_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        specs_file = specs_dir / f'data_flow_specifications_{timestamp}.json'

        with open(specs_file, 'w', encoding='utf-8') as f:
            json.dump(specs, f, indent=2, ensure_ascii=False)

        print(f"Data Flow Specifications saved: {specs_file}")

        return specs_file

def main():
    """Main execution function"""
    print("Starting E-commerce DSS Data Flow Model Generation")

    # Create model
    model = EcommerceDataFlowModel()

    # Generate visual model
    image_file, pdf_file = model.generate_model()

    # Generate detailed specifications
    specs_file = model.generate_detailed_specs()

    print("\nData Flow Model Generation Complete!")
    print(f"   - Visual Model (PNG): {image_file}")
    print(f"   - Visual Model (PDF): {pdf_file}")
    print(f"   - Specifications (JSON): {specs_file}")

    # Close plot to prevent GUI display
    plt.close()

if __name__ == "__main__":
    main()