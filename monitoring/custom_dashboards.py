#!/usr/bin/env python3
"""
Custom Monitoring Dashboards Generator
Creates Grafana dashboards and Prometheus alert rules
"""

import json
import yaml
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

class GrafanaDashboardGenerator:
    """Generate custom Grafana dashboards for E-commerce DSS"""

    def __init__(self):
        self.dashboard_templates = {}

    def create_system_overview_dashboard(self) -> Dict[str, Any]:
        """Create system overview dashboard"""
        return {
            "dashboard": {
                "id": None,
                "title": "E-commerce DSS - System Overview",
                "tags": ["ecommerce", "dss", "overview"],
                "timezone": "browser",
                "panels": [
                    {
                        "id": 1,
                        "title": "System Health Overview",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "up{job=\"backend\"}",
                                "legendFormat": "Backend Status"
                            },
                            {
                                "expr": "up{job=\"kafka\"}",
                                "legendFormat": "Kafka Status"
                            },
                            {
                                "expr": "up{job=\"spark\"}",
                                "legendFormat": "Spark Status"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Request Rate",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "rate(api_requests_total[5m])",
                                "legendFormat": "Requests/sec"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
                    },
                    {
                        "id": 3,
                        "title": "Response Time",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "histogram_quantile(0.95, rate(api_request_duration_seconds_bucket[5m]))",
                                "legendFormat": "95th percentile"
                            },
                            {
                                "expr": "histogram_quantile(0.50, rate(api_request_duration_seconds_bucket[5m]))",
                                "legendFormat": "50th percentile"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
                    },
                    {
                        "id": 4,
                        "title": "Error Rate",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "rate(api_requests_total{status=~\"4..|5..\"}[5m]) / rate(api_requests_total[5m]) * 100",
                                "legendFormat": "Error Rate %"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
                    }
                ],
                "time": {"from": "now-1h", "to": "now"},
                "refresh": "30s"
            }
        }

    def create_data_pipeline_dashboard(self) -> Dict[str, Any]:
        """Create data pipeline monitoring dashboard"""
        return {
            "dashboard": {
                "id": None,
                "title": "E-commerce DSS - Data Pipeline",
                "tags": ["ecommerce", "dss", "pipeline"],
                "panels": [
                    {
                        "id": 1,
                        "title": "Kafka Topics - Message Rate",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "rate(kafka_topic_partition_current_offset[5m])",
                                "legendFormat": "{{topic}} - {{partition}}"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Spark Streaming Jobs",
                        "type": "table",
                        "targets": [
                            {
                                "expr": "spark_streaming_total_delay_seconds",
                                "legendFormat": "Processing Delay"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
                    },
                    {
                        "id": 3,
                        "title": "Database Connections",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "pg_stat_database_numbackends",
                                "legendFormat": "PostgreSQL Connections"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
                    },
                    {
                        "id": 4,
                        "title": "Data Processing Volume",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "streaming_messages_total",
                                "legendFormat": "Total Messages Processed"
                            }
                        ],
                        "gridPos": {"h": 6, "w": 24, "x": 0, "y": 16}
                    }
                ],
                "time": {"from": "now-6h", "to": "now"},
                "refresh": "1m"
            }
        }

    def create_ml_performance_dashboard(self) -> Dict[str, Any]:
        """Create ML performance monitoring dashboard"""
        return {
            "dashboard": {
                "id": None,
                "title": "E-commerce DSS - ML Performance",
                "tags": ["ecommerce", "dss", "ml"],
                "panels": [
                    {
                        "id": 1,
                        "title": "ML Prediction Requests",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "rate(ml_predictions_total[5m])",
                                "legendFormat": "{{model_name}}"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Model Inference Time",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "histogram_quantile(0.95, rate(ml_inference_duration_seconds_bucket[5m]))",
                                "legendFormat": "95th percentile"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
                    },
                    {
                        "id": 3,
                        "title": "Model Accuracy Metrics",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "ml_model_accuracy",
                                "legendFormat": "{{model_name}} Accuracy"
                            }
                        ],
                        "gridPos": {"h": 6, "w": 24, "x": 0, "y": 8}
                    },
                    {
                        "id": 4,
                        "title": "Cache Hit Rate",
                        "type": "gauge",
                        "targets": [
                            {
                                "expr": "cache_hit_rate * 100",
                                "legendFormat": "Cache Hit Rate %"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 14}
                    },
                    {
                        "id": 5,
                        "title": "Redis Memory Usage",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "redis_memory_used_bytes",
                                "legendFormat": "Memory Used"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 14}
                    }
                ],
                "time": {"from": "now-3h", "to": "now"},
                "refresh": "30s"
            }
        }

    def create_business_metrics_dashboard(self) -> Dict[str, Any]:
        """Create business metrics dashboard"""
        return {
            "dashboard": {
                "id": None,
                "title": "E-commerce DSS - Business Metrics",
                "tags": ["ecommerce", "dss", "business"],
                "panels": [
                    {
                        "id": 1,
                        "title": "Total Revenue (24h)",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "sum(revenue_total)",
                                "legendFormat": "Total Revenue"
                            }
                        ],
                        "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Active Customers",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "active_customers_total",
                                "legendFormat": "Active Customers"
                            }
                        ],
                        "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0}
                    },
                    {
                        "id": 3,
                        "title": "Orders Count",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "orders_total",
                                "legendFormat": "Total Orders"
                            }
                        ],
                        "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0}
                    },
                    {
                        "id": 4,
                        "title": "Conversion Rate",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "conversion_rate * 100",
                                "legendFormat": "Conversion Rate %"
                            }
                        ],
                        "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0}
                    },
                    {
                        "id": 5,
                        "title": "Revenue Trend",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": "rate(revenue_total[1h])",
                                "legendFormat": "Revenue/hour"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4}
                    },
                    {
                        "id": 6,
                        "title": "Customer Segments",
                        "type": "piechart",
                        "targets": [
                            {
                                "expr": "customer_segments_total",
                                "legendFormat": "{{segment}}"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4}
                    }
                ],
                "time": {"from": "now-24h", "to": "now"},
                "refresh": "5m"
            }
        }

    def generate_all_dashboards(self) -> Dict[str, Dict[str, Any]]:
        """Generate all dashboards"""
        return {
            "system_overview": self.create_system_overview_dashboard(),
            "data_pipeline": self.create_data_pipeline_dashboard(),
            "ml_performance": self.create_ml_performance_dashboard(),
            "business_metrics": self.create_business_metrics_dashboard()
        }

class PrometheusAlertGenerator:
    """Generate Prometheus alert rules"""

    def __init__(self):
        self.alert_rules = {}

    def create_system_alerts(self) -> Dict[str, Any]:
        """Create system-level alert rules"""
        return {
            "groups": [
                {
                    "name": "ecommerce_dss_system",
                    "rules": [
                        {
                            "alert": "ServiceDown",
                            "expr": "up == 0",
                            "for": "1m",
                            "labels": {
                                "severity": "critical"
                            },
                            "annotations": {
                                "summary": "Service {{ $labels.instance }} is down",
                                "description": "{{ $labels.job }} service on {{ $labels.instance }} has been down for more than 1 minute."
                            }
                        },
                        {
                            "alert": "HighErrorRate",
                            "expr": "rate(api_requests_total{status=~\"5..\"}[5m]) / rate(api_requests_total[5m]) > 0.05",
                            "for": "5m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "High error rate detected",
                                "description": "Error rate is {{ $value | humanizePercentage }} for the last 5 minutes."
                            }
                        },
                        {
                            "alert": "HighResponseTime",
                            "expr": "histogram_quantile(0.95, rate(api_request_duration_seconds_bucket[5m])) > 2",
                            "for": "10m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "High response time detected",
                                "description": "95th percentile response time is {{ $value }}s for the last 10 minutes."
                            }
                        }
                    ]
                }
            ]
        }

    def create_data_pipeline_alerts(self) -> Dict[str, Any]:
        """Create data pipeline alert rules"""
        return {
            "groups": [
                {
                    "name": "ecommerce_dss_pipeline",
                    "rules": [
                        {
                            "alert": "KafkaLagHigh",
                            "expr": "kafka_consumer_lag > 1000",
                            "for": "5m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "Kafka consumer lag is high",
                                "description": "Consumer lag for {{ $labels.topic }} is {{ $value }} messages."
                            }
                        },
                        {
                            "alert": "SparkJobFailed",
                            "expr": "spark_job_failed_total > 0",
                            "for": "1m",
                            "labels": {
                                "severity": "critical"
                            },
                            "annotations": {
                                "summary": "Spark job failed",
                                "description": "Spark job {{ $labels.job_name }} has failed."
                            }
                        },
                        {
                            "alert": "DatabaseConnectionsHigh",
                            "expr": "pg_stat_database_numbackends > 80",
                            "for": "5m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "High database connections",
                                "description": "Database connections count is {{ $value }}."
                            }
                        }
                    ]
                }
            ]
        }

    def create_ml_alerts(self) -> Dict[str, Any]:
        """Create ML-specific alert rules"""
        return {
            "groups": [
                {
                    "name": "ecommerce_dss_ml",
                    "rules": [
                        {
                            "alert": "MLModelAccuracyLow",
                            "expr": "ml_model_accuracy < 0.8",
                            "for": "30m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "ML model accuracy is low",
                                "description": "Model {{ $labels.model_name }} accuracy is {{ $value | humanizePercentage }}."
                            }
                        },
                        {
                            "alert": "MLInferenceTimeHigh",
                            "expr": "histogram_quantile(0.95, rate(ml_inference_duration_seconds_bucket[5m])) > 5",
                            "for": "10m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "ML inference time is high",
                                "description": "95th percentile ML inference time is {{ $value }}s."
                            }
                        },
                        {
                            "alert": "CacheHitRateLow",
                            "expr": "cache_hit_rate < 0.7",
                            "for": "15m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "Cache hit rate is low",
                                "description": "Cache hit rate is {{ $value | humanizePercentage }}."
                            }
                        }
                    ]
                }
            ]
        }

    def create_business_alerts(self) -> Dict[str, Any]:
        """Create business metrics alert rules"""
        return {
            "groups": [
                {
                    "name": "ecommerce_dss_business",
                    "rules": [
                        {
                            "alert": "RevenueDropSignificant",
                            "expr": "rate(revenue_total[1h]) < rate(revenue_total[1h] offset 24h) * 0.7",
                            "for": "30m",
                            "labels": {
                                "severity": "critical"
                            },
                            "annotations": {
                                "summary": "Significant revenue drop detected",
                                "description": "Revenue rate has dropped by more than 30% compared to the same time yesterday."
                            }
                        },
                        {
                            "alert": "ConversionRateLow",
                            "expr": "conversion_rate < 0.02",
                            "for": "1h",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "Conversion rate is low",
                                "description": "Conversion rate is {{ $value | humanizePercentage }}."
                            }
                        },
                        {
                            "alert": "HighChurnRisk",
                            "expr": "avg(churn_prediction_score) > 0.7",
                            "for": "30m",
                            "labels": {
                                "severity": "warning"
                            },
                            "annotations": {
                                "summary": "High customer churn risk",
                                "description": "Average churn prediction score is {{ $value | humanizePercentage }}."
                            }
                        }
                    ]
                }
            ]
        }

    def generate_all_alerts(self) -> Dict[str, Dict[str, Any]]:
        """Generate all alert rules"""
        return {
            "system_alerts": self.create_system_alerts(),
            "pipeline_alerts": self.create_data_pipeline_alerts(),
            "ml_alerts": self.create_ml_alerts(),
            "business_alerts": self.create_business_alerts()
        }

class MonitoringSetupManager:
    """Manage monitoring setup and configuration"""

    def __init__(self, output_dir: str = "./monitoring/generated"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.dashboard_generator = GrafanaDashboardGenerator()
        self.alert_generator = PrometheusAlertGenerator()

    def generate_grafana_dashboards(self):
        """Generate and save Grafana dashboards"""
        dashboards = self.dashboard_generator.generate_all_dashboards()

        for name, dashboard in dashboards.items():
            output_file = self.output_dir / f"grafana_dashboard_{name}.json"
            with open(output_file, 'w') as f:
                json.dump(dashboard, f, indent=2)
            print(f"✅ Generated Grafana dashboard: {output_file}")

    def generate_prometheus_alerts(self):
        """Generate and save Prometheus alert rules"""
        alerts = self.alert_generator.generate_all_alerts()

        for name, alert_rules in alerts.items():
            output_file = self.output_dir / f"prometheus_{name}.yml"
            with open(output_file, 'w') as f:
                yaml.dump(alert_rules, f, default_flow_style=False)
            print(f"✅ Generated Prometheus alerts: {output_file}")

    def create_docker_compose_monitoring(self):
        """Create docker-compose override for monitoring"""
        monitoring_config = {
            "version": "3.8",
            "services": {
                "prometheus": {
                    "volumes": [
                        "./monitoring/generated:/etc/prometheus/rules:ro"
                    ],
                    "command": [
                        "--config.file=/etc/prometheus/prometheus.yml",
                        "--storage.tsdb.path=/prometheus",
                        "--web.console.libraries=/etc/prometheus/console_libraries",
                        "--web.console.templates=/etc/prometheus/consoles",
                        "--storage.tsdb.retention.time=200h",
                        "--web.enable-lifecycle",
                        "--storage.tsdb.no-lockfile"
                    ]
                },
                "grafana": {
                    "volumes": [
                        "./monitoring/generated:/var/lib/grafana/dashboards:ro"
                    ],
                    "environment": [
                        "GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/grafana_dashboard_system_overview.json"
                    ]
                }
            }
        }

        output_file = self.output_dir / "docker-compose.monitoring.yml"
        with open(output_file, 'w') as f:
            yaml.dump(monitoring_config, f, default_flow_style=False)
        print(f"✅ Generated monitoring docker-compose: {output_file}")

    def create_setup_script(self):
        """Create setup script for monitoring"""
        script_content = '''#!/bin/bash
# E-commerce DSS Monitoring Setup Script

echo "🚀 Setting up E-commerce DSS monitoring..."

# Create monitoring directories
mkdir -p monitoring/grafana/provisioning/dashboards
mkdir -p monitoring/grafana/provisioning/datasources
mkdir -p monitoring/prometheus/rules

# Copy generated files
cp monitoring/generated/grafana_dashboard_*.json monitoring/grafana/provisioning/dashboards/
cp monitoring/generated/prometheus_*.yml monitoring/prometheus/rules/

# Create Grafana datasource config
cat > monitoring/grafana/provisioning/datasources/prometheus.yml << EOF
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

# Create Grafana dashboard config
cat > monitoring/grafana/provisioning/dashboards/dashboards.yml << EOF
apiVersion: 1
providers:
  - name: 'E-commerce DSS'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /var/lib/grafana/dashboards
EOF

# Restart monitoring services
echo "📊 Restarting monitoring services..."
docker-compose -f docker-compose.yml -f monitoring/generated/docker-compose.monitoring.yml restart prometheus grafana

echo "✅ Monitoring setup complete!"
echo "📊 Grafana: http://localhost:3001 (admin/admin123)"
echo "📈 Prometheus: http://localhost:9090"
'''

        script_file = self.output_dir / "setup_monitoring.sh"
        with open(script_file, 'w') as f:
            f.write(script_content)
        script_file.chmod(0o755)
        print(f"✅ Generated setup script: {script_file}")

    def setup_complete_monitoring(self):
        """Setup complete monitoring infrastructure"""
        print("🔧 Generating complete monitoring setup...")

        self.generate_grafana_dashboards()
        self.generate_prometheus_alerts()
        self.create_docker_compose_monitoring()
        self.create_setup_script()

        print("\n🎉 Monitoring setup generation complete!")
        print(f"📁 Files generated in: {self.output_dir}")
        print("📋 Next steps:")
        print("1. Run: ./monitoring/generated/setup_monitoring.sh")
        print("2. Access Grafana at http://localhost:3001")
        print("3. Access Prometheus at http://localhost:9090")

def main():
    """Main function to generate monitoring setup"""
    setup_manager = MonitoringSetupManager()
    setup_manager.setup_complete_monitoring()

if __name__ == "__main__":
    main()