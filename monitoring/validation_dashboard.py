#!/usr/bin/env python3
"""
Validation Dashboard Generator
Tạo dashboard để kiểm tra và validate toàn bộ hệ thống monitoring
"""

import json
from datetime import datetime


def create_monitoring_validation_dashboard():
    """Tạo dashboard validation cho monitoring system"""

    dashboard = {
        "dashboard": {
            "id": None,
            "title": "🔍 E-commerce DSS - Monitoring Validation Dashboard",
            "tags": ["validation", "monitoring", "overview"],
            "timezone": "browser",
            "panels": [
                # Row 1: Service Status Overview
                {
                    "id": 1,
                    "title": "📊 Service Status Overview",
                    "type": "stat",
                    "targets": [
                        {
                            "expr": "up",
                            "legendFormat": "{{job}} - {{instance}}",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "mappings": [
                                {"options": {"0": {"text": "DOWN"}}, "type": "value"},
                                {"options": {"1": {"text": "UP"}}, "type": "value"}
                            ],
                            "color": {
                                "mode": "thresholds"
                            },
                            "thresholds": {
                                "steps": [
                                    {"color": "red", "value": 0},
                                    {"color": "green", "value": 1}
                                ]
                            }
                        }
                    },
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0}
                },

                # Row 2: System Metrics
                {
                    "id": 2,
                    "title": "💻 System Resources",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "100 - (avg(irate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
                            "legendFormat": "CPU Usage %",
                            "refId": "A"
                        },
                        {
                            "expr": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
                            "legendFormat": "Memory Usage %",
                            "refId": "B"
                        },
                        {
                            "expr": "(1 - (node_filesystem_avail_bytes / node_filesystem_size_bytes)) * 100",
                            "legendFormat": "Disk Usage %",
                            "refId": "C"
                        }
                    ],
                    "yAxes": [
                        {"label": "Percentage", "max": 100, "min": 0}
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
                },

                # Row 2: Database Metrics
                {
                    "id": 3,
                    "title": "🗄️ Database Status",
                    "type": "stat",
                    "targets": [
                        {
                            "expr": "pg_up",
                            "legendFormat": "PostgreSQL",
                            "refId": "A"
                        },
                        {
                            "expr": "redis_up",
                            "legendFormat": "Redis",
                            "refId": "B"
                        },
                        {
                            "expr": "mongodb_up",
                            "legendFormat": "MongoDB",
                            "refId": "C"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "mappings": [
                                {"options": {"0": {"text": "DOWN"}}, "type": "value"},
                                {"options": {"1": {"text": "UP"}}, "type": "value"}
                            ],
                            "color": {
                                "mode": "thresholds"
                            },
                            "thresholds": {
                                "steps": [
                                    {"color": "red", "value": 0},
                                    {"color": "green", "value": 1}
                                ]
                            }
                        }
                    },
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
                },

                # Row 3: PostgreSQL Details
                {
                    "id": 4,
                    "title": "🐘 PostgreSQL Metrics",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "pg_stat_database_numbackends",
                            "legendFormat": "Active Connections",
                            "refId": "A"
                        },
                        {
                            "expr": "rate(pg_stat_database_xact_commit[5m])",
                            "legendFormat": "Transactions/sec",
                            "refId": "B"
                        },
                        {
                            "expr": "pg_stat_database_tup_returned",
                            "legendFormat": "Tuples Returned",
                            "refId": "C"
                        }
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16}
                },

                # Row 3: Redis Details
                {
                    "id": 5,
                    "title": "📮 Redis Metrics",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "redis_connected_clients",
                            "legendFormat": "Connected Clients",
                            "refId": "A"
                        },
                        {
                            "expr": "redis_memory_used_bytes",
                            "legendFormat": "Memory Used (bytes)",
                            "refId": "B"
                        },
                        {
                            "expr": "rate(redis_commands_processed_total[5m])",
                            "legendFormat": "Commands/sec",
                            "refId": "C"
                        }
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16}
                },

                # Row 4: Network Activity
                {
                    "id": 6,
                    "title": "🌐 Network Activity",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "rate(node_network_receive_bytes_total{device!=\"lo\"}[5m])",
                            "legendFormat": "RX {{device}}",
                            "refId": "A"
                        },
                        {
                            "expr": "rate(node_network_transmit_bytes_total{device!=\"lo\"}[5m])",
                            "legendFormat": "TX {{device}}",
                            "refId": "B"
                        }
                    ],
                    "yAxes": [
                        {"label": "Bytes/sec"}
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 24}
                },

                # Row 4: Prometheus Stats
                {
                    "id": 7,
                    "title": "📈 Prometheus Stats",
                    "type": "stat",
                    "targets": [
                        {
                            "expr": "prometheus_tsdb_symbol_table_size_bytes",
                            "legendFormat": "TSDB Size",
                            "refId": "A"
                        },
                        {
                            "expr": "prometheus_config_last_reload_successful",
                            "legendFormat": "Config Reload Status",
                            "refId": "B"
                        },
                        {
                            "expr": "count(count by (__name__)({__name__=~\".+\"}))",
                            "legendFormat": "Total Metrics",
                            "refId": "C"
                        }
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 24}
                },

                # Row 5: Container Resources
                {
                    "id": 8,
                    "title": "🐳 Top Container Memory Usage",
                    "type": "table",
                    "targets": [
                        {
                            "expr": "topk(10, container_memory_usage_bytes{name!=\"\"})",
                            "legendFormat": "{{name}}",
                            "refId": "A",
                            "instant": True
                        }
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 32}
                },

                # Row 5: Service Discovery
                {
                    "id": 9,
                    "title": "🎯 Service Discovery",
                    "type": "table",
                    "targets": [
                        {
                            "expr": "up",
                            "legendFormat": "{{job}} @ {{instance}}",
                            "refId": "A",
                            "instant": True
                        }
                    ],
                    "transformations": [
                        {
                            "id": "organize",
                            "options": {
                                "excludeByName": {},
                                "indexByName": {},
                                "renameByName": {
                                    "job": "Service",
                                    "instance": "Instance",
                                    "Value": "Status"
                                }
                            }
                        }
                    ],
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 32}
                },

                # Row 6: Application Health Summary
                {
                    "id": 10,
                    "title": "🏥 Application Health Summary",
                    "type": "logs",
                    "targets": [
                        {
                            "expr": "probe_success",
                            "legendFormat": "Health Check: {{instance}}",
                            "refId": "A"
                        }
                    ],
                    "gridPos": {"h": 6, "w": 24, "x": 0, "y": 40}
                }
            ],
            "time": {
                "from": "now-1h",
                "to": "now"
            },
            "refresh": "30s"
        }
    }

    return dashboard


def create_quick_validation_script():
    """Tạo script Python để validate monitoring system"""

    script = '''#!/usr/bin/env python3
"""
Quick Monitoring Validation Script
Kiểm tra nhanh tất cả các services monitoring
"""

import requests
import json
from datetime import datetime

# Configuration
PROMETHEUS_URL = "http://localhost:9090"
GRAFANA_URL = "http://localhost:3001"

def check_prometheus_targets():
    """Kiểm tra targets trong Prometheus"""
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/targets")
        data = response.json()

        if data["status"] == "success":
            targets = data["data"]["activeTargets"]

            print("🎯 PROMETHEUS TARGETS STATUS:")
            print("=" * 50)

            up_count = 0
            down_count = 0

            for target in targets:
                job = target["labels"]["job"]
                instance = target["labels"]["instance"]
                health = target["health"]

                status_icon = "✅" if health == "up" else "❌"
                print(f"{status_icon} {job:20} | {instance:25} | {health.upper()}")

                if health == "up":
                    up_count += 1
                else:
                    down_count += 1

            print("=" * 50)
            print(f"📊 SUMMARY: {up_count} UP, {down_count} DOWN")
            return up_count, down_count
        else:
            print("❌ Prometheus API error")
            return 0, 0

    except Exception as e:
        print(f"❌ Error connecting to Prometheus: {e}")
        return 0, 0

def check_prometheus_metrics():
    """Kiểm tra số lượng metrics đang được thu thập"""
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/label/__name__/values")
        data = response.json()

        if data["status"] == "success":
            metrics = data["data"]

            print("\\n📈 METRICS COLLECTION:")
            print("=" * 50)
            print(f"Total metrics: {len(metrics)}")

            # Categorize metrics
            categories = {
                "node_": "System Metrics",
                "pg_": "PostgreSQL",
                "redis_": "Redis",
                "grafana_": "Grafana",
                "go_": "Go Runtime",
                "prometheus_": "Prometheus",
                "up": "Service Health"
            }

            for prefix, name in categories.items():
                count = sum(1 for m in metrics if m.startswith(prefix))
                if count > 0:
                    print(f"{name:20}: {count:4} metrics")

            return len(metrics)
        else:
            print("❌ Prometheus metrics API error")
            return 0

    except Exception as e:
        print(f"❌ Error checking metrics: {e}")
        return 0

def check_grafana_health():
    """Kiểm tra Grafana health"""
    try:
        response = requests.get(f"{GRAFANA_URL}/api/health")

        if response.status_code == 200:
            print("\\n📊 GRAFANA STATUS:")
            print("=" * 50)
            print("✅ Grafana is healthy and accessible")
            return True
        else:
            print(f"❌ Grafana health check failed: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error connecting to Grafana: {e}")
        return False

def main():
    """Main validation function"""
    print("🔍 E-COMMERCE DSS MONITORING VALIDATION")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Check Prometheus targets
    up_targets, down_targets = check_prometheus_targets()

    # Check metrics collection
    total_metrics = check_prometheus_metrics()

    # Check Grafana
    grafana_ok = check_grafana_health()

    # Final summary
    print("\\n🏁 FINAL VALIDATION SUMMARY:")
    print("=" * 60)
    print(f"✅ Services UP: {up_targets}")
    print(f"❌ Services DOWN: {down_targets}")
    print(f"📈 Total Metrics: {total_metrics}")
    print(f"📊 Grafana: {'OK' if grafana_ok else 'ERROR'}")

    # Overall health score
    total_services = up_targets + down_targets
    health_score = (up_targets / total_services * 100) if total_services > 0 else 0

    print(f"\\n🎯 OVERALL HEALTH SCORE: {health_score:.1f}%")

    if health_score >= 80:
        print("🎉 MONITORING SYSTEM IS HEALTHY!")
    elif health_score >= 60:
        print("⚠️  MONITORING SYSTEM NEEDS ATTENTION")
    else:
        print("🚨 MONITORING SYSTEM REQUIRES IMMEDIATE FIX")

if __name__ == "__main__":
    main()
'''

    return script


if __name__ == "__main__":
    # Tạo dashboard
    dashboard = create_monitoring_validation_dashboard()

    with open("monitoring_validation_dashboard.json", "w") as f:
        json.dump(dashboard, f, indent=2)

    print("✅ Created monitoring validation dashboard")

    # Tạo validation script
    script = create_quick_validation_script()

    with open("validate_monitoring.py", "w") as f:
        f.write(script)

    print("✅ Created monitoring validation script")
    print("\\n🚀 To run validation: python validate_monitoring.py")