#!/usr/bin/env python3
"""
Auto Import Dashboard to Grafana
Script to automatically import E-commerce DSS dashboard into Grafana
"""

import requests
import json
import time
import sys

# Configuration
GRAFANA_URL = "http://localhost:3001"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "admin"
DASHBOARD_FILE = "ecommerce_dss_dashboard.json"

def wait_for_grafana():
    """Wait for Grafana to be ready"""
    print("🔄 Waiting for Grafana to be ready...")

    for i in range(30):  # Wait up to 5 minutes
        try:
            response = requests.get(f"{GRAFANA_URL}/api/health", timeout=5)
            if response.status_code == 200:
                print("✅ Grafana is ready!")
                return True
        except requests.exceptions.RequestException:
            pass

        time.sleep(10)
        print(f"⏳ Waiting... ({i+1}/30)")

    print("❌ Grafana is not responding")
    return False

def setup_prometheus_datasource():
    """Setup Prometheus as datasource in Grafana"""
    print("🔧 Setting up Prometheus datasource...")

    datasource_config = {
        "name": "Prometheus",
        "type": "prometheus",
        "url": "http://prometheus:9090",
        "access": "proxy",
        "isDefault": True,
        "basicAuth": False
    }

    try:
        # Check if datasource already exists
        response = requests.get(
            f"{GRAFANA_URL}/api/datasources/name/Prometheus",
            auth=(GRAFANA_USER, GRAFANA_PASSWORD)
        )

        if response.status_code == 200:
            print("✅ Prometheus datasource already exists")
            return True

        # Create new datasource
        response = requests.post(
            f"{GRAFANA_URL}/api/datasources",
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            headers={"Content-Type": "application/json"},
            json=datasource_config
        )

        if response.status_code == 200:
            print("✅ Prometheus datasource created successfully")
            return True
        else:
            print(f"❌ Failed to create datasource: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"❌ Error setting up datasource: {e}")
        return False

def import_dashboard():
    """Import the E-commerce DSS dashboard"""
    print("📊 Importing E-commerce DSS dashboard...")

    try:
        # Load dashboard JSON
        with open(DASHBOARD_FILE, 'r') as f:
            dashboard_json = json.load(f)

        # Prepare import payload
        import_payload = {
            "dashboard": dashboard_json["dashboard"],
            "overwrite": True,
            "inputs": [
                {
                    "name": "DS_PROMETHEUS",
                    "type": "datasource",
                    "pluginId": "prometheus",
                    "value": "Prometheus"
                }
            ]
        }

        # Import dashboard
        response = requests.post(
            f"{GRAFANA_URL}/api/dashboards/import",
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            headers={"Content-Type": "application/json"},
            json=import_payload
        )

        if response.status_code == 200:
            result = response.json()
            dashboard_url = f"{GRAFANA_URL}/d/{result['uid']}"
            print(f"✅ Dashboard imported successfully!")
            print(f"🌐 Dashboard URL: {dashboard_url}")
            return True
        else:
            print(f"❌ Failed to import dashboard: {response.status_code} - {response.text}")
            return False

    except FileNotFoundError:
        print(f"❌ Dashboard file not found: {DASHBOARD_FILE}")
        return False
    except Exception as e:
        print(f"❌ Error importing dashboard: {e}")
        return False

def create_alerts():
    """Create basic alert rules"""
    print("🚨 Setting up alert rules...")

    alert_rules = [
        {
            "title": "High CPU Usage",
            "message": "CPU usage is above 80%",
            "frequency": "10s",
            "conditions": [
                {
                    "query": {
                        "queryType": "",
                        "refId": "A",
                        "model": {
                            "expr": "100 - (avg(irate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
                            "interval": "",
                            "legendFormat": "",
                            "refId": "A"
                        }
                    },
                    "reducer": {
                        "type": "last",
                        "params": []
                    },
                    "evaluator": {
                        "params": [80],
                        "type": "gt"
                    }
                }
            ]
        }
    ]

    try:
        for rule in alert_rules:
            response = requests.post(
                f"{GRAFANA_URL}/api/alerts",
                auth=(GRAFANA_USER, GRAFANA_PASSWORD),
                headers={"Content-Type": "application/json"},
                json=rule
            )

            if response.status_code == 200:
                print(f"✅ Alert rule created: {rule['title']}")
            else:
                print(f"⚠️ Could not create alert: {rule['title']}")

        return True

    except Exception as e:
        print(f"❌ Error creating alerts: {e}")
        return False

def main():
    """Main import process"""
    print("🎯 E-COMMERCE DSS GRAFANA DASHBOARD IMPORT")
    print("=" * 50)

    # Step 1: Wait for Grafana
    if not wait_for_grafana():
        sys.exit(1)

    time.sleep(5)  # Give Grafana a moment

    # Step 2: Setup Prometheus datasource
    if not setup_prometheus_datasource():
        print("⚠️ Continuing without datasource setup...")

    time.sleep(2)

    # Step 3: Import dashboard
    if not import_dashboard():
        print("❌ Dashboard import failed")
        sys.exit(1)

    # Step 4: Setup alerts
    create_alerts()

    print("\\n🎉 GRAFANA SETUP COMPLETED!")
    print("=" * 50)
    print(f"🌐 Grafana URL: {GRAFANA_URL}")
    print(f"👤 Username: {GRAFANA_USER}")
    print(f"🔑 Password: {GRAFANA_PASSWORD}")
    print("📊 Dashboard: E-commerce DSS - Complete Monitoring")
    print("\\n✅ Ready for monitoring!")

if __name__ == "__main__":
    main()