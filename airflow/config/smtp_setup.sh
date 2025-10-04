#!/bin/bash
"""
SMTP Configuration Setup for Airflow
====================================
This script helps configure email notifications for Airflow DAGs.
"""

echo "🔧 Setting up SMTP configuration for Airflow notifications"

# Check if we're in the Airflow environment
if [ -z "$AIRFLOW_HOME" ]; then
    export AIRFLOW_HOME="/opt/airflow"
fi

# Create configuration directory if it doesn't exist
mkdir -p "$AIRFLOW_HOME/config"

# Function to configure Gmail SMTP (example)
configure_gmail_smtp() {
    echo "📧 Configuring Gmail SMTP settings..."

    # Check if airflow.cfg exists
    if [ -f "$AIRFLOW_HOME/airflow.cfg" ]; then
        # Backup original config
        cp "$AIRFLOW_HOME/airflow.cfg" "$AIRFLOW_HOME/airflow.cfg.backup"

        # Update SMTP settings in airflow.cfg
        sed -i 's/^smtp_host = .*/smtp_host = smtp.gmail.com/' "$AIRFLOW_HOME/airflow.cfg"
        sed -i 's/^smtp_starttls = .*/smtp_starttls = True/' "$AIRFLOW_HOME/airflow.cfg"
        sed -i 's/^smtp_ssl = .*/smtp_ssl = False/' "$AIRFLOW_HOME/airflow.cfg"
        sed -i 's/^smtp_port = .*/smtp_port = 587/' "$AIRFLOW_HOME/airflow.cfg"
        sed -i 's/^smtp_mail_from = .*/smtp_mail_from = manhndhe173383@fpt.edu.vn/' "$AIRFLOW_HOME/airflow.cfg"

        echo "✅ Gmail SMTP configuration updated in airflow.cfg"
    else
        echo "⚠️ airflow.cfg not found. Creating environment variables instead."
    fi

    # Set environment variables
    export AIRFLOW__SMTP__SMTP_HOST="smtp.gmail.com"
    export AIRFLOW__SMTP__SMTP_STARTTLS="True"
    export AIRFLOW__SMTP__SMTP_SSL="False"
    export AIRFLOW__SMTP__SMTP_PORT="587"
    export AIRFLOW__SMTP__SMTP_MAIL_FROM="manhndhe173383@fpt.edu.vn"

    echo "✅ SMTP environment variables set"
}

# Function to disable email notifications (fallback)
disable_email_notifications() {
    echo "🚫 Disabling email notifications (using database-only notifications)"

    if [ -f "$AIRFLOW_HOME/airflow.cfg" ]; then
        sed -i 's/^smtp_host = .*/smtp_host = /' "$AIRFLOW_HOME/airflow.cfg"
        echo "✅ Email notifications disabled in airflow.cfg"
    fi

    unset AIRFLOW__SMTP__SMTP_HOST
    unset AIRFLOW__SMTP__SMTP_STARTTLS
    unset AIRFLOW__SMTP__SMTP_SSL
    unset AIRFLOW__SMTP__SMTP_PORT
    unset AIRFLOW__SMTP__SMTP_MAIL_FROM

    echo "✅ SMTP environment variables cleared"
}

# Function to test email configuration
test_email_config() {
    echo "🧪 Testing email configuration..."

    # Try to send a test email using Python
    python3 << EOF
import smtplib
import os
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

try:
    smtp_host = os.getenv('AIRFLOW__SMTP__SMTP_HOST', '')
    smtp_port = int(os.getenv('AIRFLOW__SMTP__SMTP_PORT', '587'))

    if smtp_host:
        # Test connection
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        print("✅ SMTP connection successful")
        server.quit()
    else:
        print("⚠️ No SMTP host configured")

except Exception as e:
    print(f"❌ SMTP test failed: {e}")
EOF
}

# Main configuration menu
echo "Choose SMTP configuration option:"
echo "1. Configure Gmail SMTP"
echo "2. Disable email notifications (database-only)"
echo "3. Test current email configuration"
echo "4. Show current configuration"

read -p "Enter choice (1-4): " choice

case $choice in
    1)
        configure_gmail_smtp
        test_email_config
        ;;
    2)
        disable_email_notifications
        ;;
    3)
        test_email_config
        ;;
    4)
        echo "📋 Current SMTP Configuration:"
        echo "SMTP_HOST: ${AIRFLOW__SMTP__SMTP_HOST:-'Not set'}"
        echo "SMTP_PORT: ${AIRFLOW__SMTP__SMTP_PORT:-'Not set'}"
        echo "SMTP_MAIL_FROM: ${AIRFLOW__SMTP__SMTP_MAIL_FROM:-'Not set'}"
        echo "SMTP_STARTTLS: ${AIRFLOW__SMTP__SMTP_STARTTLS:-'Not set'}"
        ;;
    *)
        echo "❌ Invalid choice"
        exit 1
        ;;
esac

# Create a simple notification test script
cat > "$AIRFLOW_HOME/test_notification.py" << 'EOF'
#!/usr/bin/env python3
"""
Test script for notification system
"""
import os
import sys
sys.path.append('/opt/airflow/dags')

from optimized_ecommerce_pipeline import send_success_notification_safe

def test_notification():
    """Test the notification system"""
    print("🧪 Testing notification system...")

    # Mock context
    context = {
        'execution_date': '2025-10-02T00:00:00+00:00',
        'dag_run': type('MockDagRun', (), {'run_id': 'test_run_12345'})(),
        'task_instance': type('MockTaskInstance', (), {
            'xcom_pull': lambda **kwargs: {
                'performance_summary': {
                    'total_execution_time': 1234.5,
                    'pipeline_success': True,
                    'optimization_level': 'high'
                }
            }
        })()
    }

    try:
        result = send_success_notification_safe(**context)
        if 'error' in result:
            print(f"⚠️ Notification test completed with warnings: {result['error']}")
        else:
            print(f"✅ Notification test successful: {result.get('notification_id', 'N/A')}")
    except Exception as e:
        print(f"❌ Notification test failed: {e}")

if __name__ == "__main__":
    test_notification()
EOF

chmod +x "$AIRFLOW_HOME/test_notification.py"

echo ""
echo "🎯 SMTP Setup Complete!"
echo "========================================"
echo "✅ Configuration script created"
echo "📧 Test notification script: $AIRFLOW_HOME/test_notification.py"
echo ""
echo "📋 Next Steps:"
echo "1. If using Gmail, set up App Password in your Google Account"
echo "2. Set AIRFLOW__SMTP__SMTP_USER and AIRFLOW__SMTP__SMTP_PASSWORD environment variables"
echo "3. Test notifications: python $AIRFLOW_HOME/test_notification.py"
echo "4. Restart Airflow services"
echo ""
echo "💡 Note: The pipeline will work without email. Notifications are stored in MongoDB regardless."