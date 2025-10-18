#!/usr/bin/env python3
"""
Backend diagnostic script
"""

import sys
import subprocess
import socket
import requests
import time

def check_port(port):
    """Check if port is available"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex(('localhost', port))
            return result == 0
    except:
        return False

def check_python_processes():
    """Check running Python processes"""
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq python.exe'],
                              capture_output=True, text=True)
        return result.stdout
    except:
        return "Could not check processes"

def test_backend_connection(port):
    """Test backend connection"""
    try:
        response = requests.get(f'http://localhost:{port}/', timeout=5)
        return f"SUCCESS: {response.status_code} - {response.json()}"
    except requests.exceptions.ConnectionError:
        return "ERROR: Connection refused"
    except requests.exceptions.Timeout:
        return "ERROR: Connection timeout"
    except Exception as e:
        return f"ERROR: {e}"

def main():
    print("=== Backend Diagnostic ===")
    print(f"Python version: {sys.version}")
    print()

    # Check common ports
    ports = [8000, 8001, 3000, 5000]
    print("Port availability:")
    for port in ports:
        status = "OCCUPIED" if check_port(port) else "AVAILABLE"
        print(f"  Port {port}: {status}")
    print()

    # Check Python processes
    print("Python processes:")
    processes = check_python_processes()
    print(processes)
    print()

    # Test backend connections
    print("Backend connection tests:")
    for port in [8000, 8001]:
        if check_port(port):
            result = test_backend_connection(port)
            print(f"  Port {port}: {result}")
        else:
            print(f"  Port {port}: No service running")

if __name__ == "__main__":
    main()