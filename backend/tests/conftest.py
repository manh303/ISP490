import pytest
from fastapi.testclient import TestClient
import sys
import os

# Add app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from simple_main import app

@pytest.fixture
def client():
    """FastAPI test client"""
    with TestClient(app) as test_client:
        yield test_client

@pytest.fixture
def test_user_credentials():
    """Test user credentials"""
    return {
        "admin": {"username": "admin", "password": "admin123", "role": "admin"},
        "user1": {"username": "user1", "password": "user123", "role": "user"},
        "manager": {"username": "manager", "password": "manager123", "role": "manager"},
        "analyst": {"username": "analyst", "password": "analyst123", "role": "analyst"}
    }

@pytest.fixture
def authenticated_headers(client, test_user_credentials):
    """Get authentication headers for admin user"""
    login_response = client.post(
        "/api/v1/simple-auth/login",
        json={
            "username": test_user_credentials["admin"]["username"],
            "password": test_user_credentials["admin"]["password"],
            "remember_me": False
        }
    )
    assert login_response.status_code == 200
    token = login_response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}

@pytest.fixture
def user_authenticated_headers(client, test_user_credentials):
    """Get authentication headers for regular user"""
    login_response = client.post(
        "/api/v1/simple-auth/login",
        json={
            "username": test_user_credentials["user1"]["username"],
            "password": test_user_credentials["user1"]["password"],
            "remember_me": False
        }
    )
    assert login_response.status_code == 200
    token = login_response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}