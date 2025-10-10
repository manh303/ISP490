import pytest
from fastapi.testclient import TestClient


class TestAuthEndpoints:
    """Test authentication endpoints"""

    def test_health_check(self, client: TestClient):
        """Test basic health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

    def test_root_endpoint(self, client: TestClient):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "E-commerce DSS Backend"
        assert data["status"] == "running"
        assert data["version"] == "1.0.0"

    def test_api_status(self, client: TestClient):
        """Test API status endpoint"""
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["api_status"] == "active"
        assert "endpoints" in data
        assert "timestamp" in data

    def test_get_test_credentials(self, client: TestClient):
        """Test getting test credentials"""
        response = client.get("/api/v1/simple-auth/test-credentials")
        assert response.status_code == 200
        data = response.json()
        assert "credentials" in data
        assert len(data["credentials"]) == 4

        # Check admin credentials
        admin_cred = next((c for c in data["credentials"] if c["username"] == "admin"), None)
        assert admin_cred is not None
        assert admin_cred["role"] == "admin"
        assert admin_cred["password"] == "admin123"

    def test_auth_system_status(self, client: TestClient):
        """Test authentication system status"""
        response = client.get("/api/v1/simple-auth/status")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "total_users" in data
        assert "active_sessions" in data


class TestLogin:
    """Test login functionality"""

    def test_login_with_valid_credentials(self, client: TestClient):
        """Test login with valid admin credentials"""
        response = client.post(
            "/api/v1/simple-auth/login",
            json={
                "username": "admin",
                "password": "admin123",
                "remember_me": False
            }
        )
        assert response.status_code == 200
        data = response.json()

        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert "expires_in" in data
        assert "user" in data

        user = data["user"]
        assert user["username"] == "admin"
        assert user["role"] == "admin"
        assert user["email"] == "admin@ecommerce-dss.com"
        assert user["is_active"] is True

    def test_login_with_remember_me(self, client: TestClient):
        """Test login with remember me option"""
        response = client.post(
            "/api/v1/simple-auth/login",
            json={
                "username": "user1",
                "password": "user123",
                "remember_me": True
            }
        )
        assert response.status_code == 200
        data = response.json()

        # Remember me should give longer expiration (7 days = 604800 seconds)
        assert data["expires_in"] == 604800

    def test_login_with_invalid_username(self, client: TestClient):
        """Test login with invalid username"""
        response = client.post(
            "/api/v1/simple-auth/login",
            json={
                "username": "nonexistent",
                "password": "admin123",
                "remember_me": False
            }
        )
        assert response.status_code == 401
        data = response.json()
        assert data["detail"] == "Incorrect username or password"

    def test_login_with_invalid_password(self, client: TestClient):
        """Test login with invalid password"""
        response = client.post(
            "/api/v1/simple-auth/login",
            json={
                "username": "admin",
                "password": "wrongpassword",
                "remember_me": False
            }
        )
        assert response.status_code == 401
        data = response.json()
        assert data["detail"] == "Incorrect username or password"

    def test_login_with_missing_fields(self, client: TestClient):
        """Test login with missing required fields"""
        response = client.post(
            "/api/v1/simple-auth/login",
            json={"username": "admin"}  # Missing password
        )
        assert response.status_code == 422  # Validation error

    def test_login_all_user_types(self, client: TestClient, test_user_credentials):
        """Test login for all user types"""
        for user_type, creds in test_user_credentials.items():
            response = client.post(
                "/api/v1/simple-auth/login",
                json={
                    "username": creds["username"],
                    "password": creds["password"],
                    "remember_me": False
                }
            )
            assert response.status_code == 200
            data = response.json()
            assert data["user"]["role"] == creds["role"]


class TestProtectedEndpoints:
    """Test protected endpoints that require authentication"""

    def test_get_current_user_info(self, client: TestClient, authenticated_headers):
        """Test getting current user info with valid token"""
        response = client.get(
            "/api/v1/simple-auth/me",
            headers=authenticated_headers
        )
        assert response.status_code == 200
        data = response.json()

        assert data["username"] == "admin"
        assert data["role"] == "admin"
        assert data["email"] == "admin@ecommerce-dss.com"
        assert data["is_active"] is True

    def test_get_current_user_info_without_token(self, client: TestClient):
        """Test getting current user info without token"""
        response = client.get("/api/v1/simple-auth/me")
        assert response.status_code == 403  # Forbidden

    def test_get_current_user_info_with_invalid_token(self, client: TestClient):
        """Test getting current user info with invalid token"""
        response = client.get(
            "/api/v1/simple-auth/me",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401

    def test_validate_token(self, client: TestClient, authenticated_headers):
        """Test token validation endpoint"""
        response = client.get(
            "/api/v1/simple-auth/validate",
            headers=authenticated_headers
        )
        assert response.status_code == 200
        data = response.json()

        assert data["valid"] is True
        assert "user" in data
        assert data["user"]["username"] == "admin"

    def test_logout(self, client: TestClient, authenticated_headers):
        """Test logout endpoint"""
        response = client.post(
            "/api/v1/simple-auth/logout",
            headers=authenticated_headers
        )
        assert response.status_code == 200
        data = response.json()

        assert data["success"] is True
        assert "message" in data


class TestRoleBasedAccess:
    """Test role-based access control"""

    def test_admin_only_endpoint_with_admin(self, client: TestClient, authenticated_headers):
        """Test admin-only endpoint with admin user"""
        response = client.get(
            "/api/v1/simple-auth/admin-only",
            headers=authenticated_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "admin-only" in data["message"]

    def test_admin_only_endpoint_with_regular_user(self, client: TestClient, user_authenticated_headers):
        """Test admin-only endpoint with regular user"""
        response = client.get(
            "/api/v1/simple-auth/admin-only",
            headers=user_authenticated_headers
        )
        assert response.status_code == 403
        data = response.json()
        assert data["detail"] == "Admin access required"

    def test_manager_or_admin_endpoint_with_admin(self, client: TestClient, authenticated_headers):
        """Test manager/admin endpoint with admin user"""
        response = client.get(
            "/api/v1/simple-auth/manager-or-admin",
            headers=authenticated_headers
        )
        assert response.status_code == 200

    def test_list_users_admin_only(self, client: TestClient, authenticated_headers):
        """Test listing users (admin only)"""
        response = client.get(
            "/api/v1/simple-auth/users",
            headers=authenticated_headers
        )
        assert response.status_code == 200
        data = response.json()

        assert "users" in data
        assert "total" in data
        assert data["total"] == 4
        assert len(data["users"]) == 4

    def test_list_users_with_regular_user(self, client: TestClient, user_authenticated_headers):
        """Test listing users with regular user (should fail)"""
        response = client.get(
            "/api/v1/simple-auth/users",
            headers=user_authenticated_headers
        )
        assert response.status_code == 403
        data = response.json()
        assert data["detail"] == "Only admins can view all users"

    def test_session_info_admin_only(self, client: TestClient, authenticated_headers):
        """Test session info endpoint (admin only)"""
        response = client.get(
            "/api/v1/simple-auth/sessions",
            headers=authenticated_headers
        )
        assert response.status_code == 200
        data = response.json()

        assert "active_sessions" in data
        assert "sessions" in data