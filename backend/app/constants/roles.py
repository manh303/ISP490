"""
Role and permission constants - Updated to match actual database
"""

# Valid role codes (matching actual database)
VALID_ROLES = ["ADMIN", "ANALYST", "DATA_ENGINEER", "ML"]

# Role hierarchy (higher number = more permissions)
ROLE_HIERARCHY = {
    "ADMIN": 10,
    "DATA_ENGINEER": 5,
    "ML": 5,
    "ANALYST": 3
}

# Role-based menu definitions
ROLE_MENUS = {
    "ADMIN": {
        "modules": ["Dashboard", "User Management", "Activity Logs", "System Settings", "Data Management"],
        "actions": ["view", "create", "update", "delete", "manage_users", "view_logs"],
        "permissions": ["system.admin", "user.manage", "data.write", "analytics.view", "dss.dashboard"],
        "admin_features": {
            "user_management": True,
            "activity_logs": True,
            "system_settings": True,
            "user_creation": True,
            "user_deletion": True,
            "can_access_admin_panel": True
        }
    },
    "DATA_ENGINEER": {
        "modules": ["Dashboard", "Data Pipeline", "ETL Management", "Data Quality", "System Monitoring"],
        "actions": ["view", "create", "update", "manage_pipeline", "monitor"],
        "permissions": ["data.read", "data.write", "pipeline.manage", "analytics.view", "dss.dashboard"],
        "admin_features": {
            "user_management": False,
            "activity_logs": True,
            "system_settings": False,
            "user_creation": False,
            "user_deletion": False,
            "can_access_admin_panel": False
        }
    },
    "ML": {
        "modules": ["Dashboard", "ML Models", "Model Training", "Predictions", "Model Analytics"],
        "actions": ["view", "train", "predict", "deploy", "analyze"],
        "permissions": ["data.read", "ml.train", "ml.deploy", "analytics.view", "dss.dashboard"],
        "admin_features": {
            "user_management": False,
            "activity_logs": False,
            "system_settings": False,
            "user_creation": False,
            "user_deletion": False,
            "can_access_admin_panel": False
        }
    },
    "ANALYST": {
        "modules": ["Dashboard", "Customer Analytics", "Sales Analytics", "Reports", "Data Visualization"],
        "actions": ["view", "export", "analyze"],
        "permissions": ["data.read", "analytics.view", "reports.generate", "dss.dashboard"],
        "admin_features": {
            "user_management": False,
            "activity_logs": False,
            "system_settings": False,
            "user_creation": False,
            "user_deletion": False,
            "can_access_admin_panel": False
        }
    }
}

def validate_role_code(role_code: str) -> bool:
    """Validate if role code is valid"""
    return role_code in VALID_ROLES

def get_role_menu(role_code: str) -> dict:
    """Get menu configuration for role"""
    return ROLE_MENUS.get(role_code, ROLE_MENUS["ANALYST"])

def has_permission(user_role: str, required_role: str) -> bool:
    """Check if user role has permission for required role"""
    user_level = ROLE_HIERARCHY.get(user_role, 0)
    required_level = ROLE_HIERARCHY.get(required_role, 0)
    return user_level >= required_level