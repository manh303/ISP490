"""
Role and permission constants
"""

# Valid role codes
VALID_ROLES = ["ADMIN", "ANALYST", "CUSTOMER"]

# Role hierarchy (higher number = more permissions)
ROLE_HIERARCHY = {
    "ADMIN": 3,
    "ANALYST": 2, 
    "CUSTOMER": 1
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
    },
    "CUSTOMER": {
        "modules": ["Dashboard", "Orders", "Profile", "Purchase History"],
        "actions": ["view", "create_order", "update_profile"],
        "permissions": ["profile.view", "orders.create", "data.read_own"],
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
    return ROLE_MENUS.get(role_code, ROLE_MENUS["CUSTOMER"])

def has_permission(user_role: str, required_role: str) -> bool:
    """Check if user role has permission for required role"""
    user_level = ROLE_HIERARCHY.get(user_role, 0)
    required_level = ROLE_HIERARCHY.get(required_role, 0)
    return user_level >= required_level