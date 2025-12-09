"""
Database helper functions for authentication and user management
"""

import bcrypt
import hashlib
import logging
from fastapi import HTTPException
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# Define valid users for authentication (fallback)
VALID_USERS = {
    "admin@dss.com": {
        "user_id": 1,
        "password": "admin123",
        "full_name": "System Administrator",
        "role": "ADMIN"
    },
    "analyst@dss.com": {
        "user_id": 3,
        "password": "analyst123",
        "full_name": "Data Analyst",
        "role": "ANALYST"
    },
    "ml@dss.com": {
        "user_id": 4,
        "password": "mleng123",
        "full_name": "ML Engineer",
        "role": "ML"
    },
    "dataeng@dss.com": {
        "user_id": 2,
        "password": "dataeng123",
        "full_name": "Data Engineer",
        "role": "DATA_ENGINEER"
    }
}


async def authenticate_user_db(email: str, password: str, db: Any) -> Optional[Dict[str, Any]]:
    """
    Authenticate user from database
    
    Args:
        email: User email
        password: User password
        db: Database manager instance
        
    Returns:
        User data dict if authentication successful, None otherwise
    """
    try:
        query = """
        SELECT user_id, email, password_hash, full_name, status
        FROM iam.iam_user
        WHERE email = $1 AND status = 'active'
        """
        result = await db.execute_query(query, (email.lower(),))
        
        if not result or len(result) == 0:
            return None
        
        user = result[0]
        password_hash = user['password_hash']
        
        # Verify password using bcrypt
        if not bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
            return None
        
        # Get user roles
        role_query = """
        SELECT r.role_code, r.role_name
        FROM iam.iam_user_role ur
        JOIN iam.iam_role r ON ur.role_id = r.role_id
        WHERE ur.user_id = $1
        """
        roles_result = await db.execute_query(role_query, (user['user_id'],))
        role_codes = [r['role_code'] for r in roles_result] if roles_result else []
        
        # Determine main role (first role or CUSTOMER by default)
        main_role = role_codes[0] if role_codes else 'CUSTOMER'
        
        return {
            'user_id': user['user_id'],
            'email': user['email'],
            'full_name': user['full_name'],
            'role': main_role,
            'roles': role_codes
        }
    except Exception as e:
        logger.error(f"Database authentication error: {e}")
        return None


def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Fallback: authenticate user with hardcoded users
    
    Args:
        email: User email
        password: User password
        
    Returns:
        User data dict if authentication successful, None otherwise
    """
    user_data = VALID_USERS.get(email.lower())
    if not user_data or user_data["password"] != password:
        return None
    return user_data


async def create_user_in_db(db: Any, name: str, email: str, password_hash: str) -> int:
    """
    Create new user in database
    
    Args:
        db: Database manager instance
        name: User full name
        email: User email
        password_hash: Hashed password
        
    Returns:
        User ID
        
    Raises:
        HTTPException: If user creation fails
    """
    if not db.is_connected:
        raise HTTPException(status_code=500, detail="Database not connected")

    # Insert user
    query = """
    INSERT INTO iam.iam_user (email, password_hash, full_name, status, created_at, updated_at)
    VALUES ($1, $2, $3, 'active', NOW(), NOW())
    RETURNING user_id
    """
    result = await db.execute_query(query, (email, password_hash, name))

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create user")

    user_id = result[0]['user_id']

    # Assign default CUSTOMER role
    role_query = """
    INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at)
    SELECT $1, role_id, NOW() FROM iam.iam_role WHERE role_code = 'CUSTOMER'
    """
    await db.execute_query(role_query, (user_id,))

    return user_id


async def store_verification_token(db: Any, email: str, token_hash: str) -> None:
    """
    Store email verification token
    
    Args:
        db: Database manager instance
        email: User email
        token_hash: Hashed verification token
    """
    await db.execute_query(
        """
        INSERT INTO iam.iam_email_verification (email, token_hash, created_at, expires_at, consumed)
        VALUES ($1, $2, NOW(), NOW() + INTERVAL '15 minutes', false)
        ON CONFLICT (email) DO UPDATE
        SET token_hash = EXCLUDED.token_hash,
            created_at = NOW(),
            expires_at = NOW() + INTERVAL '15 minutes',
            consumed = false
        """,
        (email, token_hash)
    )


async def verify_email_token(db: Any, email: str, token_hash: str) -> bool:
    """
    Verify email verification token
    
    Args:
        db: Database manager instance
        email: User email
        token_hash: Hashed verification token
        
    Returns:
        True if token is valid, False otherwise
    """
    if not db.is_connected:
        return False

    query = """
    SELECT token_id FROM iam.iam_email_verification_token
    WHERE email = $1 AND token_hash = $2 AND expires_at > NOW() AND used_at IS NULL
    """
    result = await db.execute_query(query, (email, token_hash))

    if result:
        # Mark token as used
        update_query = """
        UPDATE iam.iam_email_verification_token
        SET used_at = NOW()
        WHERE token_id = $1
        """
        await db.execute_query(update_query, (result[0]['token_id'],))
        return True

    return False


async def activate_user(db: Any, email: str) -> int:
    """
    Activate user account
    
    Args:
        db: Database manager instance
        email: User email
        
    Returns:
        User ID
        
    Raises:
        HTTPException: If activation fails
    """
    if not db.is_connected:
        raise HTTPException(status_code=500, detail="Database not connected")

    query = """
    UPDATE iam.iam_user
    SET status = 'active', updated_at = NOW()
    WHERE email = $1 AND status = 'pending'
    RETURNING user_id
    """
    result = await db.execute_query(query, (email,))

    if not result:
        raise HTTPException(status_code=400, detail="User not found or already activated")

    return result[0]['user_id']


async def check_email_exists(db: Any, email: str) -> bool:
    """
    Check if email already exists in database
    
    Args:
        db: Database manager instance
        email: Email to check
        
    Returns:
        True if email exists, False otherwise
    """
    if not db.is_connected:
        return False

    query = "SELECT user_id FROM iam.iam_user WHERE email = $1"
    result = await db.execute_query(query, (email,))
    return len(result) > 0
