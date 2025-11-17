"""
Profile Service - Business Logic
"""
import logging
from typing import Dict, Any, Optional
from models.user import ProfileUpdateRequest

logger = logging.getLogger(__name__)

class ProfileService:
    def __init__(self, db):
        self.db = db

    async def get_profile(self, user_id: int) -> Optional[Dict]:
        """Get user profile by ID"""
        query = """
        SELECT u.user_id, u.email, u.full_name, u.phone, u.last_login_at,
               r.role_code as role
        FROM iam.iam_user u
        LEFT JOIN iam.iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam.iam_role r ON ur.role_id = r.role_id
        WHERE u.user_id = $1 AND u.status = 'active'
        """
        result = await self.db.execute_query(query, (user_id,))
        return result[0] if result else None

    async def update_profile(self, user_id: int, profile_data: ProfileUpdateRequest) -> bool:
        """Update user profile"""
        # Check if user exists
        existing = await self.get_profile(user_id)
        if not existing:
            raise ValueError("User not found")
        
        # Build update query
        update_fields = []
        values = []
        param_count = 1
        
        if profile_data.full_name is not None:
            update_fields.append(f"full_name = ${param_count}")
            values.append(profile_data.full_name)
            param_count += 1
            
        if profile_data.phone is not None:
            update_fields.append(f"phone = ${param_count}")
            values.append(profile_data.phone)
            param_count += 1
            
        if profile_data.email is not None:
            # Check if email already exists for another user
            email_check_query = "SELECT user_id FROM iam.iam_user WHERE email = $1 AND user_id != $2"
            email_exists = await self.db.execute_query(email_check_query, (str(profile_data.email).lower(), user_id))
            
            if email_exists:
                raise ValueError("Email already exists")
            
            update_fields.append(f"email = ${param_count}")
            values.append(str(profile_data.email).lower())
            param_count += 1
        
        if not update_fields:
            raise ValueError("No fields to update")
        
        values.append(user_id)
        
        update_query = f"""
        UPDATE iam.iam_user 
        SET {', '.join(update_fields)}, updated_at = NOW()
        WHERE user_id = ${param_count}
        """
        
        await self.db.execute_query(update_query, values)
        return True