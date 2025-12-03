"""
Profile Service - Business Logic
"""
import logging
from typing import Dict, Optional
from app.models.user import ProfileUpdateRequest
import secrets
from datetime import datetime, timedelta
import bcrypt
from fastapi import HTTPException
from app.services.email_service import email_service
logger = logging.getLogger(__name__)

class ProfileService:
    def __init__(self, db):
        self.db = db
        self.email_service = email_service

    def _generate_otp(self) -> str:
        # 6 digits
        return f"{secrets.randbelow(1000000):06d}"
    
    def _hash_otp(self, otp: str) -> str:
        return bcrypt.hashpw(otp.encode(), bcrypt.gensalt()).decode()

    def _verify_otp(self, otp: str, otp_hash: str) -> bool:
        return bcrypt.checkpw(otp.encode(), otp_hash.encode())
    
    async def get_profile(self, user_id: int) -> Optional[Dict]:
        """Get user profile by ID"""
        query = """
        SELECT u.user_id, u.email, u.full_name, u.phone,
               r.role_name
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
    
    async def request_email_change(self, user_id: int, new_email: str):
        # 1. Lấy user
        user = await self.db.fetchrow(
            "SELECT user_id, email FROM iam_user WHERE user_id = $1", user_id
        )
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        old_email = user["email"]

        if new_email == old_email:
            raise HTTPException(status_code=400, detail="New email must be different")

        # 2. Check email đã tồn tại
        exists = await self.db.fetchval(
            "SELECT 1 FROM iam_user WHERE email = $1", new_email
        )
        if exists:
            raise HTTPException(status_code=400, detail="Email already in use")

        # 3. Tạo OTP
        otp = self._generate_otp()
        otp_hash = self._hash_otp(otp)
        expires_at = datetime.utcnow() + timedelta(minutes=10)

        row = await self.db.fetchrow(
            """
            INSERT INTO iam_email_change_request (
                user_id, old_email, new_email, otp_code_hash, expires_at
            )
            VALUES ($1, $2, $3, $4, $5)
            RETURNING request_id
            """,
            user_id, old_email, new_email, otp_hash, expires_at
        )
        request_id = str(row["request_id"])

        # 4. Gửi email tới old_email
        await self.email_service.send_email(
            to=old_email,
            subject="Xác nhận đổi email tài khoản",
            body=f"Mã xác nhận đổi email của bạn là: {otp}\nMã hết hạn sau 10 phút."
        )

        return {"request_id": request_id}
    
    async def confirm_email_change(self, user_id: int, request_id: str, otp: str):
        # 1. Lấy request
        req = await self.db.fetchrow(
            """
            SELECT *
            FROM iam_email_change_request
            WHERE request_id = $1 AND user_id = $2
            """,
            request_id, user_id
        )
        if not req:
            raise HTTPException(status_code=404, detail="Request not found")

        if req["status"] != "PENDING":
            raise HTTPException(status_code=400, detail="Request already processed")

        if req["expires_at"] < datetime.utcnow():
            raise HTTPException(status_code=400, detail="OTP expired")

        if not self._verify_otp(otp, req["otp_code_hash"]):
            raise HTTPException(status_code=400, detail="Invalid OTP")

        new_email = req["new_email"]

        # 2. Transaction: update email + mark request VERIFIED
        async with self.db.transaction():
            await self.db.execute(
                "UPDATE iam_user SET email = $1 WHERE user_id = $2",
                new_email, user_id
            )
            await self.db.execute(
                """
                UPDATE iam_email_change_request
                SET status = 'VERIFIED', verified_at = NOW()
                WHERE request_id = $1
                """,
                request_id
            )

        # (Optional) log lại vào iam_audit_log

        return {"success": True, "new_email": new_email}