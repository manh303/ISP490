#!/usr/bin/env python3
"""
Email Service for OTP and Authentication
Handles email sending with Mailjet API and OTP generation
"""

import os
import secrets
import string
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import logging
from dataclasses import dataclass
try:
    import redis.asyncio as redis
except ImportError:
    redis = None

logger = logging.getLogger(__name__)

@dataclass
class EmailConfig:
    """Email configuration settings"""
    mailjet_api_key: str = os.getenv("MAILJET_API_KEY","0577893665068e154a703ae532f617ed")
    mailjet_api_secret: str = os.getenv("MAILJET_API_SECRET","a518e0492ff0ba92d1d304570cb97840")
    sender_email: str = os.getenv("EMAIL_FROM","mtminh1606@gmail.com")
    sender_name: str = os.getenv("EMAIL_FROM_NAME", "DSS E-commerce")
    mailjet_url: str = "https://api.mailjet.com/v3.1/send"
    timeout: int = 30
    retry_attempts: int = 3

    # OTP settings
    otp_length: int = 6
    otp_expires_minutes: int = 10
    max_attempts: int = 3

class OTPManager:
    """Manages OTP generation, storage and validation"""

    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.config = EmailConfig()

    async def generate_otp(self, email: str) -> str:
        """Generate a new OTP for email"""
        # Generate random 6-digit OTP
        otp = ''.join(secrets.choice(string.digits) for _ in range(self.config.otp_length))

        # Store in Redis with expiration
        if self.redis_client:
            otp_key = f"otp:{email}"
            attempts_key = f"otp_attempts:{email}"

            # Store OTP with expiration
            await self.redis_client.setex(
                otp_key,
                self.config.otp_expires_minutes * 60,
                otp
            )

            # Reset attempts counter
            await self.redis_client.setex(
                attempts_key,
                self.config.otp_expires_minutes * 60,
                0
            )
        else:
            # Fallback to in-memory storage (for development)
            if not hasattr(self, '_otp_storage'):
                self._otp_storage = {}

            self._otp_storage[email] = {
                'otp': otp,
                'expires': datetime.now() + timedelta(minutes=self.config.otp_expires_minutes),
                'attempts': 0
            }

        logger.info(f"Generated OTP for {email}: {otp}")  # Remove in production
        return otp

    async def verify_otp(self, email: str, provided_otp: str) -> Dict[str, Any]:
        """Verify OTP for email"""
        if self.redis_client:
            otp_key = f"otp:{email}"
            attempts_key = f"otp_attempts:{email}"

            # Get stored OTP and attempts
            stored_otp = await self.redis_client.get(otp_key)
            attempts = await self.redis_client.get(attempts_key) or 0
            attempts = int(attempts)

            if not stored_otp:
                return {
                    'valid': False,
                    'error': 'OTP expired or not found',
                    'attempts_remaining': 0
                }

            # Check attempts limit
            if attempts >= self.config.max_attempts:
                await self.redis_client.delete(otp_key, attempts_key)
                return {
                    'valid': False,
                    'error': 'Too many failed attempts',
                    'attempts_remaining': 0
                }

            # Verify OTP
            if stored_otp.decode() == provided_otp:
                # Success - clean up
                await self.redis_client.delete(otp_key, attempts_key)
                return {
                    'valid': True,
                    'message': 'OTP verified successfully'
                }
            else:
                # Increment attempts
                await self.redis_client.incr(attempts_key)
                remaining = self.config.max_attempts - (attempts + 1)
                return {
                    'valid': False,
                    'error': 'Invalid OTP',
                    'attempts_remaining': remaining
                }
        else:
            # Fallback to in-memory storage
            if not hasattr(self, '_otp_storage') or email not in self._otp_storage:
                return {
                    'valid': False,
                    'error': 'OTP expired or not found',
                    'attempts_remaining': 0
                }

            stored = self._otp_storage[email]

            # Check expiration
            if datetime.now() > stored['expires']:
                del self._otp_storage[email]
                return {
                    'valid': False,
                    'error': 'OTP expired',
                    'attempts_remaining': 0
                }

            # Check attempts
            if stored['attempts'] >= self.config.max_attempts:
                del self._otp_storage[email]
                return {
                    'valid': False,
                    'error': 'Too many failed attempts',
                    'attempts_remaining': 0
                }

            # Verify OTP
            if stored['otp'] == provided_otp:
                del self._otp_storage[email]
                return {
                    'valid': True,
                    'message': 'OTP verified successfully'
                }
            else:
                stored['attempts'] += 1
                remaining = self.config.max_attempts - stored['attempts']
                return {
                    'valid': False,
                    'error': 'Invalid OTP',
                    'attempts_remaining': remaining
                }

class EmailService:
    """Email service for sending OTP and other notifications using Mailjet"""

    def __init__(self, redis_client=None):
        self.config = EmailConfig()
        self.otp_manager = OTPManager(redis_client)

    def send_email(self, to_email: str, subject: str, html_content: str) -> Dict[str, Any]:
        """Send email using Mailjet API (synchronous)"""
        # Check if Mailjet is configured
        if not self.config.mailjet_api_key or not self.config.mailjet_api_secret or not self.config.sender_email:
            logger.warning(
                f"DEV MODE (Mailjet): Missing configuration. "
                f"Would send email to {to_email}"
            )
            return {'success': True, 'message': 'Email sent (dev mode)'}

        last_error = None

        for attempt in range(self.config.retry_attempts):
            try:
                payload = {
                    "Messages": [
                        {
                            "From": {
                                "Email": self.config.sender_email,
                                "Name": self.config.sender_name
                            },
                            "To": [
                                {
                                    "Email": to_email,
                                    "Name": to_email.split('@')[0]
                                }
                            ],
                            "Subject": subject,
                            "HTMLPart": html_content
                        }
                    ]
                }

                # Mailjet uses Basic Auth
                auth = (self.config.mailjet_api_key, self.config.mailjet_api_secret)

                response = requests.post(
                    self.config.mailjet_url,
                    json=payload,
                    auth=auth,
                    timeout=self.config.timeout
                )

                if 200 <= response.status_code < 300:
                    logger.info(f"[Mailjet] Email sent successfully to {to_email} on attempt {attempt + 1}")
                    return {
                        'success': True,
                        'message': 'Email sent successfully'
                    }
                else:
                    last_error = f"Status {response.status_code}: {response.text}"
                    logger.warning(f"[Mailjet] Attempt {attempt + 1} failed for {to_email}: {last_error}")

            except Exception as e:
                last_error = str(e)
                logger.warning(f"[Mailjet] Attempt {attempt + 1} failed for {to_email}: {e}")

            # Wait before retry (except last attempt)
            if attempt < self.config.retry_attempts - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"[Mailjet] Failed to send email to {to_email} after {self.config.retry_attempts} attempts: {last_error}")
        return {
            'success': False,
            'error': f'Failed to send email: {last_error}'
        }

    async def send_otp_email(self, email: str, name: str = None) -> Dict[str, Any]:
        """Generate and send OTP to email"""
        try:
            # Generate OTP
            otp = await self.otp_manager.generate_otp(email)

            

            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Email Verification Code</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        line-height: 1.6;
                        color: #333;
                        max-width: 600px;
                        margin: 0 auto;
                        padding: 20px;
                        background-color: #f8f9fa;
                    }}
                    .container {{
                        background: white;
                        border-radius: 8px;
                        padding: 30px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    .header {{
                        text-align: center;
                        margin-bottom: 30px;
                    }}
                    .logo {{
                        font-size: 24px;
                        font-weight: bold;
                        color: #2563eb;
                        margin-bottom: 10px;
                    }}
                    .otp-code {{
                        background: #f3f4f6;
                        border: 2px dashed #d1d5db;
                        border-radius: 8px;
                        padding: 20px;
                        text-align: center;
                        margin: 25px 0;
                    }}
                    .otp-number {{
                        font-size: 32px;
                        font-weight: bold;
                        color: #1f2937;
                        letter-spacing: 8px;
                        font-family: monospace;
                    }}
                    .warning {{
                        background: #fef3c7;
                        border-left: 4px solid #f59e0b;
                        padding: 15px;
                        margin: 20px 0;
                        border-radius: 4px;
                    }}
                    .footer {{
                        text-align: center;
                        color: #6b7280;
                        font-size: 14px;
                        margin-top: 30px;
                        padding-top: 20px;
                        border-top: 1px solid #e5e7eb;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <div class="logo">🛒 DSS E-commerce</div>
                        <h1>Email Verification Code</h1>
                    </div>

                    <p>Chào {name},</p>

                    <p>Cảm ơn bạn đã tạo tài khoản với DSS E-commerce! Để hoàn tất đăng ký, vui lòng sử dụng mã xác minh bên dưới:</p>

                    <div class="otp-code">
                        <div style="margin-bottom: 10px; color: #6b7280; font-size: 14px;">Mã xác minh của bạn là:</div>
                        <div class="otp-number">{otp}</div>
                    </div>

                    <div class="warning">
                        <strong>⚠️ Quan trọng:</strong>
                        <ul style="margin: 5px 0; padding-left: 20px;">
                            <li>Mã này sẽ hết hạn sau <strong>{self.config.otp_expires_minutes} phút</strong></li>
                            <li>Bạn có <strong>{self.config.max_attempts} lần thử</strong> để nhập đúng mã</li>
                            <li>Không bao giờ chia sẻ mã này với bất kỳ ai</li>
                        </ul>
                    </div>

                    <p>Nếu bạn không yêu cầu mã xác minh này, bạn có thể bỏ qua email này. Có thể ai đó đã nhập nhầm địa chỉ email của bạn.</p>

                    <div class="footer">
                        <p>Đây là tin nhắn tự động từ Hệ thống DSS E-commerce</p>
                        <p>© 2024 DSS E-commerce. Bảo lưu mọi quyền.</p>
                    </div>
                </div>
            </body>
            </html>
            """

            # Send email (synchronous call)
            result = self.send_email(
                to_email=email,
                subject="Email Verification Code - DSS E-commerce",
                html_content=html_content
            )

            if result['success']:
                return {
                    'success': True,
                    'message': f'Verification code sent to {email}',
                    'expires_in_minutes': self.config.otp_expires_minutes
                }
            else:
                return {
                    'success': False,
                    'error': f'Failed to send email: {result["error"]}'
                }

        except Exception as e:
            logger.error(f"Failed to send OTP email to {email}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def verify_email_otp(self, email: str, otp: str) -> Dict[str, Any]:
        """Verify OTP for email verification"""
        return await self.otp_manager.verify_otp(email, otp)

# Singleton instance
email_service = EmailService()

# Export functions for easy import
async def send_otp_email(email: str, name: str = None) -> Dict[str, Any]:
    """Send OTP to email"""
    return await email_service.send_otp_email(email, name)

async def verify_otp(email: str, otp: str) -> Dict[str, Any]:
    """Verify OTP"""
    return await email_service.verify_email_otp(email, otp)

__all__ = ['EmailService', 'OTPManager', 'email_service', 'send_otp_email', 'verify_otp']