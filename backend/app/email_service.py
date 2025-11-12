#!/usr/bin/env python3
"""
Email Service for OTP and Authentication
Handles email sending with Gmail SMTP and OTP generation
"""

import os
import asyncio
import secrets
import string
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str = os.getenv("SMTP_USERNAME", "manhndhe173383@fpt.edu.vn")
    timeout: int = 30  # Increased timeout
    retry_attempts: int = 3  # Retry mechanism
    sender_password: str = os.getenv("SMTP_PASSWORD", "cvin xncb nmfi ogsa")
    sender_name: str = "DSS E-commerce"

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
    """Email service for sending OTP and other notifications"""

    def __init__(self, redis_client=None):
        self.config = EmailConfig()
        self.otp_manager = OTPManager(redis_client)

    async def send_email(self, to_email: str, subject: str, html_content: str) -> Dict[str, Any]:
        """Send email using Gmail SMTP (async) with retry mechanism"""
        last_error = None

        for attempt in range(self.config.retry_attempts):
            try:
                # Create message
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From'] = f"{self.config.sender_name} <{self.config.sender_email}>"
                msg['To'] = to_email

                # Add HTML content
                html_part = MIMEText(html_content, 'html')
                msg.attach(html_part)

                # Send email using aiosmtplib with increased timeout
                await aiosmtplib.send(
                    msg,
                    hostname=self.config.smtp_server,
                    port=self.config.smtp_port,
                    start_tls=True,
                    username=self.config.sender_email,
                    password=self.config.sender_password,
                    timeout=self.config.timeout  # Increased timeout
                )

                logger.info(f"Email sent successfully to {to_email} on attempt {attempt + 1}")
                return {
                    'success': True,
                    'message': 'Email sent successfully'
                }

            except Exception as e:
                last_error = e
                logger.warning(f"Email attempt {attempt + 1} failed for {to_email}: {e}")

                # Wait before retry (except last attempt)
                if attempt < self.config.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"Failed to send email to {to_email} after {self.config.retry_attempts} attempts: {last_error}")
        return {
            'success': False,
            'error': str(last_error)
        }

    async def send_otp_email(self, email: str, name: str = None) -> Dict[str, Any]:
        """Generate and send OTP to email"""
        try:
            # Generate OTP
            otp = await self.otp_manager.generate_otp(email)

            # Create email content
            display_name = name or email.split('@')[0]

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

                    <p>Hi {display_name},</p>

                    <p>Thank you for creating an account with DSS E-commerce! To complete your registration, please use the verification code below:</p>

                    <div class="otp-code">
                        <div style="margin-bottom: 10px; color: #6b7280; font-size: 14px;">Your verification code is:</div>
                        <div class="otp-number">{otp}</div>
                    </div>

                    <div class="warning">
                        <strong>⚠️ Important:</strong>
                        <ul style="margin: 5px 0; padding-left: 20px;">
                            <li>This code will expire in <strong>{self.config.otp_expires_minutes} minutes</strong></li>
                            <li>You have <strong>{self.config.max_attempts} attempts</strong> to enter the correct code</li>
                            <li>Never share this code with anyone</li>
                        </ul>
                    </div>

                    <p>If you didn't request this verification code, you can safely ignore this email. Someone else might have typed your email address by mistake.</p>

                    <div class="footer">
                        <p>This is an automated message from DSS E-commerce System</p>
                        <p>© 2024 DSS E-commerce. All rights reserved.</p>
                    </div>
                </div>
            </body>
            </html>
            """

            # Send email
            result = await self.send_email(
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