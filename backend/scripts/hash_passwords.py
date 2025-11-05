#!/usr/bin/env python3
"""
Script to hash passwords for IAM users
"""

import bcrypt

def hash_password(password: str) -> str:
    """Hash password using bcrypt"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

# Test user passwords
passwords = {
    'admin@dss.com': 'admin123',
    'manager@dss.com': 'manager123',
    'analyst@dss.com': 'analyst123',
    'customer@dss.com': 'customer123'
}

print("Hashed passwords for IAM users:")
print("=" * 50)

for email, password in passwords.items():
    hashed = hash_password(password)
    print(f"Email: {email}")
    print(f"Password: {password}")
    print(f"Hash: {hashed}")
    print(f"SQL: UPDATE iam_user SET password_hash = '{hashed}' WHERE email = '{email}';")
    print("-" * 50)