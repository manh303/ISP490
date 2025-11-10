"""
Common validation utilities
"""
import re
from typing import Optional

def validate_phone(phone: Optional[str]) -> Optional[str]:
    """Validate Vietnamese phone number"""
    if phone is None:
        return None
    
    # Remove spaces and dashes
    phone_clean = phone.replace(' ', '').replace('-', '')
    
    if not phone_clean.isdigit():
        raise ValueError('Phone number must contain only digits')
    
    if not phone_clean.startswith('0'):
        raise ValueError('Phone number must start with 0')
    
    if len(phone_clean) < 10:
        raise ValueError('Phone number must have at least 10 digits')
    
    if len(phone_clean) > 11:
        raise ValueError('Phone number cannot exceed 11 digits')
    
    return phone_clean

def validate_password(password: str, min_length: int = 6) -> str:
    """Validate password strength"""
    if len(password) < min_length:
        raise ValueError(f'Password must have at least {min_length} characters')
    
    if not re.search(r'[a-zA-Z]', password):
        raise ValueError('Password must contain at least one letter')
    
    if not re.search(r'\d', password):
        raise ValueError('Password must contain at least one digit')
    
    return password