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
        raise ValueError('Số điện thoại chỉ được chứa các chữ số')
    
    if not phone_clean.startswith('0'):
        raise ValueError('Số điện thoại phải bắt đầu bằng số 0')
    
    if len(phone_clean) < 10:
        raise ValueError('Số điện thoại phải có tối thiểu 10 chữ số')
    
    if len(phone_clean) > 11:
        raise ValueError('Số điện thoại không được quá 11 chữ số')
    
    return phone_clean

def validate_password(password: str, min_length: int = 6) -> str:
    """Validate password strength"""
    if len(password) < min_length:
        raise ValueError(f'Mật khẩu phải có tối thiểu {min_length} ký tự')
    
    if not re.search(r'[a-zA-Z]', password):
        raise ValueError('Mật khẩu phải chứa ít nhất một chữ cái')
    
    if not re.search(r'\d', password):
        raise ValueError('Mật khẩu phải chứa ít nhất một chữ số')
    
    return password