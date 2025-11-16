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

def validate_full_name(full_name: Optional[str]) -> Optional[str]:
    """Validate full name - only letters and spaces allowed"""
    if full_name is None:
        return None
    
    # Remove leading/trailing spaces
    name_clean = full_name.strip()
    
    if not name_clean:
        raise ValueError('Tên không được để trống')
    
    if len(name_clean) < 2:
        raise ValueError('Tên phải có tối thiểu 2 ký tự')
    
    if len(name_clean) > 100:
        raise ValueError('Tên không được quá 100 ký tự')
    
    # Only allow letters (including Vietnamese), spaces, and some special characters
    # Vietnamese alphabet includes: àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ
    if not re.match(r'^[a-zA-ZàáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđĐÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ\s]+$', name_clean):
        raise ValueError('Tên chỉ được chứa chữ cái và khoảng trắng, không được chứa số hoặc ký tự đặc biệt')
    
    # Check for multiple consecutive spaces
    if '  ' in name_clean:
        raise ValueError('Tên không được chứa nhiều khoảng trắng liên tiếp')
    
    return name_clean

def validate_role_code(role_code: Optional[str]) -> Optional[str]:
    """Validate role code - only uppercase letters and underscores allowed"""
    if role_code is None:
        return None
    
    # Remove leading/trailing spaces
    code_clean = role_code.strip()
    
    if not code_clean:
        raise ValueError('Mã vai trò không được để trống')
    
    if len(code_clean) < 2:
        raise ValueError('Mã vai trò phải có tối thiểu 2 ký tự')
    
    if len(code_clean) > 50:
        raise ValueError('Mã vai trò không được quá 50 ký tự')
    
    # Only allow uppercase letters (A-Z) and underscores (_)
    if not re.match(r'^[A-Z_]+$', code_clean):
        raise ValueError('Mã vai trò chỉ được chứa chữ cái viết hoa (A-Z) và dấu gạch dưới (_)')
    
    # Must start with a letter, not underscore
    if code_clean.startswith('_'):
        raise ValueError('Mã vai trò phải bắt đầu bằng chữ cái, không được bắt đầu bằng dấu gạch dưới')
    
    # Must end with a letter, not underscore
    if code_clean.endswith('_'):
        raise ValueError('Mã vai trò phải kết thúc bằng chữ cái, không được kết thúc bằng dấu gạch dưới')
    
    # Check for consecutive underscores
    if '__' in code_clean:
        raise ValueError('Mã vai trò không được chứa nhiều dấu gạch dưới liên tiếp')
    
    return code_clean

def validate_role_name(role_name: Optional[str]) -> Optional[str]:
    """Validate role name - only letters and spaces allowed"""
    if role_name is None:
        return None
    
    # Remove leading/trailing spaces
    name_clean = role_name.strip()
    
    if not name_clean:
        raise ValueError('Tên vai trò không được để trống')
    
    if len(name_clean) < 2:
        raise ValueError('Tên vai trò phải có tối thiểu 2 ký tự')
    
    if len(name_clean) > 100:
        raise ValueError('Tên vai trò không được quá 100 ký tự')
    
    # Only allow letters (a-z, A-Z, Vietnamese) and spaces
    if not re.match(r'^[a-zA-ZàáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđĐÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ\s]+$', name_clean):
        raise ValueError('Tên vai trò chỉ được chứa chữ cái và khoảng trắng, không được chứa số hoặc ký tự đặc biệt')
    
    # Check for multiple consecutive spaces
    if '  ' in name_clean:
        raise ValueError('Tên vai trò không được chứa nhiều khoảng trắng liên tiếp')
    
    return name_clean

def validate_email(email: str):
    if not re.match(r'^[^@]+@[^@]+\.[^@.]+(\.[^@.]+)*$', email):
        raise ValueError('Email không hợp lệ')
    return email