import unittest
import sys
import os
from app.utils.validators import validate_email, validate_full_name,validate_password,validate_phone,validate_role_code,validate_role_name
class TestValidator(unittest.TestCase):
    def setUp(self):
        # Add parent directory to path to import app modules
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
        self.validate_email = validate_email
        self.validate_password_strength = validate_password
        self.validate_full_name = validate_full_name
        self.validate_phone = validate_phone
        self.validate_role_name = validate_role_name
        self.validate_role_code = validate_role_code

    def test_validate_email_valid(self):
        """Test valid email addresses"""
        self.validate_email("xL0bE@example.com")
    
    def test_validate_email_invalid_with_multiple_at_symbols(self):
        """Test invalid email with multiple @ symbols"""
        with self.assertRaisesRegex(ValueError, "Email phải có đúng 1 ký tự @"):
            self.validate_email("xzc@@example.com")
    
    def test_validate_email_format(self):
        """Test invalid email format"""
        with self.assertRaisesRegex(ValueError, "Email không hợp lệ"):
            self.validate_email("a,nc.    @   gmail..com")

    def test_validate_email_length(self):
        with self.assertRaisesRegex(ValueError, "Email không được vượt quá 255 ký tự"):
            self.validate_email("a" * 256 + "@example.com")

    def test_validate_password_length_short(self):
        with self.assertRaisesRegex(ValueError, "Mật khẩu phải có tối thiểu 8 ký tự"):
            self.validate_password_strength("short")

    def test_validate_password_length_long(self):
        with self.assertRaisesRegex(ValueError, "Mật khẩu không được vượt quá 65 ký tự"):
            self.validate_password_strength("a" * 66)

    def test_validate_password_have_letter(self):
        with self.assertRaisesRegex(ValueError, "Mật khẩu phải chứa ít nhất một chữ cái"):
            self.validate_password_strength("12345678")
    
    def test_validate_password_have_digit(self):
        with self.assertRaisesRegex(ValueError, "Mật khẩu phải chứa ít nhất một chữ số"):
            self.validate_password_strength("abcdefgh")
    
    def test_full_name_empty(self):
        """Test empty full name"""
        with self.assertRaisesRegex(ValueError, "Tên không được để trống"):
            self.validate_full_name("   ")
            
    def test_validate_full_name_invalid_characters(self):
        """Test full name with invalid characters"""
        with self.assertRaisesRegex(ValueError, "Tên chỉ được chứa chữ cái và khoảng trắng, không được chứa số hoặc ký tự đặc biệt"):
            self.validate_full_name("John_Doe123")
    
    def test_validate_full_name_multiple_spaces(self):
        """Test full name with multiple consecutive spaces"""
        with self.assertRaisesRegex(ValueError, "Tên không được chứa nhiều khoảng trắng liên tiếp"):
            self.validate_full_name("John  Doe")

    def test_validate_full_name_length(self):
        """Test full name length constraints"""
        with self.assertRaisesRegex(ValueError, "Tên phải có tối thiểu 2 ký tự"):
            self.validate_full_name("A")
        with self.assertRaisesRegex(ValueError, "Tên không được quá 100 ký tự"):
            self.validate_full_name("A" * 101)
    
    def test_validate_phone_invalid_start(self):
        """Test phone number not starting with 0"""
        with self.assertRaisesRegex(ValueError, "Số điện thoại phải bắt đầu bằng số 0"):
            self.validate_phone("1234567890")
    
    def test_validate_phone_length(self):
        """Test phone number length constraints"""
        with self.assertRaisesRegex(ValueError, "Số điện thoại phải có tối thiểu 10 chữ số"):
            self.validate_phone("012345678")
        with self.assertRaisesRegex(ValueError, "Số điện thoại không được quá 11 chữ số"):
            self.validate_phone("012345678901")
    
    def test_validate_phone_valid(self):
        """Test valid phone numbers"""
        self.validate_phone("0123456789")
        self.validate_phone("01234567890")

    def test_validate_phone_letters(self):
        """Test phone number with letters"""
        with self.assertRaisesRegex(ValueError, "Số điện thoại chỉ được chứa các chữ số"):
            self.validate_phone("01234abcde")

    def test_validate_role_name_length(self):
        """Test role name length constraints"""
        with self.assertRaisesRegex(ValueError, "Tên vai trò phải có tối thiểu 2 ký tự"):
            self.validate_role_name("A")
        with self.assertRaisesRegex(ValueError, "Tên vai trò không được quá 100 ký tự"):
            self.validate_role_name("A" * 101)
        with self.assertRaisesRegex(ValueError, "Tên vai trò chỉ được chứa chữ cái và khoảng trắng, không được chứa số hoặc ký tự đặc biệt"):
            self.validate_role_name("Admin123")
        with self.assertRaisesRegex(ValueError, "Tên vai trò không được chứa nhiều khoảng trắng liên tiếp"):
            self.validate_role_name("Admin  Role")
    
    def test_validate_role_code_format(self):
        """Test role code format constraints"""
        with self.assertRaisesRegex(ValueError,"Mã vai trò không được để trống"):
            self.validate_role_code("")
        with self.assertRaisesRegex(ValueError, "Mã vai trò phải bắt đầu bằng chữ cái, không được bắt đầu bằng dấu gạch dưới"):
            self.validate_role_code("_ADMIN")
        with self.assertRaisesRegex(ValueError, "Mã vai trò phải kết thúc bằng chữ cái, không được kết thúc bằng dấu gạch dưới"):
            self.validate_role_code("ADMIN_")
        with self.assertRaisesRegex(ValueError, "Mã vai trò không được chứa nhiều dấu gạch dưới liên tiếp"):
            self.validate_role_code("ADMIN__CODE")
        with self.assertRaisesRegex(ValueError, "Mã vai trò chỉ được chứa chữ cái viết hoa \\(A-Z\\) và dấu gạch dưới \\(_\\)"):
            self.validate_role_code("admin-role")
        with self.assertRaisesRegex(ValueError, "Mã vai trò phải có tối thiểu 2 ký tự"):
            self.validate_role_code("A")
        with self.assertRaisesRegex(ValueError, "Mã vai trò không được quá 50 ký tự"):
            self.validate_role_code("A" * 51)