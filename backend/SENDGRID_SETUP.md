# Hướng dẫn khắc phục lỗi gửi email trên Render

## Vấn đề
Render chặn port 587 (SMTP) nên không thể gửi email qua Gmail SMTP.

## Giải pháp: Sử dụng SendGrid (Miễn phí 100 emails/ngày)

### Bước 1: Đăng ký SendGrid
1. Truy cập: https://signup.sendgrid.com/
2. Đăng ký tài khoản miễn phí
3. Xác thực email

### Bước 2: Tạo API Key
1. Đăng nhập SendGrid
2. Vào **Settings** → **API Keys**
3. Click **Create API Key**
4. Chọn **Full Access** hoặc **Restricted Access** (chỉ cần Mail Send)
5. Copy API Key (chỉ hiện 1 lần)

### Bước 3: Verify Sender Email
1. Vào **Settings** → **Sender Authentication**
2. Click **Verify a Single Sender**
3. Nhập email `manhndhe173383@fpt.edu.vn`
4. Check email và click link xác thực

### Bước 4: Cấu hình Render
1. Vào Render Dashboard → Your Service
2. **Environment** → **Add Environment Variable**
3. Thêm:
   - Key: `SENDGRID_API_KEY`
   - Value: `SG.xxxxxxxxxxxxxxxxxxxxxxxxx` (API key vừa copy)
4. Click **Save Changes**
5. Service sẽ tự động redeploy

### Bước 5: Cài đặt httpx
```bash
pip install httpx
```

Thêm vào `requirements.txt`:
```
httpx==0.27.0
```

## Cách hoạt động
- Code sẽ tự động dùng SendGrid nếu có `SENDGRID_API_KEY`
- Nếu không có, fallback về SMTP (chỉ hoạt động local)
- Trên Render: SendGrid → Success, SMTP → Dev mode (skip)

## Test
```python
# Email sẽ được gửi thật qua SendGrid
await send_otp_email("manh07051@gmail.com")
```

## Giải pháp thay thế

### Option 2: Resend (Khuyến nghị cho production)
- 100 emails/ngày miễn phí
- Dễ setup hơn SendGrid
- https://resend.com

### Option 3: Mailgun
- 5,000 emails/tháng miễn phí (3 tháng đầu)
- https://www.mailgun.com

### Option 4: AWS SES
- 62,000 emails/tháng miễn phí (nếu gửi từ EC2)
- Cần verify domain
