-- =========================
-- IAM (Identity & Access)
-- =========================

CREATE TABLE iam_user (
  user_id        BIGSERIAL PRIMARY KEY,
  email          VARCHAR(255) UNIQUE NOT NULL,
  password_hash  TEXT NOT NULL,              -- bcrypt/argon2 hash
  full_name      TEXT,
  phone          VARCHAR(32),
  status         VARCHAR(16) NOT NULL DEFAULT 'active',  -- active|locked|disabled
  last_login_at  TIMESTAMP,
  mfa_enabled    BOOLEAN DEFAULT FALSE,
  mfa_secret     TEXT,                        -- nếu dùng TOTP
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL
);3

CREATE TABLE iam_role (
  role_id     SERIAL PRIMARY KEY,
  role_code   VARCHAR(50) UNIQUE NOT NULL,    -- ADMIN|ANALYST|CUSTOMER ...
  role_name   TEXT NOT NULL,
  description TEXT
);

CREATE TABLE iam_permission (
  perm_id     SERIAL PRIMARY KEY,
  perm_code   VARCHAR(100) UNIQUE NOT NULL,   -- ví dụ: user.read, user.write, data.view, data.export
  perm_name   TEXT NOT NULL,
  module      VARCHAR(50),                    -- user|dataset|model|report...
  action      VARCHAR(50),                    -- read|write|delete|deploy...
  description TEXT
);

-- N-N: user <-> role
CREATE TABLE iam_user_role (
  user_id  BIGINT NOT NULL,
  role_id  INT NOT NULL,
  assigned_at TIMESTAMP NOT NULL,
  PRIMARY KEY (user_id, role_id),
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id),
  FOREIGN KEY (role_id) REFERENCES iam_role(role_id)
);

-- N-N: role <-> permission
CREATE TABLE iam_role_permission (
  role_id INT NOT NULL,
  perm_id INT NOT NULL,
  granted_at TIMESTAMP NOT NULL,
  PRIMARY KEY (role_id, perm_id),
  FOREIGN KEY (role_id) REFERENCES iam_role(role_id),
  FOREIGN KEY (perm_id) REFERENCES iam_permission(perm_id)
);

-- Phiên đăng nhập / refresh token
CREATE TABLE iam_user_session (
  session_id        BIGSERIAL PRIMARY KEY,
  user_id           BIGINT NOT NULL,
  issued_at         TIMESTAMP NOT NULL,
  expires_at        TIMESTAMP NOT NULL,
  refresh_token_hash TEXT,                    -- lưu hash refresh token
  ip_address        VARCHAR(64),
  user_agent        TEXT,
  revoked_at        TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- Token đặt lại mật khẩu
CREATE TABLE iam_password_reset_token (
  token_id    BIGSERIAL PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  token_hash  TEXT NOT NULL,
  expires_at  TIMESTAMP NOT NULL,
  used_at     TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- Token xác thực email cho đăng ký
CREATE TABLE iam_email_verification_token (
  token_id    BIGSERIAL PRIMARY KEY,
  email       VARCHAR(255) NOT NULL,
  token_hash  TEXT NOT NULL,
  expires_at  TIMESTAMP NOT NULL,
  used_at     TIMESTAMP,
  created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- API key cho tích hợp (tùy chọn)
CREATE TABLE iam_api_key (
  api_key_id   BIGSERIAL PRIMARY KEY,
  user_id      BIGINT NOT NULL,
  name         TEXT,
  key_hash     TEXT NOT NULL,
  is_active    BOOLEAN DEFAULT TRUE,
  created_at   TIMESTAMP NOT NULL,
  last_used_at TIMESTAMP,
  expires_at   TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- Nhật ký hành động (Audit)
CREATE TABLE iam_audit_log (
  audit_id     BIGSERIAL PRIMARY KEY,
  user_id      BIGINT,
  action       VARCHAR(100) NOT NULL,        -- ví dụ: LOGIN_SUCCESS, USER_CREATE, MODEL_DEPLOY
  target_type  VARCHAR(100),                 -- User|Dataset|Model|Report...
  target_id    VARCHAR(100),
  details_text TEXT,
  ip_address   VARCHAR(64),
  created_at   TIMESTAMP NOT NULL,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- Ghi nhận lần đăng nhập (thành công/thất bại)
CREATE TABLE iam_login_attempt (
  attempt_id   BIGSERIAL PRIMARY KEY,
  user_id      BIGINT,                       -- có thể null nếu chưa map được email
  email        VARCHAR(255),
  ip_address   VARCHAR(64),
  result       VARCHAR(16) NOT NULL,         -- success|fail
  reason       TEXT,                         -- wrong_password|locked|...
  attempted_at TIMESTAMP NOT NULL,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- =========================
-- Profiles theo vai trò
-- =========================

-- Hồ sơ khách hàng (người dùng kinh doanh/DSS consumer)
CREATE TABLE customer_profile (
  user_id        BIGINT PRIMARY KEY,
  company_name   TEXT,
  tax_code       VARCHAR(64),
  address        TEXT,
  contact_person TEXT,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- Hồ sơ analyst (phân tích/DS/BI)
CREATE TABLE analyst_profile (
  user_id     BIGINT PRIMARY KEY,
  team        VARCHAR(100),                  -- Data Team / BI / ML
  specialty   VARCHAR(100),                  -- Pricing/Forecast/Anomaly...
  seniority   VARCHAR(50),                   -- Junior|Mid|Senior|Lead
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- (Tùy chọn) Hồ sơ admin nếu cần thông tin riêng
CREATE TABLE admin_profile (
  user_id     BIGINT PRIMARY KEY,
  note        TEXT,
  FOREIGN KEY (user_id) REFERENCES iam_user(user_id)
);

-- =========================
-- RBAC tới Dataset (tùy chọn)
-- Liên kết quyền truy cập dataset trong metadata (nếu đã tạo meta_dataset)
-- =========================
CREATE TABLE iam_role_dataset_access (
  role_id      INT NOT NULL,
  dataset_id   BIGINT NOT NULL,              -- meta_dataset.dataset_id
  access_level VARCHAR(16) NOT NULL,         -- read|write|admin
  granted_at   TIMESTAMP NOT NULL,
  PRIMARY KEY (role_id, dataset_id),
  FOREIGN KEY (role_id) REFERENCES iam_role(role_id)
  -- Bạn có thể thêm FK tới meta_dataset(dataset_id) nếu đã import bảng đó:
  -- , FOREIGN KEY (dataset_id) REFERENCES meta_dataset(dataset_id)
);
