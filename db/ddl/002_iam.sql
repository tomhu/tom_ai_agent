-- 002_iam.sql — IAM/TOTP 首切片（console 运维控制台认证授权；PG10 兼容）。
-- 覆盖：操作员账号（口令 PBKDF2 哈希 + TOTP 密钥）+ 服务端会话（仅存 token 哈希）。
-- role 取值：admin（全权限）/ operator（指令下发与取消）/ auditor（只读审计）。

CREATE SCHEMA IF NOT EXISTS iam;

-- ---------- 操作员 ----------
CREATE TABLE IF NOT EXISTS iam.op_user (
    user_id        BIGSERIAL PRIMARY KEY,
    username       VARCHAR(64)  NOT NULL UNIQUE,
    password_hash  VARCHAR(256) NOT NULL,               -- "pbkdf2$<iter>$<salt_b64>$<hash_b64>"
    role           VARCHAR(16)  NOT NULL,               -- admin / operator / auditor
    totp_secret    VARCHAR(64),                         -- base32（无 padding）；NULL=未启用 TOTP
    totp_confirmed BOOLEAN      NOT NULL DEFAULT false, -- enroll 后经一次正确验证码确认方为生效
    status         VARCHAR(16)  NOT NULL DEFAULT 'active', -- active / disabled
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------- 会话 ----------
-- 只存 token 的 SHA-256 哈希：库泄露不等于会话泄露
CREATE TABLE IF NOT EXISTS iam.session (
    session_id BIGSERIAL PRIMARY KEY,
    token_hash CHAR(64)     NOT NULL UNIQUE,
    username   VARCHAR(64)  NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_token ON iam.session(token_hash);
CREATE INDEX IF NOT EXISTS idx_session_expires ON iam.session(expires_at);
