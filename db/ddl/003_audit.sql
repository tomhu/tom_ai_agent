-- 003_audit.sql — console 审计日志（PG10 兼容）。
-- 覆盖：管控台全部写操作与认证/越权失败的审计留痕。
-- actor 为操作者用户名，匿名引导（首 admin 创建）与未认证失败记 'anonymous'；
-- action 形如 auth.login / auth.totp_enroll / user.create / command.submit /
-- command.cancel / session.revoke；result 取值 ok / denied / error。

CREATE TABLE IF NOT EXISTS iam.audit (
    audit_id   BIGSERIAL PRIMARY KEY,
    actor      VARCHAR(64)  NOT NULL,              -- 操作者用户名；匿名引导记 'anonymous'
    action     VARCHAR(64)  NOT NULL,              -- auth.login / user.create / command.submit ...
    target     VARCHAR(128),                       -- 操作对象（cmd_id、被操作用户名、session_id 等）
    result     VARCHAR(16)  NOT NULL,              -- ok / denied / error
    client_ip  VARCHAR(64),                        -- 请求来源（RemoteAddr）
    detail     JSONB,                              -- 附加上下文（拒绝原因、上游状态码等）
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_actor_time ON iam.audit(actor, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_time ON iam.audit(created_at);
