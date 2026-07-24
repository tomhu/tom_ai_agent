-- 001_core.sql — P1 落库子集（platform-architecture.md §5 裁剪；PG10 兼容）。
-- 覆盖：注册幂等 + 证书台账 + 指令状态机 + 生命周期事件 + 事务 Outbox。
-- 生产差异：cmd_command.operator_id 引用 iam.op_user（P2 落地）；asset 引用 cmdb_asset（缓建）。

CREATE SCHEMA IF NOT EXISTS cmd;
CREATE SCHEMA IF NOT EXISTS register;

-- ---------- 指令 ----------

CREATE TABLE IF NOT EXISTS cmd.command (
    cmd_id                UUID PRIMARY KEY,
    asset_id              VARCHAR(64) NOT NULL,
    cell_id               VARCHAR(32) NOT NULL DEFAULT 'dev-cell-1',
    action_id             VARCHAR(64) NOT NULL,
    params                JSONB NOT NULL DEFAULT '{}',
    risk_level            VARCHAR(8)  NOT NULL DEFAULT 'low',
    status                VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
    timeout_sec           INT NOT NULL DEFAULT 60,
    expires_at            TIMESTAMPTZ NOT NULL,
    result_payload        BYTEA,                      -- 终态结果 JSON（大结果二期走对象存储）
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmd_asset_time ON cmd.command(asset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cmd_active ON cmd.command(status)
    WHERE status IN ('QUEUED','DISPATCHING','DELIVERED','ACCEPTED','RUNNING','UNKNOWN');

-- 生命周期事件（仅追加）
CREATE TABLE IF NOT EXISTS cmd.event (
    event_id    BIGSERIAL PRIMARY KEY,
    cmd_id      UUID NOT NULL,
    event_type  VARCHAR(32) NOT NULL,
    from_status VARCHAR(20),
    to_status   VARCHAR(20),
    actor       VARCHAR(64),
    detail      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmdevent_cmd ON cmd.event(cmd_id, event_id);

-- 事务 Outbox：投递/取消请求与状态迁移同事务，dispatcher 异步消费
CREATE TABLE IF NOT EXISTS cmd.outbox (
    event_id      BIGSERIAL PRIMARY KEY,
    cmd_id        UUID NOT NULL,
    event_type    VARCHAR(32) NOT NULL,      -- DISPATCH_REQUESTED / CANCEL_REQUESTED
    payload_bytes BYTEA NOT NULL,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INT NOT NULL DEFAULT 0,
    locked_by     VARCHAR(64),
    locked_until  TIMESTAMPTZ,
    published_at  TIMESTAMPTZ,
    last_error    VARCHAR(256)
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON cmd.outbox(available_at) WHERE published_at IS NULL;

-- ---------- 注册 ----------

CREATE TABLE IF NOT EXISTS register.enrollment (
    enrollment_request_id UUID PRIMARY KEY,
    bootstrap_token_hash  CHAR(64) NOT NULL,
    csr_pubkey_sha256     CHAR(64) NOT NULL,
    asset_id              VARCHAR(64),
    cert_id               BIGINT,
    materials             JSONB NOT NULL DEFAULT '{}',
    status                VARCHAR(16) NOT NULL DEFAULT 'processing', -- processing/completed/failed
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS register.agent_certificate (
    cert_id            BIGSERIAL PRIMARY KEY,
    asset_id           VARCHAR(64) NOT NULL,
    serial_no          VARCHAR(64) NOT NULL UNIQUE,
    fingerprint        CHAR(64) NOT NULL UNIQUE,
    issuer_id          VARCHAR(64) NOT NULL DEFAULT 'dev-root-ca',
    not_before         TIMESTAMPTZ NOT NULL,
    not_after          TIMESTAMPTZ NOT NULL,
    status             VARCHAR(16) NOT NULL DEFAULT 'active', -- active/revoked/expired
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cert_asset ON register.agent_certificate(asset_id, status);
