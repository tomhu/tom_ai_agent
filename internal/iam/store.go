// store.go — IAM PostgreSQL 持久化（DDL db/ddl/002_iam.sql）。
// 写路径纪律：单语句即单事务（与 platform.Store 一致）；本平台组件共用 aiops 库、按 schema 隔离。
package iam

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

var ErrNotFound = errors.New("not found")
var ErrDuplicate = errors.New("duplicate username")

type Store struct {
	db *sql.DB
}

// OpenStore 打开连接并 ping（platform.Store 未暴露 *sql.DB，iam 自行 sql.Open）。
func OpenStore(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ---------- 操作员 ----------

// UserInfo 登录/鉴权所需的用户视图。
type UserInfo struct {
	PasswordHash  string
	Role          string
	TOTPSecret    string
	TOTPConfirmed bool
	Status        string
}

// CreateUser 创建用户；用户名冲突返回 ErrDuplicate。
func (s *Store) CreateUser(ctx context.Context, username, passwordHash, role string) error {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO iam.op_user(username, password_hash, role) VALUES ($1,$2,$3)
		 ON CONFLICT (username) DO NOTHING`, username, passwordHash, role)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrDuplicate
	}
	return nil
}

// FindUser 按用户名查询；不存在返回 ErrNotFound。
func (s *Store) FindUser(ctx context.Context, username string) (*UserInfo, error) {
	u := &UserInfo{}
	var secret sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT password_hash, role, totp_secret, totp_confirmed, status
		 FROM iam.op_user WHERE username=$1`, username).
		Scan(&u.PasswordHash, &u.Role, &secret, &u.TOTPConfirmed, &u.Status)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	u.TOTPSecret = secret.String
	return u, nil
}

// SetTOTPSecret 写入待确认的 TOTP 密钥；重置 totp_confirmed=false（换钥必须重新确认，
// 否则旧 confirmed 标记会对新密钥误生效）。
func (s *Store) SetTOTPSecret(ctx context.Context, username, secret string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE iam.op_user SET totp_secret=$2, totp_confirmed=false WHERE username=$1`,
		username, secret)
	return err
}

// ConfirmTOTP 验证码校验通过后置 confirmed（登录路径只认 confirmed 的密钥）。
func (s *Store) ConfirmTOTP(ctx context.Context, username string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE iam.op_user SET totp_confirmed=true WHERE username=$1 AND totp_secret IS NOT NULL`,
		username)
	return err
}

// UserCount 用户总数（引导判定：0 时允许匿名创建首个 admin）。
func (s *Store) UserCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM iam.op_user`).Scan(&n)
	return n, err
}

// ---------- 会话 ----------

// CreateSession 落库会话（仅存 token 哈希），返回过期时间。
func (s *Store) CreateSession(ctx context.Context, username, tokenHash string, ttl time.Duration) (time.Time, error) {
	expires := time.Now().Add(ttl)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO iam.session(token_hash, username, expires_at) VALUES ($1,$2,$3)`,
		tokenHash, username, expires)
	if err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

// FindSession 按 token 哈希查询；不存在返回 ErrNotFound（过期判定由调用方做）。
func (s *Store) FindSession(ctx context.Context, tokenHash string) (string, time.Time, error) {
	var username string
	var expires time.Time
	err := s.db.QueryRowContext(ctx,
		`SELECT username, expires_at FROM iam.session WHERE token_hash=$1`, tokenHash).
		Scan(&username, &expires)
	if err == sql.ErrNoRows {
		return "", time.Time{}, ErrNotFound
	}
	if err != nil {
		return "", time.Time{}, err
	}
	return username, expires, nil
}

// DeleteSession 按 token 哈希删除会话（logout）；不存在视为成功（幂等）。
func (s *Store) DeleteSession(ctx context.Context, tokenHash string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM iam.session WHERE token_hash=$1`, tokenHash)
	return err
}

// SessionInfo 会话视图（活跃=未过期）。
type SessionInfo struct {
	SessionID int64     `json:"session_id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// ListSessions 活跃（未过期）会话列表，按创建时间倒序。
func (s *Store) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, username, created_at, expires_at
		 FROM iam.session WHERE expires_at > now() ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionInfo
	for rows.Next() {
		var si SessionInfo
		if err := rows.Scan(&si.SessionID, &si.Username, &si.CreatedAt, &si.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, si)
	}
	return out, rows.Err()
}

// RevokeSession 按 session_id 删除会话；不存在返回 ErrNotFound。
func (s *Store) RevokeSession(ctx context.Context, sessionID int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM iam.session WHERE session_id=$1`, sessionID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteExpiredSessions 清理过期会话，返回删除行数（供定时/启动清理调用）。
func (s *Store) DeleteExpiredSessions(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM iam.session WHERE expires_at < now()`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ---------- 审计 ----------

// AuditEntry 一条审计记录；Target/ClientIP 可空，Detail 为 JSON（空存 NULL）。
type AuditEntry struct {
	AuditID   int64           `json:"audit_id"`
	Actor     string          `json:"actor"`
	Action    string          `json:"action"`
	Target    string          `json:"target,omitempty"`
	Result    string          `json:"result"`
	ClientIP  string          `json:"client_ip,omitempty"`
	Detail    json.RawMessage `json:"detail,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// Audit 写入一条审计记录。调用方异步使用，失败仅 slog.Warn，不得影响请求路径。
func (s *Store) Audit(ctx context.Context, e AuditEntry) error {
	var target, clientIP, detail any
	if e.Target != "" {
		target = e.Target
	}
	if e.ClientIP != "" {
		clientIP = e.ClientIP
	}
	if len(e.Detail) > 0 {
		detail = string(e.Detail)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO iam.audit(actor, action, target, result, client_ip, detail)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		e.Actor, e.Action, target, e.Result, clientIP, detail)
	return err
}

// ListAudit 按时间倒序取最近 limit 条审计记录。
func (s *Store) ListAudit(ctx context.Context, limit int) ([]AuditEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT audit_id, actor, action, target, result, client_ip, detail, created_at
		 FROM iam.audit ORDER BY audit_id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var target, clientIP, detail sql.NullString
		if err := rows.Scan(&e.AuditID, &e.Actor, &e.Action, &target, &e.Result,
			&clientIP, &detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Target = target.String
		e.ClientIP = clientIP.String
		if detail.Valid {
			e.Detail = json.RawMessage(detail.String)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
