// store.go — IAM PostgreSQL 持久化（DDL db/ddl/002_iam.sql）。
// 写路径纪律：单语句即单事务（与 platform.Store 一致）；本平台组件共用 aiops 库、按 schema 隔离。
package iam

import (
	"context"
	"database/sql"
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

// DeleteExpiredSessions 清理过期会话，返回删除行数（供定时/启动清理调用）。
func (s *Store) DeleteExpiredSessions(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM iam.session WHERE expires_at < now()`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
