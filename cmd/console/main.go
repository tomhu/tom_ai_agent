// main.go — console 运维控制台（IAM/TOTP 首切片）。
// 职责：操作员认证（PBKDF2 口令 + RFC 6238 TOTP）、服务端会话、RBAC，
// 并把 connector 的 admin 指令端点（:18090）加 RBAC 后反向代理给运维侧。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/iam"
)

// otpauth issuer 标识（Authenticator 中展示名）
const totpIssuer = "aiops"

type api struct {
	store          *iam.Store
	connectorAdmin string
	sessionTTL     time.Duration
	httpClient     *http.Client
}

// sessionCtx 认证通过后的调用者身份。
type sessionCtx struct {
	Username string
	Role     string
}

func main() {
	addr := flag.String("addr", ":18093", "HTTP 监听地址")
	dsn := flag.String("dsn", "", "PostgreSQL DSN（必填）")
	connectorAdmin := flag.String("connector-admin", "http://127.0.0.1:18090", "connector admin 基址")
	sessionTTL := flag.Duration("session-ttl", 8*time.Hour, "会话有效期")
	flag.Parse()
	if *dsn == "" {
		slog.Error("-dsn required")
		return
	}

	store, err := iam.OpenStore(*dsn)
	if err != nil {
		slog.Error("open store failed", "err", err)
		return
	}
	defer store.Close()

	a := &api{
		store:          store,
		connectorAdmin: strings.TrimRight(*connectorAdmin, "/"),
		sessionTTL:     *sessionTTL,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/users", a.createUser)
	mux.HandleFunc("POST /api/v1/login", a.login)
	mux.HandleFunc("POST /api/v1/totp/enroll", a.auth("", a.totpEnroll))
	mux.HandleFunc("POST /api/v1/totp/confirm", a.auth("", a.totpConfirm))
	mux.HandleFunc("GET /api/v1/whoami", a.auth("", a.whoami))
	mux.HandleFunc("POST /api/v1/command/submit", a.auth(iam.PermCommandSubmit, a.proxy("/admin/command")))
	mux.HandleFunc("POST /api/v1/command/cancel", a.auth(iam.PermCommandCancel, a.proxy("/admin/cancel")))
	mux.HandleFunc("GET /api/v1/command/result", a.auth(iam.PermCommandResult, a.proxy("/admin/result")))
	mux.HandleFunc("GET /api/v1/assets", a.auth(iam.PermAssetView, a.proxy("/admin/sessions")))

	slog.Info("console listening", "addr", *addr, "connector_admin", a.connectorAdmin)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		slog.Error("http server exited", "err", err)
	}
}

// ---------- 公共辅助 ----------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// authenticate 认证：Bearer token → token 哈希查会话 → 过期/用户状态复核。
// 失败时已写响应并返回 nil。
func (a *api) authenticate(w http.ResponseWriter, r *http.Request) *sessionCtx {
	h := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || token == "" {
		writeErr(w, http.StatusUnauthorized, "missing_token")
		return nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	username, expires, err := a.store.FindSession(ctx, iam.TokenHash(token))
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "invalid_token")
		return nil
	}
	if time.Now().After(expires) {
		writeErr(w, http.StatusUnauthorized, "session_expired")
		return nil
	}
	u, err := a.store.FindUser(ctx, username)
	if err != nil || u.Status != "active" {
		writeErr(w, http.StatusUnauthorized, "invalid_token")
		return nil
	}
	return &sessionCtx{Username: username, Role: u.Role}
}

// auth 认证 + 权限中间件；perm 为空表示仅需登录。
func (a *api) auth(perm string, next func(http.ResponseWriter, *http.Request, *sessionCtx)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess := a.authenticate(w, r)
		if sess == nil {
			return
		}
		if perm != "" && !iam.HasPermission(sess.Role, perm) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden", "need": perm})
			return
		}
		next(w, r, sess)
	}
}

// ---------- 用户与登录 ----------

// createUser 创建用户。引导：系统无用户时允许匿名创建首个 admin；
// 之后要求已认证会话且持有 user.manage 权限（admin 经 "*" 通配覆盖）。
func (a *api) createUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	count, err := a.store.UserCount(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	if count == 0 {
		// 引导窗口：首用户必须是 admin，否则系统无人能再建用户
		if req.Role != iam.RoleAdmin {
			writeErr(w, http.StatusBadRequest, "first_user_must_be_admin")
			return
		}
	} else {
		sess := a.authenticate(w, r)
		if sess == nil {
			return
		}
		if !iam.HasPermission(sess.Role, iam.PermUserManage) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden", "need": iam.PermUserManage})
			return
		}
	}

	if req.Username == "" {
		writeErr(w, http.StatusBadRequest, "username_required")
		return
	}
	if len(req.Password) < 8 {
		writeErr(w, http.StatusBadRequest, "password_too_short")
		return
	}
	if !iam.ValidRole(req.Role) {
		writeErr(w, http.StatusBadRequest, "invalid_role")
		return
	}
	hash, err := iam.HashPassword(req.Password)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	if err := a.store.CreateUser(ctx, req.Username, hash, req.Role); err != nil {
		if err == iam.ErrDuplicate {
			writeErr(w, http.StatusConflict, "user_exists")
			return
		}
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	slog.Info("user created", "username", req.Username, "role", req.Role)
	writeJSON(w, http.StatusCreated, map[string]string{"username": req.Username, "role": req.Role})
}

// login 口令 + （已确认时）TOTP 双因子登录，成功签发服务端会话。
func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		TOTPCode string `json:"totp_code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	u, err := a.store.FindUser(ctx, req.Username)
	if err != nil || !iam.VerifyPassword(req.Password, u.PasswordHash) {
		// 用户不存在与口令错误同响应，防账号枚举
		writeErr(w, http.StatusUnauthorized, "invalid_credentials")
		return
	}
	if u.Status != "active" {
		writeErr(w, http.StatusUnauthorized, "user_disabled")
		return
	}
	if u.TOTPConfirmed {
		if req.TOTPCode == "" {
			writeErr(w, http.StatusUnauthorized, "totp_required")
			return
		}
		if !iam.Verify(u.TOTPSecret, req.TOTPCode, time.Now()) {
			writeErr(w, http.StatusUnauthorized, "totp_invalid")
			return
		}
	}
	token, err := iam.NewToken()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	expires, err := a.store.CreateSession(ctx, req.Username, iam.TokenHash(token), a.sessionTTL)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	slog.Info("login ok", "username", req.Username)
	writeJSON(w, http.StatusOK, map[string]string{
		"token":      token,
		"expires_at": expires.UTC().Format(time.RFC3339),
		"role":       u.Role,
	})
}

// ---------- TOTP 注册 ----------

// totpEnroll 生成密钥落库（未确认），返回 secret 与 otpauth 链接。
func (a *api) totpEnroll(w http.ResponseWriter, r *http.Request, sess *sessionCtx) {
	secret, err := iam.GenerateSecret()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.SetTOTPSecret(ctx, sess.Username, secret); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"secret":      secret,
		"otpauth_url": iam.OtpauthURL(totpIssuer, sess.Username, secret),
	})
}

// totpConfirm 用一次正确验证码确认密钥；确认后登录强制要求 TOTP。
func (a *api) totpConfirm(w http.ResponseWriter, r *http.Request, sess *sessionCtx) {
	var req struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	u, err := a.store.FindUser(ctx, sess.Username)
	if err != nil || u.TOTPSecret == "" {
		writeErr(w, http.StatusBadRequest, "totp_not_enrolled")
		return
	}
	if !iam.Verify(u.TOTPSecret, req.Code, time.Now()) {
		writeErr(w, http.StatusBadRequest, "totp_invalid")
		return
	}
	if err := a.store.ConfirmTOTP(ctx, sess.Username); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal")
		return
	}
	slog.Info("totp confirmed", "username", sess.Username)
	writeJSON(w, http.StatusOK, map[string]string{"status": "confirmed"})
}

// whoami 返回当前身份与权限集。
func (a *api) whoami(w http.ResponseWriter, _ *http.Request, sess *sessionCtx) {
	writeJSON(w, http.StatusOK, map[string]any{
		"username":    sess.Username,
		"role":        sess.Role,
		"permissions": iam.PermissionsOf(sess.Role),
	})
}

// ---------- connector 反代 ----------

// proxy 把请求体与 query 原样转发到 connector admin，并回传其状态码与响应体。
func (a *api) proxy(path string) func(http.ResponseWriter, *http.Request, *sessionCtx) {
	return func(w http.ResponseWriter, r *http.Request, sess *sessionCtx) {
		upstream := a.connectorAdmin + path
		if r.URL.RawQuery != "" {
			upstream += "?" + r.URL.RawQuery
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, upstream, r.Body)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal")
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		resp, err := a.httpClient.Do(req)
		if err != nil {
			slog.Warn("connector proxy failed", "path", path, "user", sess.Username, "err", err)
			writeErr(w, http.StatusBadGateway, "connector_unreachable")
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		if err != nil {
			writeErr(w, http.StatusBadGateway, "connector_read_failed")
			return
		}
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	}
}
