// Package register 实现 agent 首启注册与身份管理（设计文档 §8.1/§8.3，M2 版）。
//
// 要点：
//   - asset_id 是平台签发的最终身份，machine-id 等只是证明材料（防克隆撞号）
//   - 注册幂等：enrollment_request_id 持久化，重试返回原身份
//   - M2 走 HTTP /v1/register（mock 注册服务）；gRPC Bootstrap 服务随 proto 冻结替换
package register

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
)

// Identity 持久化的 agent 身份。
type Identity struct {
	AssetID             string `json:"asset_id"`
	EnrollmentRequestID string `json:"enrollment_request_id"`
	RegisteredAt        int64  `json:"registered_at"`
}

type registerRequest struct {
	EnrollmentRequestID string            `json:"enrollment_request_id"`
	BootstrapToken      string            `json:"bootstrap_token"`
	Materials           map[string]string `json:"materials"` // machine_id/hostname/os/arch 等证明材料
}

type registerResponse struct {
	AssetID string `json:"asset_id"`
	Error   string `json:"error,omitempty"`
}

// Module 注册模块：启动时恢复身份，未注册且配置引导凭据则执行注册。
type Module struct {
	cfg      *config.Config
	identity *Identity
	onReady  func(assetID string)
}

func New(cfg *config.Config, onReady func(assetID string)) *Module {
	return &Module{cfg: cfg, onReady: onReady}
}

func (m *Module) Name() string { return "register" }

func (m *Module) identityPath() string {
	return filepath.Join(m.cfg.Agent.DataDir, "identity.json")
}

// LoadIdentity 读取已持久化身份（若存在）。
func (m *Module) LoadIdentity() *Identity {
	data, err := os.ReadFile(m.identityPath())
	if err != nil {
		return nil
	}
	var id Identity
	if json.Unmarshal(data, &id) != nil || id.AssetID == "" {
		return nil
	}
	return &id
}

func (m *Module) saveIdentity(id *Identity) error {
	if err := os.MkdirAll(m.cfg.Agent.DataDir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(id, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.identityPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.identityPath())
}

func (m *Module) Start(ctx context.Context) error {
	// 1. 配置直接指定 asset_id（联调模式）
	if m.cfg.Agent.AssetID != "" {
		slog.Info("using configured asset_id", "asset_id", m.cfg.Agent.AssetID)
		m.onReady(m.cfg.Agent.AssetID)
		return nil
	}
	// 2. 已注册身份恢复
	if id := m.LoadIdentity(); id != nil {
		slog.Info("identity restored", "asset_id", id.AssetID)
		m.onReady(id.AssetID)
		return nil
	}
	// 3. 无引导凭据：匿名运行（仅本地/stdout 调试用）
	token := m.cfg.Register.BootstrapToken
	if token == "" || m.registerBaseURL() == "" {
		slog.Warn("no identity and no bootstrap token; running unregistered (asset_id empty)")
		m.onReady("")
		return nil
	}
	// 4. 执行注册（幂等：enrollment_request_id 复用）
	go m.registerLoop(ctx, token)
	return nil
}

// registerLoop 注册重试循环：成功后回调并退出；失败指数退避。
func (m *Module) registerLoop(ctx context.Context, token string) {
	enrollID := newEnrollmentID()
	// 幂等键复用：若之前注册中断（已生成 enrollID 未持久化），M2 接受重新生成——
	// 服务端幂等键是 (enrollID, csr_pubkey_hash)，正式版在生成后立即持久化。
	backoff := 5 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		assetID, err := m.doRegister(ctx, token, enrollID)
		if err != nil {
			slog.Warn("register failed, retry later", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Minute {
				backoff *= 2
			}
			continue
		}
		id := &Identity{AssetID: assetID, EnrollmentRequestID: enrollID, RegisteredAt: time.Now().UnixMilli()}
		if err := m.saveIdentity(id); err != nil {
			slog.Error("save identity failed", "err", err)
		}
		slog.Info("registered", "asset_id", assetID)
		m.onReady(assetID)
		return
	}
}

// registerBaseURL 注册引导地址：http 模式用 uplink.addr；grpc 模式用 uplink.http_addr 回退。
func (m *Module) registerBaseURL() string {
	switch m.cfg.Uplink.Mode {
	case "http":
		return m.cfg.Uplink.Addr
	case "grpc":
		return m.cfg.Uplink.HTTPAddr
	default:
		return ""
	}
}

func (m *Module) doRegister(ctx context.Context, token, enrollID string) (string, error) {
	materials := collectMaterials()
	reqBody := registerRequest{
		EnrollmentRequestID: enrollID,
		BootstrapToken:      token,
		Materials:           materials,
	}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		m.registerBaseURL()+"/v1/register", bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var rr registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return "", fmt.Errorf("decode register response: %w", err)
	}
	if rr.Error != "" {
		return "", fmt.Errorf("register rejected: %s", rr.Error)
	}
	if rr.AssetID == "" {
		return "", fmt.Errorf("empty asset_id in response (status %s)", resp.Status)
	}
	return rr.AssetID, nil
}

// collectMaterials 采集注册证明材料（非强身份，仅冲突检测输入）。
func collectMaterials() map[string]string {
	mat := map[string]string{}
	if data, err := os.ReadFile("/etc/machine-id"); err == nil {
		mat["machine_id"] = strings.TrimSpace(string(data))
	}
	if hn, err := os.Hostname(); err == nil {
		mat["hostname"] = hn
	}
	if data, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		mat["kernel"] = strings.TrimSpace(string(data))
	}
	mat["arch"] = runtime.GOARCH
	if data, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "PRETTY_NAME=") {
				mat["os"] = strings.Trim(strings.TrimPrefix(line, "PRETTY_NAME="), `"`)
			}
		}
	}
	return mat
}

// newEnrollmentID 生成注册幂等键（crypto/rand，无外部依赖）。
func newEnrollmentID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
