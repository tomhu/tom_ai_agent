// grpc.go — gRPC Bootstrap 注册（P1，proto v1.1）：agent 本地生成 Ed25519 密钥对，
// CSR 上送，平台签发客户端证书（CN=asset_id）。私钥永不离开 agent。
// 首启无 CA 可验证注册服务端：支持 register.bootstrap_ca_file 校验；
// 缺省 TOFU（信任首次使用，开发态）并以 bootstrap token 为认证因子。
package register

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

// PKIPaths 注册落地后的证书三件套路径。
type PKIPaths struct {
	CA   string
	Cert string
	Key  string
}

func (m *Module) pkiDir() string { return filepath.Join(m.cfg.Agent.DataDir, "pki") }

// PKIPaths 三件套齐备时返回路径，否则 nil。
func (m *Module) PKIPaths() *PKIPaths {
	p := &PKIPaths{
		CA:   filepath.Join(m.pkiDir(), "ca.crt"),
		Cert: filepath.Join(m.pkiDir(), "agent.crt"),
		Key:  filepath.Join(m.pkiDir(), "agent.key"),
	}
	for _, f := range []string{p.CA, p.Cert, p.Key} {
		if _, err := os.Stat(f); err != nil {
			return nil
		}
	}
	return p
}

// EnsureIdentity 同步确保身份就绪（gRPC bootstrap 模式启动前置）：
// 已有身份+证书则直接恢复；否则阻塞注册直到成功或 ctx 取消。
// 非 bootstrap 配置（无 token/addr）直接返回，交回 Start 的异步流程。
func (m *Module) EnsureIdentity(ctx context.Context) error {
	if id := m.LoadIdentity(); id != nil && m.PKIPaths() != nil {
		slog.Info("identity restored", "asset_id", id.AssetID)
		// 证书临期轮换检查（fail-open：失败仅告警，不阻断启动）
		rotCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_ = m.MaybeRotate(rotCtx)
		cancel()
		m.onReady(id.AssetID)
		return nil
	}
	token := m.cfg.Register.BootstrapToken
	if token == "" || m.cfg.Register.BootstrapAddr == "" {
		return nil
	}
	enrollID := newEnrollmentID()
	backoff := 5 * time.Second
	for {
		assetID, notAfter, err := m.doRegisterGRPC(ctx, token, enrollID)
		if err == nil {
			id := &Identity{AssetID: assetID, EnrollmentRequestID: enrollID,
				RegisteredAt: time.Now().UnixMilli(), CertNotAfter: notAfter}
			if err := m.saveIdentity(id); err != nil {
				return fmt.Errorf("save identity: %w", err)
			}
			slog.Info("registered via gRPC bootstrap", "asset_id", assetID)
			m.onReady(assetID)
			return nil
		}
		slog.Warn("bootstrap register failed, retry later", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 60*time.Second {
			backoff *= 2
		}
	}
}

// doRegisterGRPC 单次 gRPC 注册尝试：密钥对+CSR → Register → 证书三件套落盘。
// 返回 asset_id 与证书到期时间（Unix 秒，写入 identity 驱动轮换）。
func (m *Module) doRegisterGRPC(ctx context.Context, token, enrollID string) (string, int64, error) {
	priv, csrDER, err := newKeyAndCSR("pending-" + enrollID[:8]) // 平台强制改写 CN=asset_id
	if err != nil {
		return "", 0, err
	}

	tlsConf := &tls.Config{MinVersion: tls.VersionTLS13}
	if caFile := m.cfg.Register.BootstrapCAFile; caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return "", 0, fmt.Errorf("read bootstrap ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return "", 0, fmt.Errorf("bootstrap ca file: no valid certificates")
		}
		tlsConf.RootCAs = pool
		tlsConf.ServerName = m.cfg.Uplink.ServerName
	} else {
		tlsConf.InsecureSkipVerify = true // TOFU（仅开发）：认证因子是 bootstrap token
		slog.Warn("bootstrap TLS server verification skipped (TOFU, dev only)")
	}

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, m.cfg.Register.BootstrapAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)), grpc.WithBlock())
	if err != nil {
		return "", 0, fmt.Errorf("dial bootstrap: %w", err)
	}
	defer conn.Close()

	resp, err := agentv1.NewAgentBootstrapClient(conn).Register(dialCtx, &agentv1.RegisterRequest{
		EnrollmentRequestId: enrollID,
		BootstrapToken:      token,
		Materials:           collectMaterials(),
		CsrDer:              csrDER,
	})
	if err != nil {
		return "", 0, err
	}
	if resp.AssetId == "" || len(resp.CertificateDer) == 0 || len(resp.CaDer) == 0 {
		return "", 0, fmt.Errorf("incomplete register response (asset_id/cert/ca)")
	}

	if err := m.persistPKI(priv, resp.CertificateDer, resp.CaDer); err != nil {
		return "", 0, fmt.Errorf("persist pki: %w", err)
	}
	slog.Info("certificate issued", "asset_id", resp.AssetId,
		"not_after", time.Unix(resp.NotAfter, 0).Format(time.RFC3339))
	return resp.AssetId, resp.NotAfter, nil
}

// MaybeRotate 证书临期自动轮换（M5b）：identity 记录的 cert_not_after 剩余不足
// register.rotate_before_days（缺省 30 天）时，用当前有效客户端证书走 mTLS
// 向接入地址上的 AgentBootstrap 换新证书。fail-open：任何失败仅告警返回 nil。
func (m *Module) MaybeRotate(ctx context.Context) error {
	id := m.LoadIdentity()
	if id == nil || id.CertNotAfter <= 0 {
		return nil // 无到期信息（HTTP 回退注册路径）：不轮换
	}
	pki := m.PKIPaths()
	if pki == nil {
		return nil
	}
	beforeDays := m.cfg.Register.RotateBeforeDays
	if beforeDays <= 0 {
		beforeDays = 30
	}
	remaining := time.Until(time.Unix(id.CertNotAfter, 0))
	if remaining > time.Duration(beforeDays)*24*time.Hour {
		return nil // 未到轮换窗口
	}
	if err := m.rotate(ctx, id, pki); err != nil {
		slog.Warn("certificate rotation failed (fail-open, old cert still valid)",
			"asset_id", id.AssetID, "err", err)
	}
	return nil
}

// rotate 执行一次轮换：新密钥对+CSR → mTLS RotateCertificate → 三件套落盘（ca.crt 不变）→ identity 更新。
func (m *Module) rotate(ctx context.Context, id *Identity, pki *PKIPaths) error {
	priv, csrDER, err := newKeyAndCSR("rotate-" + id.AssetID) // CN 仅占位，平台强制改写
	if err != nil {
		return err
	}
	// mTLS 凭据：当前有效客户端证书 + 本地 CA（参照 uplink dialOption）
	cert, err := tls.LoadX509KeyPair(pki.Cert, pki.Key)
	if err != nil {
		return fmt.Errorf("load client cert/key: %w", err)
	}
	caPEM, err := os.ReadFile(pki.CA)
	if err != nil {
		return fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("ca file: no valid certificates")
	}
	caBlock, _ := pem.Decode(caPEM)
	if caBlock == nil {
		return fmt.Errorf("ca file: no PEM block")
	}
	serverName := m.cfg.Uplink.ServerName
	if serverName == "" {
		host, _, err := net.SplitHostPort(m.cfg.Uplink.Addr)
		if err != nil {
			return fmt.Errorf("parse uplink.addr: %w", err)
		}
		serverName = host
	}

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, m.cfg.Uplink.Addr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			ServerName:   serverName,
			MinVersion:   tls.VersionTLS13, // Ed25519 + TLS1.3（信创基线）
		})), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}
	defer conn.Close()

	resp, err := agentv1.NewAgentBootstrapClient(conn).RotateCertificate(dialCtx, &agentv1.RotateCertRequest{
		AssetId: id.AssetID, CsrDer: csrDER,
	})
	if err != nil {
		return fmt.Errorf("rotate rpc: %w", err)
	}
	if len(resp.CertificateDer) == 0 || resp.NotAfter <= 0 {
		return fmt.Errorf("incomplete rotate response")
	}

	if err := m.persistPKI(priv, resp.CertificateDer, caBlock.Bytes); err != nil {
		return fmt.Errorf("persist pki: %w", err)
	}
	id.CertNotAfter = resp.NotAfter
	if err := m.saveIdentity(id); err != nil {
		return fmt.Errorf("save identity: %w", err)
	}
	slog.Info("certificate rotated", "asset_id", id.AssetID,
		"not_after", time.Unix(resp.NotAfter, 0).Format(time.RFC3339))
	return nil
}

// newKeyAndCSR 生成 Ed25519 密钥对与 PKCS#10 CSR（注册/轮换共用；CN 仅占位，平台强制改写为 asset_id）。
func newKeyAndCSR(cn string) (ed25519.PrivateKey, []byte, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:            pkix.Name{CommonName: cn},
		SignatureAlgorithm: x509.PureEd25519,
	}, priv)
	if err != nil {
		return nil, nil, fmt.Errorf("create csr: %w", err)
	}
	return priv, csrDER, nil
}

// persistPKI 原子落盘：agent.key(0600) / agent.crt / ca.crt。先写临时文件再 rename。
func (m *Module) persistPKI(priv ed25519.PrivateKey, certDER, caDER []byte) error {
	dir := m.pkiDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}
	files := []struct {
		name string
		pem  *pem.Block
		mode os.FileMode
	}{
		{"agent.key", &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}, 0o600},
		{"agent.crt", &pem.Block{Type: "CERTIFICATE", Bytes: certDER}, 0o644},
		{"ca.crt", &pem.Block{Type: "CERTIFICATE", Bytes: caDER}, 0o644},
	}
	for _, f := range files {
		tmp := filepath.Join(dir, f.name+".tmp")
		if err := os.WriteFile(tmp, pem.EncodeToMemory(f.pem), f.mode); err != nil {
			return err
		}
		if err := os.Rename(tmp, filepath.Join(dir, f.name)); err != nil {
			return err
		}
	}
	return nil
}
