// bootstrap.go — AgentBootstrap gRPC 服务（P1，platform-architecture.md §6.1）。
// Register：server-auth TLS + bootstrap token 认证，幂等 enrollment_request_id；
// 校验 CSR 后由平台 CA 签发客户端证书（CN 强制改写为 asset_id，私钥永不离 agent）。
// 台账：register.enrollment（幂等）+ register.agent_certificate（证书账本），见 db/ddl/001_core.sql。
package connector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
	"github.com/tomhu/tom_ai_agent/internal/platform"
)

// BootstrapServer 注册服务。certTTL 为签发有效期（dev 90 天）。
type BootstrapServer struct {
	agentv1.UnimplementedAgentBootstrapServer

	store       *platform.Store
	caCert      *x509.Certificate
	caKey       ed25519.PrivateKey
	caDER       []byte
	tokenHash   [32]byte // sha256(bootstrap token)，比较用常量时间
	certTTL     time.Duration
	gatewayAddr string // 回给 agent 的接入地址（信息性）
}

func NewBootstrapServer(store *platform.Store, caCert *x509.Certificate, caDER []byte, caKey ed25519.PrivateKey,
	bootstrapToken, gatewayAddr string, certTTL time.Duration) *BootstrapServer {
	return &BootstrapServer{
		store: store, caCert: caCert, caDER: caDER, caKey: caKey,
		tokenHash: sha256.Sum256([]byte(bootstrapToken)),
		certTTL:   certTTL, gatewayAddr: gatewayAddr,
	}
}

func (b *BootstrapServer) Register(ctx context.Context, req *agentv1.RegisterRequest) (*agentv1.RegisterResponse, error) {
	if req.EnrollmentRequestId == "" || req.BootstrapToken == "" {
		return nil, status.Error(codes.InvalidArgument, "enrollment_request_id and bootstrap_token required")
	}
	// token 校验（常量时间）
	sum := sha256.Sum256([]byte(req.BootstrapToken))
	if subtle.ConstantTimeCompare(sum[:], b.tokenHash[:]) != 1 {
		slog.Warn("bootstrap token rejected", "enroll_id", req.EnrollmentRequestId)
		return nil, status.Error(codes.PermissionDenied, "invalid bootstrap token")
	}

	// 幂等：已完成的注册直接重放原签发结果（材料在 enrollment.materials）
	rec, err := b.store.FindEnrollment(ctx, req.EnrollmentRequestId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "find enrollment: %v", err)
	}
	if rec != nil {
		if rec.Status == "completed" && len(rec.CertDER) > 0 {
			slog.Info("enrollment replayed (idempotent)", "enroll_id", req.EnrollmentRequestId, "asset_id", rec.AssetID)
			return &agentv1.RegisterResponse{
				AssetId: rec.AssetID, GatewayAddr: b.gatewayAddr,
				CertificateDer: rec.CertDER, CaDer: b.caDER, NotAfter: rec.NotAfter,
			}, nil
		}
		// processing：上次签发中断，补偿删除后本次重新走完整流程
		if err := b.store.FailEnrollment(ctx, req.EnrollmentRequestId); err != nil {
			return nil, status.Errorf(codes.Internal, "reset stale enrollment: %v", err)
		}
	}

	// CSR 校验（v1.1 起必须携带；CN 忽略，平台强制改写为 asset_id）
	if len(req.CsrDer) == 0 {
		return nil, status.Error(codes.InvalidArgument, "csr_der required (proto v1.1)")
	}
	csr, err := x509.ParseCertificateRequest(req.CsrDer)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse csr: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "csr signature invalid: %v", err)
	}
	pub, ok := csr.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "only Ed25519 CSR keys accepted")
	}
	pubHash := sha256.Sum256(pub)
	// asset_id 由平台签发：绑定 CSR 公钥哈希（同 key 重注册得到同一身份）
	assetID := "asset-" + hex.EncodeToString(pubHash[:8])

	inserted, err := b.store.BeginEnrollment(ctx, req.EnrollmentRequestId,
		hex.EncodeToString(sum[:]), hex.EncodeToString(pubHash[:]), req.Materials)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin enrollment: %v", err)
	}
	if !inserted {
		// 并发同 enrollID：让客户端退避重试，由先到的请求完成签发
		return nil, status.Error(codes.Unavailable, "enrollment in progress, retry")
	}

	certDER, serial, fingerprint, notBefore, notAfter, err := b.sign(pub, assetID)
	if err != nil {
		_ = b.store.FailEnrollment(ctx, req.EnrollmentRequestId)
		return nil, status.Errorf(codes.Internal, "sign certificate: %v", err)
	}
	if err := b.store.CompleteEnrollment(ctx, req.EnrollmentRequestId, assetID,
		serial, fingerprint, notBefore, notAfter, certDER); err != nil {
		_ = b.store.FailEnrollment(ctx, req.EnrollmentRequestId)
		return nil, status.Errorf(codes.Internal, "complete enrollment: %v", err)
	}

	slog.Info("agent registered", "asset_id", assetID, "enroll_id", req.EnrollmentRequestId,
		"hostname", req.Materials["hostname"], "not_after", notAfter.Format(time.RFC3339))
	return &agentv1.RegisterResponse{
		AssetId: assetID, GatewayAddr: b.gatewayAddr,
		CertificateDer: certDER, CaDer: b.caDER, NotAfter: notAfter.Unix(),
	}, nil
}

// RotateCertificate 证书轮换（P1 缓建：待 mTLS 身份复核接入后启用）。
func (b *BootstrapServer) RotateCertificate(ctx context.Context, req *agentv1.RotateCertRequest) (*agentv1.RotateCertResponse, error) {
	return nil, status.Error(codes.Unimplemented, "certificate rotation deferred (post-P1)")
}

// sign 用平台 CA 签发客户端证书：CN=asset_id，ClientAuth，Ed25519。
func (b *BootstrapServer) sign(pub ed25519.PublicKey, assetID string) (der []byte, serial, fingerprint string, notBefore, notAfter time.Time, err error) {
	serialBytes := make([]byte, 16)
	if _, err = rand.Read(serialBytes); err != nil {
		return nil, "", "", time.Time{}, time.Time{}, err
	}
	serialNum := new(big.Int).SetBytes(serialBytes)
	notBefore = time.Now().Add(-5 * time.Minute)
	notAfter = time.Now().Add(b.certTTL)
	tmpl := &x509.Certificate{
		SerialNumber:          serialNum,
		Subject:               pkix.Name{CommonName: assetID},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err = x509.CreateCertificate(rand.Reader, tmpl, b.caCert, pub, b.caKey)
	if err != nil {
		return nil, "", "", time.Time{}, time.Time{}, err
	}
	fp := sha256.Sum256(der)
	return der, fmt.Sprintf("%x", serialNum), hex.EncodeToString(fp[:]), notBefore, notAfter, nil
}
