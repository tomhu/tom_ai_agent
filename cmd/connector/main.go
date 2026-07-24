// connector — Cell 级接入网关：终止 agent mTLS、会话管理、指令邮箱、数据出口。
// P1：指令状态机落库（-dsn 启用，写路径 状态迁移+事件+Outbox 单事务）+ AgentBootstrap 注册服务。
// 用法:
//
//	connector -grpc :18091 -admin :18090 -bootstrap-grpc :18092 \
//	  -tls-ca pki/dev/ca.crt -tls-cert pki/dev/connector.crt -tls-key pki/dev/connector.key \
//	  -sign-key pki/dev/signer.key \
//	  -dsn "postgres://aiops:***@127.0.0.1:5432/aiops?sslmode=disable" \
//	  -ca-key pki/dev/ca.key -bootstrap-token <token> -gateway-addr <host>:18091
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/tomhu/tom_ai_agent/internal/authenv"
	"github.com/tomhu/tom_ai_agent/internal/connector"
	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
	"github.com/tomhu/tom_ai_agent/internal/platform"
)

// logSink P0 数据出口：结构化日志（P1 换 Kafka producer，接口不变）。
type logSink struct{}

func (logSink) MetricBatch(b *agentv1.MetricBatch) error {
	log.Printf("[metrics] asset=%s seq=%d samples=%d", b.AssetId, b.Sequence, len(b.Samples))
	return nil
}

func (logSink) Report(kind, id string, payload []byte) error {
	log.Printf("[report:%s] id=%s bytes=%d", kind, id, len(payload))
	return nil
}

func (logSink) Event(eventType string, attrs map[string]string) error {
	log.Printf("[event] %s %v", eventType, attrs)
	return nil
}

func main() {
	grpcAddr := flag.String("grpc", ":18091", "agent 接入地址（mTLS）")
	adminAddr := flag.String("admin", ":18090", "管理 API 地址")
	bootstrapAddr := flag.String("bootstrap-grpc", "", "注册服务地址（server-auth TLS；空=不启用）")
	tlsCA := flag.String("tls-ca", "", "根 CA 证书")
	tlsCert := flag.String("tls-cert", "", "服务端证书")
	tlsKey := flag.String("tls-key", "", "服务端私钥")
	signKey := flag.String("sign-key", "", "指令签名私钥")
	dsn := flag.String("dsn", "", "PostgreSQL DSN（空=P0 纯内存，不落库）")
	caKeyFile := flag.String("ca-key", "", "CA 私钥（PKCS8 PEM，注册签发用）")
	bootstrapToken := flag.String("bootstrap-token", "", "注册引导 token")
	gatewayAddr := flag.String("gateway-addr", "", "回给 agent 的接入地址（信息性，默认取 -grpc）")
	flag.Parse()

	// 持久化（可选）：启用后指令走 cmd.command/event/outbox 状态机
	var store *platform.Store
	if *dsn != "" {
		st, err := platform.OpenStore(*dsn)
		if err != nil {
			log.Fatalf("open store: %v", err)
		}
		defer st.Close()
		store = st
		log.Printf("command state machine persisted (dsn enabled)")
	}

	signer := loadSigner(*signKey)
	srv := connector.NewServer(signer, logSink{})
	if store != nil {
		srv.SetStore(store)
		go outboxDispatcher(store, srv)
	}

	// gRPC 接入（mTLS 强制）
	creds, err := serverMTLS(*tlsCA, *tlsCert, *tlsKey)
	if err != nil {
		log.Fatalf("mtls: %v", err)
	}
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.Creds(creds))
	agentv1.RegisterAgentGatewayServer(gs, srv)
	go func() {
		log.Printf("connector gRPC(mTLS) listening on %s", *grpcAddr)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// 注册服务（server-auth TLS：agent 尚无证书，凭 bootstrap token 认证）
	if *bootstrapAddr != "" {
		if store == nil || *caKeyFile == "" || *bootstrapToken == "" {
			log.Fatalf("bootstrap requires -dsn, -ca-key and -bootstrap-token")
		}
		caCert, caDER, err := loadCACert(*tlsCA)
		if err != nil {
			log.Fatalf("load ca cert: %v", err)
		}
		caKey, err := authenv.LoadPrivateKeyPEM(*caKeyFile)
		if err != nil {
			log.Fatalf("load ca key: %v", err)
		}
		adv := *gatewayAddr
		if adv == "" {
			adv = *grpcAddr
		}
		bs := connector.NewBootstrapServer(store, caCert, caDER, caKey, *bootstrapToken, adv, 90*24*time.Hour)
		blis, err := net.Listen("tcp", *bootstrapAddr)
		if err != nil {
			log.Fatalf("bootstrap listen: %v", err)
		}
		bgs := grpc.NewServer(grpc.Creds(serverAuthTLS(*tlsCert, *tlsKey)))
		agentv1.RegisterAgentBootstrapServer(bgs, bs)
		go func() {
			log.Printf("connector bootstrap(server-auth TLS) listening on %s", *bootstrapAddr)
			if err := bgs.Serve(blis); err != nil {
				log.Fatalf("bootstrap serve: %v", err)
			}
		}()
	}

	// 离线清扫（周期标记超时会话）
	go func() {
		for range time.Tick(30 * time.Second) {
			for _, s := range srv.Sessions().OfflineSweep(srv.OfflineTimeout) {
				log.Printf("[event] agent.offline_suspect asset_id=%s session=%s", s.AssetID, s.SessionID)
			}
		}
	}()

	adminMux(srv, store)
	log.Printf("connector admin listening on %s", *adminAddr)
	log.Fatal(http.ListenAndServe(*adminAddr, nil))
}

// outboxDispatcher 消费 cmd.outbox：DISPATCH_REQUESTED → 邮箱投递；CANCEL_REQUESTED → 取消。
// 崩溃安全：抢占锁 30s 过期自动释放，至少一次投递（邮箱按 cmd_id 幂等去重）。
func outboxDispatcher(store *platform.Store, srv *connector.Server) {
	for range time.Tick(300 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		entries, err := store.FetchOutbox(ctx, "connector-1", 32)
		if err != nil {
			log.Printf("outbox fetch: %v", err)
			cancel()
			continue
		}
		for _, e := range entries {
			if err := dispatchOutbox(ctx, store, srv, e); err != nil {
				log.Printf("outbox dispatch cmd=%s: %v", e.CmdID, err)
				_ = store.MarkOutboxError(ctx, e.EventID, err.Error())
				continue
			}
			_ = store.MarkPublished(ctx, e.EventID)
		}
		cancel()
	}
}

func dispatchOutbox(ctx context.Context, store *platform.Store, srv *connector.Server, e platform.OutboxEntry) error {
	var p struct {
		CmdID      string            `json:"cmd_id"`
		AssetID    string            `json:"asset_id"`
		Action     string            `json:"action"`
		Params     map[string]string `json:"params"`
		TimeoutSec int               `json:"timeout_sec"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	switch e.EventType {
	case "DISPATCH_REQUESTED":
		if err := srv.Mailbox().Submit(p.AssetID, p.CmdID, p.Action, p.Params, p.TimeoutSec); err != nil {
			if err == connector.ErrDupCmdID {
				return nil // 重投幂等
			}
			return err // 邮箱满等：留待重试
		}
		return store.Transition(ctx, p.CmdID, "dispatching", "DISPATCHING", "connector", nil)
	case "CANCEL_REQUESTED":
		_, _ = srv.Mailbox().Cancel(p.AssetID, p.CmdID) // 已终态则忽略
		return nil
	default:
		return fmt.Errorf("unknown outbox event %q", e.EventType)
	}
}

func adminMux(srv *connector.Server, store *platform.Store) {
	http.HandleFunc("/admin/command", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CmdID      string            `json:"cmd_id"`
			Action     string            `json:"action"`
			Params     map[string]string `json:"params"`
			TimeoutSec int               `json:"timeout_sec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assetID := r.URL.Query().Get("asset_id")
		if assetID == "" || req.Action == "" {
			http.Error(w, "asset_id and action required", http.StatusBadRequest)
			return
		}
		if req.TimeoutSec <= 0 {
			req.TimeoutSec = 60
		}
		if store != nil {
			// 落库模式：cmd_id 必须 UUID（缺省/非法则平台生成并返回）
			cmdID := req.CmdID
			if !isUUID(cmdID) {
				cmdID = platform.NewUUID()
			}
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			err := store.SubmitCommand(ctx, cmdID, assetID, req.Action, req.Params, req.TimeoutSec, 5*time.Minute)
			if err == platform.ErrDuplicate {
				http.Error(w, "duplicate cmd_id", http.StatusConflict)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{"cmd_id": cmdID})
			return
		}
		if req.CmdID == "" {
			http.Error(w, "cmd_id required", http.StatusBadRequest)
			return
		}
		if err := srv.Mailbox().Submit(assetID, req.CmdID, req.Action, req.Params, req.TimeoutSec); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	http.HandleFunc("/admin/cancel", func(w http.ResponseWriter, r *http.Request) {
		assetID, cmdID := r.URL.Query().Get("asset_id"), r.URL.Query().Get("cmd_id")
		if store != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			if err := store.CancelCommand(ctx, cmdID); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = srv.Mailbox().Cancel(assetID, cmdID) // 即时取消内存态（outbox 兜底）
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if _, err := srv.Mailbox().Cancel(assetID, cmdID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	// /admin/result：落库模式返回状态机视图（状态+事件+结果），否则返回邮箱结果体
	http.HandleFunc("/admin/result", func(w http.ResponseWriter, r *http.Request) {
		cmdID := r.URL.Query().Get("cmd_id")
		if store != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			ci, err := store.CommandStatus(ctx, cmdID)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"cmd_id": ci.CmdID, "asset_id": ci.AssetID, "action": ci.Action,
					"status": ci.Status, "result": json.RawMessage(ci.Result), "events": ci.Events,
				})
				return
			}
		}
		c, ok := srv.Mailbox().Result(cmdID)
		if !ok || c.State != connector.StateTerminal {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(c.Result)
	})
	http.HandleFunc("/admin/sessions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(srv.Sessions().Online())
	})
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F') {
				return false
			}
		}
	}
	return true
}

func loadSigner(path string) ed25519.PrivateKey {
	if path == "" {
		log.Printf("WARN: no -sign-key, envelopes unsigned (dev only)")
		return nil
	}
	k, err := authenv.LoadPrivateKeyPEM(path)
	if err != nil {
		log.Fatalf("load sign key: %v", err)
	}
	return k
}

func loadCACert(path string) (*x509.Certificate, []byte, error) {
	caPEM, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(caPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("ca file: no PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, block.Bytes, nil
}

// serverAuthTLS 注册服务：仅服务端证书（agent 首启无客户端证书，凭 token 认证）。
func serverAuthTLS(certFile, keyFile string) credentials.TransportCredentials {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("load server cert: %v", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	})
}

func serverMTLS(caFile, certFile, keyFile string) (credentials.TransportCredentials, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca file: no valid certificates")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}), nil
}
