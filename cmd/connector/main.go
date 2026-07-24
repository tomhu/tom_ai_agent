// connector — Cell 级接入网关（P0 原型）：终止 agent mTLS、会话管理、指令邮箱、数据出口。
// 用法:
//
//	connector -grpc :18091 -admin :18090 \
//	  -tls-ca pki/dev/ca.crt -tls-cert pki/dev/connector.crt -tls-key pki/dev/connector.key \
//	  -sign-key pki/dev/signer.key
package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
	tlsCA := flag.String("tls-ca", "", "根 CA 证书")
	tlsCert := flag.String("tls-cert", "", "服务端证书")
	tlsKey := flag.String("tls-key", "", "服务端私钥")
	signKey := flag.String("sign-key", "", "指令签名私钥")
	flag.Parse()

	var signer = loadSigner(*signKey)
	srv := connector.NewServer(signer, logSink{})

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

	// 离线清扫（周期标记超时会话）
	go func() {
		for range time.Tick(30 * time.Second) {
			for _, s := range srv.Sessions().OfflineSweep(srv.OfflineTimeout) {
				log.Printf("[event] agent.offline_suspect asset_id=%s session=%s", s.AssetID, s.SessionID)
			}
		}
	}()

	// 管理 API（P0 内部形态；生产对接 Command Service/OpenAPI）
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
		if assetID == "" || req.CmdID == "" || req.Action == "" {
			http.Error(w, "asset_id, cmd_id, action required", http.StatusBadRequest)
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
		if _, err := srv.Mailbox().Cancel(assetID, cmdID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	http.HandleFunc("/admin/result", func(w http.ResponseWriter, r *http.Request) {
		c, ok := srv.Mailbox().Result(r.URL.Query().Get("cmd_id"))
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

	log.Printf("connector admin listening on %s", *adminAddr)
	log.Fatal(http.ListenAndServe(*adminAddr, nil))
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
