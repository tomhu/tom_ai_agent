// mtlsprobe — mTLS 连通性探针：模拟 agent 拨号 gRPC，发送 Hello，期待 Welcome。
// 用法: mtlsprobe -addr localhost:18081 -ca pki/dev/ca.crt -cert pki/dev/agent.crt -key pki/dev/agent.key -asset a-mock-000001 -server-name localhost
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

func main() {
	addr := flag.String("addr", "localhost:18081", "gRPC 地址")
	ca := flag.String("ca", "", "根 CA")
	cert := flag.String("cert", "", "客户端证书")
	key := flag.String("key", "", "客户端私钥")
	serverName := flag.String("server-name", "localhost", "TLS ServerName")
	asset := flag.String("asset", "a-mock-000001", "asset_id")
	flag.Parse()

	caPEM, err := os.ReadFile(*ca)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	c, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{c},
		RootCAs:      pool,
		ServerName:   *serverName,
		MinVersion:   tls.VersionTLS13,
	})))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := agentv1.NewAgentGatewayClient(conn).Control(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Send(&agentv1.AgentControlFrame{Frame: &agentv1.AgentControlFrame_Hello{
		Hello: &agentv1.AgentHello{AssetId: *asset, BootId: "probe", AgentVersion: "probe-0"}}}); err != nil {
		log.Fatal(err)
	}
	f, err := s.Recv()
	if err != nil {
		log.Fatal(err)
	}
	w := f.GetWelcome()
	if w == nil {
		log.Fatalf("expected welcome, got %T", f.Frame)
	}
	fmt.Printf("PROBE OK: session=%s epoch=%d server_time=%d\n", w.SessionId, w.SessionEpoch, w.ServerTime)
}
