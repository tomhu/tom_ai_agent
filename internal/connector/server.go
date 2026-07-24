// server.go — Connector gRPC 服务端（P0 原型，platform-architecture.md §3/§6）。
// 与 mockgateway 的区别：这是生产形态组件——会话注册表（epoch fencing + replace_old）、
// 指令邮箱（离线缓存+TTL）、事件/指标 Sink 抽象（P1 接 Kafka）。
package connector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/tomhu/tom_ai_agent/internal/authenv"
	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
	"github.com/tomhu/tom_ai_agent/internal/platform"
)

// Sink 下行数据出口抽象（P0 日志实现；P1 Kafka producer）。
type Sink interface {
	MetricBatch(b *agentv1.MetricBatch) error
	Report(kind string, itemID string, payload []byte) error
	Event(eventType string, attrs map[string]string) error
}

type Server struct {
	agentv1.UnimplementedAgentGatewayServer

	sessions *SessionRegistry
	mailbox  *Mailbox
	sink     Sink
	signer   ed25519.PrivateKey // nil=不签名（仅开发）
	store    *platform.Store    // nil=P0 纯内存（不Persist）

	OfflineTimeout time.Duration
}

func NewServer(signer ed25519.PrivateKey, sink Sink) *Server {
	return &Server{
		sessions:       NewSessionRegistry(),
		mailbox:        NewMailbox(64, 5*time.Minute),
		sink:           sink,
		signer:         signer,
		OfflineTimeout: 90 * time.Second,
	}
}

// SetStore 启用指令状态机落库（P1）。nil 保持 P0 内存行为。
func (s *Server) SetStore(st *platform.Store) { s.store = st }

// persist 落库辅助：库未启用或指令不在库（开发态直投邮箱）仅记日志，不阻断数据面。
func (s *Server) persist(event string, fn func(ctx context.Context) error) {
	if s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fn(ctx); err != nil && err != platform.ErrNotFound {
		slog.Warn("command persist failed", "event", event, "err", err)
	}
}

func (s *Server) Mailbox() *Mailbox             { return s.mailbox }
func (s *Server) Sessions() *SessionRegistry    { return s.sessions }

// ---------- 控制流 ----------

func (s *Server) Control(stream agentv1.AgentGateway_ControlServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("first frame must be AgentHello")
	}
	// mTLS 身份复核：证书 CN == asset_id
	if p, ok := peer.FromContext(stream.Context()); ok {
		if ti, ok2 := p.AuthInfo.(credentials.TLSInfo); ok2 && len(ti.State.PeerCertificates) > 0 {
			if cn := ti.State.PeerCertificates[0].Subject.CommonName; cn != hello.AssetId {
				_ = s.sink.Event("security.cert_cn_mismatch", map[string]string{
					"asset_id": hello.AssetId, "cert_cn": cn})
				return fmt.Errorf("certificate CN %q != asset_id %q", cn, hello.AssetId)
			}
		}
	}

	sess, old := s.sessions.Register(hello.AssetId, newSessionID(), hello.AgentVersion, hello.ActionCatalogVersion)
	if old != nil {
		slog.Warn("duplicate connection replaced", "asset_id", hello.AssetId,
			"old_session", old.SessionID, "new_session", sess.SessionID)
		_ = s.sink.Event("security.duplicate_connection", map[string]string{
			"asset_id": hello.AssetId, "old_session": old.SessionID, "policy": "replace_old"})
	}
	defer s.sessions.Unregister(sess)

	if err := stream.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Welcome{
		Welcome: &agentv1.GatewayWelcome{
			SessionId: sess.SessionID, SessionEpoch: sess.Epoch,
			ServerTime: time.Now().UnixMilli(), HeartbeatIntervalSec: 15,
		}}}); err != nil {
		return err
	}
	slog.Info("session up", "asset_id", hello.AssetId, "session", sess.SessionID, "epoch", sess.Epoch)
	_ = s.sink.Event("agent.online", map[string]string{
		"asset_id": hello.AssetId, "agent_version": hello.AgentVersion})

	recvErr := make(chan error, 1)
	go func() {
		for {
			f, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			switch fr := f.Frame.(type) {
			case *agentv1.AgentControlFrame_Heartbeat:
				s.sessions.Touch(hello.AssetId, sess.Epoch)
			case *agentv1.AgentControlFrame_CommandAck:
				slog.Info("command ack", "asset_id", hello.AssetId,
					"cmd_id", fr.CommandAck.CmdId, "accepted", fr.CommandAck.Accepted)
				if !fr.CommandAck.Accepted {
					to := fr.CommandAck.RejectReason
					if to == "" {
						to = "REJECTED"
					}
					s.persist("rejected", func(ctx context.Context) error {
						return s.store.CompleteCommand(ctx, fr.CommandAck.CmdId, to, nil)
					})
				}
			}
		}
	}()

	// 派发循环：信箱有货或被栅栏化/断开前持续
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-sess.Fenced():
			_ = stream.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Fence{
				Fence: &agentv1.FenceNotice{SessionEpoch: sess.Epoch, Reason: "replaced_by_new_connection"}}})
			slog.Info("session fenced", "asset_id", hello.AssetId, "session", sess.SessionID)
			return nil
		case err := <-recvErr:
			if err != io.EOF {
				slog.Warn("control recv ended", "asset_id", hello.AssetId, "err", err)
			}
			return nil
		default:
		}

		cmds, cancels := s.mailbox.NextDispatch(hello.AssetId)
		for _, c := range cmds {
			env := &agentv1.CommandEnvelope{
				CmdId: c.CmdID, Action: c.Action, Params: c.Params,
				TimeoutSec: uint32(c.TimeoutSec),
				IssuedAt:   time.Now().UnixMilli(),
				ExpiresAt:  c.ExpiresAt.UnixMilli(),
			}
			if s.signer != nil {
				env.Nonce = make([]byte, 16)
				_, _ = rand.Read(env.Nonce)
				authenv.Sign(s.signer, env)
			}
			if err := stream.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Command{Command: env}}); err != nil {
				return err
			}
			slog.Info("command dispatched", "asset_id", hello.AssetId, "cmd_id", c.CmdID, "action", c.Action)
			s.persist("delivered", func(ctx context.Context) error {
				return s.store.Transition(ctx, c.CmdID, "delivered", "DELIVERED", "connector", nil)
			})
		}
		for _, id := range cancels {
			if err := stream.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Cancel{
				Cancel: &agentv1.CancelCommand{CmdId: id}}}); err != nil {
				return err
			}
			slog.Info("cancel dispatched", "asset_id", hello.AssetId, "cmd_id", id)
		}
		if len(cmds) == 0 && len(cancels) == 0 {
			s.mailbox.Wait(hello.AssetId, time.Second) // 无货阻塞等信号
		}
	}
}

// ---------- 指标流 ----------

func (s *Server) Metrics(stream agentv1.AgentGateway_MetricsServer) error {
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.sink.MetricBatch(b); err != nil {
			slog.Warn("metric sink failed", "seq", b.Sequence, "err", err)
			return err // agent 侧 waiter 失败 → 保 WAL 重试
		}
		if err := stream.Send(&agentv1.MetricAck{LastAckedSequence: b.Sequence}); err != nil {
			return err
		}
	}
}

// ---------- 可靠流 ----------

func (s *Server) Reports(stream agentv1.AgentGateway_ReportsServer) error {
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		ack := &agentv1.ReportAck{Kind: r.Kind}
		for _, it := range r.Items {
			kind := reportKindName(r.Kind)
			if err := s.sink.Report(kind, it.Id, it.Payload); err != nil {
				slog.Warn("report sink failed", "kind", kind, "id", it.Id, "err", err)
				return err // 不 ACK → agent 保 WAL 重发（至少一次）
			}
			if r.Kind == agentv1.ReportKind_REPORT_KIND_RESULTS {
				s.mailbox.Complete(it.Id, it.Payload)
				terminal := resultStatus(it.Payload)
				s.persist("result_received", func(ctx context.Context) error {
					return s.store.CompleteCommand(ctx, it.Id, terminal, it.Payload)
				})
			}
			ack.AckedIds = append(ack.AckedIds, it.Id)
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func reportKindName(k agentv1.ReportKind) string {
	switch k {
	case agentv1.ReportKind_REPORT_KIND_RESULTS:
		return "results"
	case agentv1.ReportKind_REPORT_KIND_AUDIT:
		return "audit"
	case agentv1.ReportKind_REPORT_KIND_INVENTORY:
		return "inventory"
	default:
		return "unknown"
	}
}

// resultStatus 从执行器结果 JSON 提取终态（executor.Result.Status）；缺失按 SUCCEEDED 兜底。
func resultStatus(payload []byte) string {
	var r struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(payload, &r); err == nil && r.Status != "" {
		return r.Status
	}
	return "SUCCEEDED"
}

func newSessionID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return fmt.Sprintf("s-%d-%x", time.Now().Unix(), b)
}
