//go:build linux

// grpc.go — gRPC 上行（proto v1 冻结版，platform-architecture.md §6.1）。
//
// 三条流：
//   - Control：Hello 握手 → 收 CommandEnvelope/CancelCommand → 分发执行引擎；周期心跳
//   - Metrics：批量发送，凭 MetricAck 推进（替代 HTTP fire-and-forget）
//   - Reports：可靠批量发送，凭 ReportAck(acked_ids 全覆盖) 确认后推进 WAL 游标
//
// 断线语义：任一流出错 → 全部 waiter 立即失败（reporter 保 WAL 重试）→ 退避重连。
package uplink

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
)

// CommandHandler 指令分发目标（executor 实现）。
type CommandHandler interface {
	SubmitEnvelope(env *agentv1.CommandEnvelope)
	CancelCommand(cmdID string)
}

// GRPCUplink 实现 reporter.Sink + 指令控制流。
type GRPCUplink struct {
	addr        string
	agentVer    string
	catalogVer  string
	handler     CommandHandler
	assetIDFunc func() string

	connMu sync.Mutex
	conn   *grpc.ClientConn

	metricOut chan *metricSend
	reportOut chan *reportSend

	metricAcks sync.Map // sequence -> chan error
	reportMu   sync.Mutex
	reportWait []*reportWaiter

	ready chan struct{} // 握手完成关闭；重连时重建
	readyMu sync.Mutex
}

type metricSend struct {
	batch *agentv1.MetricBatch
	ack   chan error
}

type reportSend struct {
	batch *agentv1.ReliableReport
	ack   chan error
}

type reportWaiter struct {
	kind agentv1.ReportKind
	ids  map[string]struct{}
	ch   chan error
}

func NewGRPC(addr, agentVer, catalogVer string, h CommandHandler, assetID func() string) *GRPCUplink {
	return &GRPCUplink{
		addr:        addr,
		agentVer:    agentVer,
		catalogVer:  catalogVer,
		handler:     h,
		assetIDFunc: assetID,
		metricOut:   make(chan *metricSend, 64),
		reportOut:   make(chan *reportSend, 64),
		ready:       make(chan struct{}),
	}
}

// Run 连接监督循环（core Module 语义：阻塞至 ctx 取消）。
func (u *GRPCUplink) Name() string { return "grpc-uplink" }

func (u *GRPCUplink) Start(ctx context.Context) error {
	go u.supervise(ctx)
	return nil
}

func (u *GRPCUplink) supervise(ctx context.Context) {
	backoff := time.Second
	for {
		err := u.session(ctx)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("grpc session ended, reconnect", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// session 一次连接生命周期：拨号 → 控制流握手 → 指标/可靠流 → 任一出错全部回收。
func (u *GRPCUplink) session(ctx context.Context) error {
	u.readyMu.Lock()
	u.ready = make(chan struct{})
	u.readyMu.Unlock()

	conn, err := grpc.NewClient(u.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := agentv1.NewAgentGatewayClient(conn)

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 3)

	// 1. 控制流（含握手与心跳）
	control, err := client.Control(sessCtx)
	if err != nil {
		return fmt.Errorf("open control stream: %w", err)
	}
	hello := &agentv1.AgentControlFrame{Frame: &agentv1.AgentControlFrame_Hello{Hello: &agentv1.AgentHello{
		AssetId:            u.assetIDFunc(),
		BootId:             bootID(),
		AgentVersion:       u.agentVer,
		ProtocolMinVersion: 1,
		ProtocolMaxVersion: 1,
		ActionCatalogVersion: u.catalogVer,
	}}}
	if err := control.Send(hello); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	go u.controlRecv(sessCtx, control, errCh)

	// 2. 指标流
	metrics, err := client.Metrics(sessCtx)
	if err != nil {
		return fmt.Errorf("open metrics stream: %w", err)
	}
	go u.metricsSendLoop(sessCtx, metrics, errCh)
	go u.metricsRecvLoop(sessCtx, metrics, errCh)

	// 3. 可靠流
	reports, err := client.Reports(sessCtx)
	if err != nil {
		return fmt.Errorf("open reports stream: %w", err)
	}
	go u.reportsSendLoop(sessCtx, reports, errCh)
	go u.reportsRecvLoop(sessCtx, reports, errCh)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		u.failAllWaiters(err)
		return err
	}
}

// controlRecv 控制流接收：Welcome 握手完成 → 打开闸门；Command/Cancel 分发。
func (u *GRPCUplink) controlRecv(ctx context.Context, s agentv1.AgentGateway_ControlClient, errCh chan<- error) {
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	recvCh := make(chan *agentv1.GatewayControlFrame, 8)
	go func() {
		for {
			f, err := s.Recv()
			if err != nil {
				recvCh <- nil
				return
			}
			recvCh <- f
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			_ = s.Send(&agentv1.AgentControlFrame{Frame: &agentv1.AgentControlFrame_Heartbeat{
				Heartbeat: &agentv1.Heartbeat{SentAt: time.Now().UnixMilli()}}})
		case f := <-recvCh:
			if f == nil {
				errCh <- errors.New("control stream closed")
				return
			}
			switch fr := f.Frame.(type) {
			case *agentv1.GatewayControlFrame_Welcome:
				slog.Info("gateway welcome", "session_id", fr.Welcome.SessionId, "epoch", fr.Welcome.SessionEpoch)
				u.readyMu.Lock()
				close(u.ready)
				u.readyMu.Unlock()
			case *agentv1.GatewayControlFrame_Command:
				if u.handler != nil {
					u.handler.SubmitEnvelope(fr.Command)
				} else {
					slog.Warn("command received but executor disabled", "cmd_id", fr.Command.CmdId)
				}
			case *agentv1.GatewayControlFrame_Cancel:
				if u.handler != nil {
					u.handler.CancelCommand(fr.Cancel.CmdId)
				}
			case *agentv1.GatewayControlFrame_ReconnectHint:
				slog.Info("reconnect hint", "after_sec", fr.ReconnectHint.ReconnectAfterSec)
				errCh <- errors.New("reconnect hint")
				return
			}
		}
	}
}

// ---------- 指标流 ----------

func (u *GRPCUplink) metricsSendLoop(ctx context.Context, s agentv1.AgentGateway_MetricsClient, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-u.metricOut:
			u.metricAcks.Store(m.batch.Sequence, m.ack)
			if err := s.Send(m.batch); err != nil {
				u.metricAcks.Delete(m.batch.Sequence)
				m.ack <- err
				errCh <- err
				return
			}
		}
	}
}

func (u *GRPCUplink) metricsRecvLoop(ctx context.Context, s agentv1.AgentGateway_MetricsClient, errCh chan<- error) {
	for {
		ack, err := s.Recv()
		if err != nil {
			if err != io.EOF && ctx.Err() == nil {
				errCh <- err
			}
			return
		}
		// 累计 ACK：推进所有 <= last_acked 的等待者
		u.metricAcks.Range(func(k, v any) bool {
			if k.(uint64) <= ack.LastAckedSequence {
				v.(chan error) <- nil
				u.metricAcks.Delete(k)
			}
			return true
		})
	}
}

// ---------- 可靠流 ----------

func (u *GRPCUplink) reportsSendLoop(ctx context.Context, s agentv1.AgentGateway_ReportsClient, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-u.reportOut:
			if err := s.Send(r.batch); err != nil {
				r.ack <- err
				errCh <- err
				return
			}
		}
	}
}

func (u *GRPCUplink) reportsRecvLoop(ctx context.Context, s agentv1.AgentGateway_ReportsClient, errCh chan<- error) {
	for {
		ack, err := s.Recv()
		if err != nil {
			if err != io.EOF && ctx.Err() == nil {
				errCh <- err
			}
			return
		}
		u.reportMu.Lock()
		remaining := u.reportWait[:0]
		for _, w := range u.reportWait {
			if w.kind != ack.Kind {
				remaining = append(remaining, w)
				continue
			}
			for _, id := range ack.AckedIds {
				delete(w.ids, id)
			}
			if len(w.ids) == 0 {
				w.ch <- nil
			} else {
				remaining = append(remaining, w)
			}
		}
		u.reportWait = remaining
		u.reportMu.Unlock()
	}
}

// failAllWaiters 断线时立即失败所有等待者（reporter 保留 WAL，下一会话重发）。
func (u *GRPCUplink) failAllWaiters(err error) {
	u.metricAcks.Range(func(k, v any) bool {
		select {
		case v.(chan error) <- err:
		default:
		}
		u.metricAcks.Delete(k)
		return true
	})
	u.reportMu.Lock()
	for _, w := range u.reportWait {
		w.ch <- err
	}
	u.reportWait = nil
	u.reportMu.Unlock()
}

// ---------- reporter.Sink 实现 ----------

func (u *GRPCUplink) SendMetrics(ctx context.Context, b *reporter.Batch) error {
	pb := &agentv1.MetricBatch{AssetId: b.AssetID, SentAt: b.SentAt, Sequence: b.Sequence}
	for _, m := range b.Samples {
		pb.Samples = append(pb.Samples, &agentv1.Metric{
			Metric: m.Name, Timestamp: m.Timestamp, Value: m.Value, Labels: m.Labels,
		})
	}
	ack := make(chan error, 1)
	select {
	case u.metricOut <- &metricSend{batch: pb, ack: ack}:
	case <-time.After(2 * time.Second):
		return errors.New("metrics send queue full (uplink down?)")
	}
	return u.waitReady(ctx, ack)
}

func (u *GRPCUplink) SendReliable(ctx context.Context, b *reporter.ReliableBatch) error {
	kind := reportKindOf(b.Kind)
	pb := &agentv1.ReliableReport{AssetId: b.AssetID, Kind: kind, SentAt: b.SentAt}
	w := &reportWaiter{kind: kind, ids: map[string]struct{}{}, ch: make(chan error, 1)}
	for _, it := range b.Items {
		pb.Items = append(pb.Items, &agentv1.ReliableItem{
			Id: it.ID, Kind: kind, CreatedAt: it.CreatedAt, Payload: it.Payload,
		})
		w.ids[it.ID] = struct{}{}
	}
	u.reportMu.Lock()
	u.reportWait = append(u.reportWait, w)
	u.reportMu.Unlock()

	select {
	case u.reportOut <- &reportSend{batch: pb, ack: w.ch}:
	case <-time.After(2 * time.Second):
		u.removeWaiter(w)
		return errors.New("reports send queue full (uplink down?)")
	}
	return u.waitReady(ctx, w.ch)
}

// waitReady 等待握手完成 + ACK；内部超时兜底防永久阻塞。
func (u *GRPCUplink) waitReady(ctx context.Context, ack <-chan error) error {
	u.readyMu.Lock()
	ready := u.ready
	u.readyMu.Unlock()

	select {
	case <-ready:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("gateway handshake timeout")
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return errors.New("ack timeout")
	}
}

func (u *GRPCUplink) removeWaiter(w *reportWaiter) {
	u.reportMu.Lock()
	defer u.reportMu.Unlock()
	for i, x := range u.reportWait {
		if x == w {
			u.reportWait = append(u.reportWait[:i], u.reportWait[i+1:]...)
			return
		}
	}
}

func reportKindOf(k reporter.QueueKind) agentv1.ReportKind {
	switch k {
	case reporter.QueueResults:
		return agentv1.ReportKind_REPORT_KIND_RESULTS
	case reporter.QueueAudit:
		return agentv1.ReportKind_REPORT_KIND_AUDIT
	case reporter.QueueInventory:
		return agentv1.ReportKind_REPORT_KIND_INVENTORY
	default:
		return agentv1.ReportKind_REPORT_KIND_UNSPECIFIED
	}
}

// bootID 每次启动生成（会话语义，标识本次进程生命）。
func bootID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
