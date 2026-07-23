// grpc.go — 模拟网关的 gRPC 服务端（proto v1）：Control/Metrics/Reports 三流。
// 与 HTTP 端点共享 cmdQueues/cancels/results 状态，admin 端点不变。
package main

import (
	"io"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

type gatewayServer struct {
	agentv1.UnimplementedAgentGatewayServer
}

func RegisterGRPC(s *grpc.Server) {
	agentv1.RegisterAgentGatewayServer(s, &gatewayServer{})
}

// ---------- 控制流 ----------

func (g *gatewayServer) Control(s agentv1.AgentGateway_ControlServer) error {
	// 首帧必须 Hello
	first, err := s.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return grpc.ErrServerStopped
	}
	assetID := hello.AssetId
	log.Printf("[grpc] hello asset=%s ver=%s catalog=%s", assetID, hello.AgentVersion, hello.ActionCatalogVersion)

	if err := s.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Welcome{
		Welcome: &agentv1.GatewayWelcome{
			SessionId:           newSessionID(),
			SessionEpoch:        1,
			ServerTime:          time.Now().UnixMilli(),
			HeartbeatIntervalSec: 15,
		}}}); err != nil {
		return err
	}

	// 下发循环：轮询共享队列，有指令/取消即推
	errCh := make(chan error, 1)
	go func() {
		for {
			f, err := s.Recv()
			if err != nil {
				errCh <- err
				return
			}
			switch fr := f.Frame.(type) {
			case *agentv1.AgentControlFrame_Heartbeat:
				// 心跳从简：仅维持连接
			case *agentv1.AgentControlFrame_CommandAck:
				log.Printf("[grpc] command ack cmd=%s accepted=%v %s", fr.CommandAck.CmdId, fr.CommandAck.Accepted, fr.CommandAck.RejectReason)
			}
		}
	}()

	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.Context().Done():
			log.Printf("[grpc] control closed asset=%s", assetID)
			return nil
		case err := <-errCh:
			log.Printf("[grpc] control recv ended asset=%s err=%v", assetID, err)
			return nil
		case <-ticker.C:
			cmdQueuesMu.Lock()
			cmds := cmdQueues[assetID]
			cancelsForAsset := cancels[assetID]
			delete(cmdQueues, assetID)
			delete(cancels, assetID)
			cmdQueuesMu.Unlock()
			for _, c := range cmds {
				env := &agentv1.CommandEnvelope{
					CmdId: c.CmdID, Action: c.Action, Params: c.Params,
					TimeoutSec: uint32(c.TimeoutSec),
					IssuedAt:   time.Now().UnixMilli(),
					ExpiresAt:  time.Now().Add(5 * time.Minute).UnixMilli(),
				}
				if err := s.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Command{Command: env}}); err != nil {
					return err
				}
				log.Printf("[grpc] command pushed asset=%s cmd=%s action=%s", assetID, c.CmdID, c.Action)
			}
			for _, id := range cancelsForAsset {
				if err := s.Send(&agentv1.GatewayControlFrame{Frame: &agentv1.GatewayControlFrame_Cancel{
					Cancel: &agentv1.CancelCommand{CmdId: id}}}); err != nil {
					return err
				}
				log.Printf("[grpc] cancel pushed asset=%s cmd=%s", assetID, id)
			}
		}
	}
}

// ---------- 指标流 ----------

func (g *gatewayServer) Metrics(s agentv1.AgentGateway_MetricsServer) error {
	for {
		b, err := s.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		counters["metrics"].Add(1)
		log.Printf("[metrics] grpc batch seq=%d samples=%d", b.Sequence, len(b.Samples))
		if err := s.Send(&agentv1.MetricAck{LastAckedSequence: b.Sequence}); err != nil {
			return err
		}
	}
}

// ---------- 可靠流 ----------

func (g *gatewayServer) Reports(s agentv1.AgentGateway_ReportsServer) error {
	for {
		r, err := s.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		kind := kindName(r.Kind)
		counters[kind].Add(1)
		ack := &agentv1.ReportAck{Kind: r.Kind}
		if r.Kind == agentv1.ReportKind_REPORT_KIND_RESULTS {
			resultsMu.Lock()
			for _, it := range r.Items {
				results[it.Id] = it.Payload
				log.Printf("[results] cmd=%s stored", it.Id)
			}
			resultsMu.Unlock()
		}
		for _, it := range r.Items {
			ack.AckedIds = append(ack.AckedIds, it.Id)
		}
		log.Printf("[%s] grpc batch items=%d", kind, len(r.Items))
		if err := s.Send(ack); err != nil {
			return err
		}
	}
}

func kindName(k agentv1.ReportKind) string {
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

var (
	sessionSeq   uint64
	sessionSeqMu sync.Mutex
)

func newSessionID() string {
	sessionSeqMu.Lock()
	defer sessionSeqMu.Unlock()
	sessionSeq++
	return "s-" + time.Now().Format("150405") + "-" + itoa(sessionSeq)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
