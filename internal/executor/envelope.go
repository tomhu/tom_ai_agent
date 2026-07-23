//go:build linux

// envelope.go — gRPC 指令信封 → 执行引擎适配（uplink.CommandHandler 实现）。
// M5a 开发态不验签（signature 为空接受）；M5c 启用 Ed25519 验签与 nonce 防重放。
package executor

import (
	"time"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

// SubmitEnvelope 信封入口：先做过期检查（fail-closed），再交动作目录校验与队列。
func (e *Engine) SubmitEnvelope(env *agentv1.CommandEnvelope) {
	if env.ExpiresAt > 0 && time.Now().UnixMilli() > env.ExpiresAt {
		e.reportImmediate(Result{
			CmdID: env.CmdId, Status: "REJECTED_POLICY", ExitCode: -1,
			Stderr: "envelope expired", FinishedAt: time.Now().UnixMilli(),
		})
		return
	}
	res := e.Submit(Command{
		CmdID:      env.CmdId,
		Action:     env.Action,
		Params:     env.Params,
		TimeoutSec: int(env.TimeoutSec),
	})
	if res.Status != "QUEUED" {
		e.reportImmediate(res) // 策略拒绝/队列满也回传终态
	}
}

// CancelCommand 取消执行中指令。
func (e *Engine) CancelCommand(cmdID string) { e.Cancel(cmdID) }
