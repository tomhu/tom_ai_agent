//go:build linux

// envelope.go — gRPC 指令信封 → 执行引擎适配（uplink.CommandHandler 实现）。
// M5a 开发态不验签（signature 为空接受）；M5c 启用 Ed25519 验签与 nonce 防重放。
package executor

import (
	"log/slog"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/authenv"
	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

// SubmitEnvelope 信封入口：验签（M5c fail-closed）→ 过期 → nonce 防重放 → 动作目录与队列。
func (e *Engine) SubmitEnvelope(env *agentv1.CommandEnvelope) {
	reject := func(reason string) {
		slog.Warn("envelope rejected", "cmd_id", env.CmdId, "reason", reason)
		e.reportImmediate(Result{
			CmdID: env.CmdId, Status: "REJECTED_POLICY", ExitCode: -1,
			Stderr: reason, FinishedAt: time.Now().UnixMilli(),
		})
	}
	now := time.Now().UnixMilli()
	if e.verifyKey != nil {
		if err := authenv.Verify(e.verifyKey, env); err != nil {
			reject("signature invalid: " + err.Error())
			return
		}
		if len(env.Nonce) == 0 {
			reject("nonce required")
			return
		}
		if err := e.nonces.Check(env.Nonce, env.ExpiresAt, now); err != nil {
			reject(err.Error())
			return
		}
	}
	if env.ExpiresAt > 0 && now > env.ExpiresAt {
		reject("envelope expired")
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
