// mailbox.go — 指令邮箱与生命周期（P0 内存版；生产版落 cmd_command 状态机 + Outbox，见 DDL §5）。
//
// 状态机：QUEUED → DISPATCHED（推上控制流）→ terminal(SUCCEEDED/FAILED/TIMEOUT_KILLED/CANCELLED/REJECTED_*)
// 离线 agent 的指令留在邮箱（TTL 过期 → EXPIRED）；重连后按 FIFO 补投。
package connector

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type CommandState string

const (
	StateQueued     CommandState = "QUEUED"
	StateDispatched CommandState = "DISPATCHED"
	StateTerminal   CommandState = "TERMINAL" // 结果已落（结果体另存）
	StateExpired    CommandState = "EXPIRED"
)

type MailboxCommand struct {
	CmdID      string
	Action     string
	Params     map[string]string
	TimeoutSec int
	State      CommandState
	QueuedAt   time.Time
	ExpiresAt  time.Time
	Result     []byte // 终态结果 JSON（Reports 流落库前暂存）
}

var (
	ErrMailboxFull = errors.New("mailbox full")
	ErrDupCmdID    = errors.New("duplicate cmd_id")
)

type Mailbox struct {
	mu             sync.Mutex
	perAsset       int
	ttl            time.Duration
	byAsset        map[string][]*MailboxCommand // FIFO
	byID           map[string]*MailboxCommand
	notify         map[string]chan struct{} // asset -> 有新指令信号
	pendingCancels map[string][]string      // asset -> 已派发指令的取消待推
}

func NewMailbox(perAsset int, ttl time.Duration) *Mailbox {
	return &Mailbox{
		perAsset: perAsset, ttl: ttl,
		byAsset:        map[string][]*MailboxCommand{},
		byID:           map[string]*MailboxCommand{},
		notify:         map[string]chan struct{}{},
		pendingCancels: map[string][]string{},
	}
}

// Submit 入邮箱（幂等：同 cmd_id 拒绝）。
func (m *Mailbox) Submit(assetID, cmdID, action string, params map[string]string, timeoutSec int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.byID[cmdID]; dup {
		return ErrDupCmdID
	}
	q := m.byAsset[assetID]
	pending := 0
	for _, c := range q {
		if c.State == StateQueued || c.State == StateDispatched {
			pending++
		}
	}
	if pending >= m.perAsset {
		return ErrMailboxFull
	}
	c := &MailboxCommand{
		CmdID: cmdID, Action: action, Params: params, TimeoutSec: timeoutSec,
		State: StateQueued, QueuedAt: time.Now(), ExpiresAt: time.Now().Add(m.ttl),
	}
	m.byAsset[assetID] = append(q, c)
	m.byID[cmdID] = c
	m.signalLocked(assetID)
	return nil
}

// Cancel 取消：未派发直接终态；已派发需经控制流下发 CancelCommand。
func (m *Mailbox) Cancel(assetID, cmdID string) (needPush bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.byID[cmdID]
	if !ok || c.State == StateTerminal || c.State == StateExpired {
		return false, fmt.Errorf("cmd %s not cancellable", cmdID)
	}
	if c.State == StateQueued {
		c.State = StateExpired // 未派发即取消：不投递
		return false, nil
	}
	m.pendingCancels[assetID] = append(m.pendingCancels[assetID], cmdID)
	m.signalLocked(assetID) // 已派发：推送取消帧
	return true, nil
}

// NextDispatch 取该 asset 待派发指令与待取消清单（控制流推送循环调用）。
func (m *Mailbox) NextDispatch(assetID string) (cmds []*MailboxCommand, cancels []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	q := m.byAsset[assetID]
	kept := q[:0]
	for _, c := range q {
		switch c.State {
		case StateQueued:
			if now.After(c.ExpiresAt) {
				c.State = StateExpired
				continue
			}
			c.State = StateDispatched
			cmds = append(cmds, c)
		case StateDispatched:
			if now.After(c.ExpiresAt) {
				c.State = StateExpired
				continue
			}
		}
		kept = append(kept, c)
	}
	m.byAsset[assetID] = kept
	// 已派发且收到取消的（Cancel 标记为待推）：简化——cancels 由 Cancel 时记录
	for _, id := range m.pendingCancels[assetID] {
		cancels = append(cancels, id)
	}
	m.pendingCancels[assetID] = nil
	return cmds, cancels
}

// Wait 等待资产信箱变化信号（超时返回 false）。
func (m *Mailbox) Wait(assetID string, d time.Duration) bool {
	m.mu.Lock()
	ch, ok := m.notify[assetID]
	if !ok {
		ch = make(chan struct{}, 1)
		m.notify[assetID] = ch
	}
	m.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// Complete 结果落地（Reports 流回调）。
func (m *Mailbox) Complete(cmdID string, result []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.byID[cmdID]; ok {
		c.State = StateTerminal
		c.Result = result
	}
}

// Result 查询指令状态与结果。
func (m *Mailbox) Result(cmdID string) (*MailboxCommand, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.byID[cmdID]
	return c, ok
}

func (m *Mailbox) signalLocked(assetID string) {
	if ch, ok := m.notify[assetID]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
