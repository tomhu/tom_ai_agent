// session.go — Connector 会话注册表（platform-architecture.md §6.1 Hello/Welcome + §7.3 重复连接策略）。
//
// 语义：
//   - 每 asset 同时只允许一个活跃会话；新连接 replace_old：旧会话收到 FenceNotice 后关闭
//   - session_epoch 单调递增（fencing token，防旧会话幽灵写入）
//   - 心跳保活；超过 offline_timeout 未心跳标记离线（事件上送，连接由读超时自然回收）
package connector

import (
	"sync"
	"time"
)

type Session struct {
	AssetID     string
	SessionID   string
	Epoch       uint64
	AgentVer    string
	CatalogVer  string
	ConnectedAt time.Time
	LastSeen    time.Time
	fence       chan struct{} // 关闭即通知旧会话退出
}

type SessionRegistry struct {
	mu       sync.Mutex
	byAsset  map[string]*Session
	epochSeq uint64

	OnReplace func(old, new *Session) // 安全事件钩子（重复连接替换）
}

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{byAsset: map[string]*Session{}}
}

// Register 建立新会话；若同 asset 已有会话，按 replace_old 栅栏化旧会话。
func (r *SessionRegistry) Register(assetID, sessionID, agentVer, catalogVer string) (*Session, *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epochSeq++
	s := &Session{
		AssetID: assetID, SessionID: sessionID, Epoch: r.epochSeq,
		AgentVer: agentVer, CatalogVer: catalogVer,
		ConnectedAt: time.Now(), LastSeen: time.Now(),
		fence: make(chan struct{}),
	}
	old := r.byAsset[assetID]
	if old != nil {
		close(old.fence) // 旧会话感知后必须退出
	}
	r.byAsset[assetID] = s
	return s, old
}

// Fenced 旧会话感知通道。
func (s *Session) Fenced() <-chan struct{} { return s.fence }

// Touch 心跳刷新（带 epoch 校验：旧会话心跳不得复活）。
func (r *SessionRegistry) Touch(assetID string, epoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byAsset[assetID]
	if !ok || s.Epoch != epoch {
		return false
	}
	s.LastSeen = time.Now()
	return true
}

// Unregister 注销（仅当仍是当前会话）。
func (r *SessionRegistry) Unregister(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.byAsset[s.AssetID]; ok && cur == s {
		delete(r.byAsset, s.AssetID)
	}
}

// Online 当前在线快照。
func (r *SessionRegistry) Online() []Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Session, 0, len(r.byAsset))
	for _, s := range r.byAsset {
		out = append(out, *s)
	}
	return out
}

// OfflineSweep 标记超时未心跳的会话（调用方生成离线事件）。
func (r *SessionRegistry) OfflineSweep(timeout time.Duration) []Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	var stale []Session
	for _, s := range r.byAsset {
		if time.Since(s.LastSeen) > timeout {
			stale = append(stale, *s)
		}
	}
	return stale
}
