//go:build linux

// sentinel.go — 资源哨兵（设计文档 §7.1 第 2 层）。
// RSS/FD 接近阈值时自动降级（暂停非关键采集器 + 释放内存）；
// 超过硬限时自我退出，交 systemd 拉起——宁可 agent 重启，不可拖垮生产服务器。
package watchdog

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
)

// Degradable 可被降级的组件（采集调度器实现）。
type Degradable interface {
	SetDegraded(on bool)
}

// EventSink 审计事件出口（reporter 可靠队列）。
type EventSink interface {
	SubmitReliable(kind reporter.QueueKind, id string, payload any) error
}

// Sentinel 周期检查自身资源水位。
type Sentinel struct {
	cfg      *config.WatchdogConf
	target   Degradable
	events   EventSink
	degraded bool
}

func NewSentinel(cfg *config.WatchdogConf, target Degradable, events EventSink) *Sentinel {
	return &Sentinel{cfg: cfg, target: target, events: events}
}

func (s *Sentinel) emit(event string, detail map[string]any) {
	if s.events == nil {
		return
	}
	if detail == nil {
		detail = map[string]any{}
	}
	detail["event"] = event
	if err := s.events.SubmitReliable(reporter.QueueAudit, fmt.Sprintf("%s-%d", event, time.Now().UnixNano()), detail); err != nil {
		slog.Error("emit audit event failed", "event", event, "err", err)
	}
}

func (s *Sentinel) Name() string { return "resource-sentinel" }

func (s *Sentinel) Start(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.check()
			}
		}
	}()
	return nil
}

func (s *Sentinel) check() {
	rss, err := selfRSS()
	if err != nil {
		return
	}
	fds := countFDs()
	softBytes := float64(s.cfg.RSSSoftMB) * (1 << 20)
	hardBytes := float64(s.cfg.RSSHardMB) * (1 << 20)

	switch {
	case rss >= hardBytes || (s.cfg.FDLimit > 0 && fds >= s.cfg.FDLimit*2):
		// 硬限：自我退出（安全姿势，交 systemd 重启）
		slog.Error("resource hard limit exceeded, self-terminating for systemd restart",
			"rss_mb", int(rss)>>20, "fds", fds)
		s.emit("agent.self_terminate", map[string]any{"rss_bytes": rss, "fds": fds})
		os.Exit(1)

	case rss >= softBytes || (s.cfg.FDLimit > 0 && fds >= s.cfg.FDLimit):
		if !s.degraded && s.cfg.DegradedMode {
			s.degraded = true
			s.target.SetDegraded(true)
			debug.FreeOSMemory()
			slog.Warn("resource soft limit exceeded, entering degraded mode",
				"rss_mb", int(rss)>>20, "fds", fds)
			s.emit("agent.degraded_on", map[string]any{"rss_bytes": rss, "fds": fds})
		}

	default:
		// 回落到软限 80% 以下才恢复，防抖
		if s.degraded && rss < softBytes*0.8 {
			s.degraded = false
			s.target.SetDegraded(false)
			slog.Info("resource back to normal, leaving degraded mode", "rss_mb", int(rss)>>20)
			s.emit("agent.degraded_off", map[string]any{"rss_bytes": rss})
		}
	}
}

// countFDs 统计当前进程打开的 fd 数。
func countFDs() int {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(ents)
}
