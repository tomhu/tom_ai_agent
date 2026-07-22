// Package watchdog 提供 agent 自监控（设计文档 §7，M1 版）。
// M1 范围：自身指标上送（uptime/rss/goroutine/缓冲/采集健康）。
// M3 增加：模块级重启、资源哨兵降级、sd_notify systemd 看门狗。
package watchdog

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/collector"
	"github.com/tomhu/tom_ai_agent/internal/config"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
)

// SelfMonitor 周期采集 agent 自身状态并走指标管道上送。
type SelfMonitor struct {
	rep     *reporter.Reporter
	version string
	startAt time.Time
}

func NewSelfMonitor(cfg *config.Config, rep *reporter.Reporter, version string) *SelfMonitor {
	return &SelfMonitor{rep: rep, version: version, startAt: time.Now()}
}

func (s *SelfMonitor) Name() string { return "self-monitor" }

func (s *SelfMonitor) Start(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		s.report()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.report()
			}
		}
	}()
	return nil
}

func (s *SelfMonitor) report() {
	now := time.Now().UnixMilli()
	labels := map[string]string{"agent_version": s.version}
	depth, dropped := s.rep.Stats()

	metrics := []collector.Metric{
		{Name: "agent.uptime.seconds", Timestamp: now, Value: time.Since(s.startAt).Seconds(), Labels: labels},
		{Name: "agent.self.goroutines", Timestamp: now, Value: float64(runtime.NumGoroutine()), Labels: labels},
		{Name: "agent.buffer.depth", Timestamp: now, Value: float64(depth), Labels: labels},
		{Name: "agent.buffer.dropped.total", Timestamp: now, Value: float64(dropped), Labels: labels},
	}
	if rss, err := selfRSS(); err == nil {
		metrics = append(metrics, collector.Metric{Name: "agent.self.rss.bytes", Timestamp: now, Value: rss, Labels: labels})
	}
	s.rep.Submit(metrics)
}

// selfRSS 从 /proc/self/status 读 VmRSS（字节）。
func selfRSS() (float64, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			break
		}
		kb, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	return 0, nil
}
