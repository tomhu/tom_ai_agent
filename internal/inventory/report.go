//go:build linux

// report.go — 资产上报模块：首启全量 + 周期全量校验 + 平台触发刷新（预留）。
// 走审计级可靠队列（设计文档 §8.4：先落 WAL 再发送，不允许静默丢弃）。
package inventory

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
)

// Module 资产上报模块。
type Module struct {
	cfg       *config.InventoryConf
	collector *Collector
	rep       *reporter.Reporter
	revision  int64
}

func NewModule(cfg *config.InventoryConf, rep *reporter.Reporter) *Module {
	return &Module{cfg: cfg, collector: NewCollector(cfg), rep: rep}
}

func (m *Module) Name() string { return "inventory" }

func (m *Module) Start(ctx context.Context) error {
	if !m.cfg.Enabled {
		slog.Info("inventory disabled")
		return nil
	}
	go m.loop(ctx)
	return nil
}

func (m *Module) loop(ctx context.Context) {
	m.reportFull(ctx) // 首启全量
	interval := m.cfg.FullInterval
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reportFull(ctx)
		}
	}
}

// reportFull 采集全量并提交可靠队列（WAL 先持久化）。
func (m *Module) reportFull(ctx context.Context) {
	r := m.collector.CollectFull(ctx)
	m.revision++
	id := fmt.Sprintf("inv-full-%d-%d", time.Now().UnixMilli(), m.revision)
	payload := struct {
		Revision int64   `json:"revision"`
		Report   *Report `json:"report"`
	}{Revision: m.revision, Report: r}

	if err := m.rep.SubmitReliable(reporter.QueueInventory, id, payload); err != nil {
		slog.Error("submit inventory failed", "err", err)
		return
	}
	slog.Info("inventory reported",
		"revision", m.revision,
		"packages", len(r.Packages),
		"processes_collected", len(m.collector.LastProcesses()),
		"processes_uploaded", len(r.Processes))
}
