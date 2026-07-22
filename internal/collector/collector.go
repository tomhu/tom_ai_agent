// Package collector 实现指标采集框架与基础采集器（设计文档 §3）。
// 原则：插件化、独立周期、失败隔离（单次采集 recover + 超时）、基数控制。
package collector

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
)

// Metric 是一条时序样本。M1 用通用结构；proto 冻结后切换为 metric_id+字典引用。
type Metric struct {
	Name      string            `json:"metric"`
	Timestamp int64             `json:"timestamp"` // Unix 毫秒(UTC)
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Sink 是采集结果的下游（由 reporter 实现）。
type Sink interface {
	Submit(metrics []Metric)
}

// Collector 采集器接口（设计文档 §3.2）。
type Collector interface {
	Name() string
	Collect(ctx context.Context) ([]Metric, error)
}

// Scheduler 按各自周期调度采集器，失败隔离；支持降级模式（资源哨兵触发）。
type Scheduler struct {
	cfg        *config.Config
	sink       Sink
	collectors []Collector
	intervals  map[string]time.Duration
	wg         sync.WaitGroup

	// 降级模式：跳过非关键采集器（diskcap/net/load 等高频低优先），保留 cpu/memory
	degraded atomic.Bool

	// 自监控计数（watchdog 读取）
	collectErrors  map[string]*int64
	collectLatency map[string]*int64 // 毫秒
	mu             sync.RWMutex
}

// nonCritical 降级时被跳过的采集器。
var nonCritical = map[string]bool{"diskcap": true, "net": true, "load": true}

// SetDegraded 实现 watchdog.Degradable。
func (s *Scheduler) SetDegraded(on bool) { s.degraded.Store(on) }

// Degraded 当前是否处于降级模式。
func (s *Scheduler) Degraded() bool { return s.degraded.Load() }

func NewScheduler(cfg *config.Config, sink Sink) *Scheduler {
	s := &Scheduler{
		cfg:            cfg,
		sink:           sink,
		intervals:      map[string]time.Duration{},
		collectErrors:  map[string]*int64{},
		collectLatency: map[string]*int64{},
	}

	reg := map[string]Collector{
		"cpu":     NewCPU(),
		"memory":  NewMemory(),
		"diskcap": NewDiskCap(),
		"net":     NewNet(),
		"load":    NewLoad(),
	}
	for name, c := range reg {
		cc, ok := cfg.Collectors[name]
		if !ok || !cc.Enabled {
			continue
		}
		s.collectors = append(s.collectors, c)
		s.intervals[name] = cc.Interval
		var errCnt, lat int64
		s.collectErrors[name] = &errCnt
		s.collectLatency[name] = &lat
	}
	return s
}

func (s *Scheduler) Name() string { return "collector-scheduler" }

// Stats 供自监控上报。
func (s *Scheduler) Stats() (errs map[string]int64, latMs map[string]int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	errs = map[string]int64{}
	latMs = map[string]int64{}
	for k, v := range s.collectErrors {
		errs[k] = *v
	}
	for k, v := range s.collectLatency {
		latMs[k] = *v
	}
	return
}

func (s *Scheduler) Start(ctx context.Context) error {
	for _, c := range s.collectors {
		c := c
		interval := s.intervals[c.Name()]
		if interval <= 0 {
			interval = 10 * time.Second
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.loop(ctx, c, interval)
		}()
	}
	return nil
}

func (s *Scheduler) loop(ctx context.Context, c Collector, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.runOnce(ctx, c) // 启动即采一次
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runOnce(ctx, c)
		}
	}
}

// runOnce 单次采集：独立超时 + panic 隔离（设计文档 §3.2 失败隔离）。
// 降级模式下跳过非关键采集器。
func (s *Scheduler) runOnce(ctx context.Context, c Collector) {
	name := c.Name()
	if s.degraded.Load() && nonCritical[name] {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			*s.collectErrors[name]++
			slog.Error("collector panic", "collector", name, "panic", r)
		}
		*s.collectLatency[name] = time.Since(start).Milliseconds()
	}()

	metrics, err := c.Collect(cctx)
	if err != nil {
		*s.collectErrors[name]++
		slog.Warn("collect failed", "collector", name, "err", err)
		return
	}
	if len(metrics) > 0 {
		s.sink.Submit(metrics)
	}
}
