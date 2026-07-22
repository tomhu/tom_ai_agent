//go:build linux

package collector

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CPU 采集器：解析 /proc/stat 汇总行，计算周期间增量得出使用率。
// 默认只采总量（每核指标基数大，设计文档 §3.1：默认关闭，平台动态开启）。
type CPU struct {
	mu   sync.Mutex
	prev cpuTimes
	has  bool
}

type cpuTimes struct{ user, nice, system, idle, iowait, irq, softirq, steal uint64 }

func NewCPU() *CPU { return &CPU{} }

func (c *CPU) Name() string { return "cpu" }

func (c *CPU) Collect(ctx context.Context) ([]Metric, error) {
	cur, err := readCPUTimes()
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.has {
		c.prev, c.has = cur, true
		return nil, nil // 首帧只建立基线
	}
	prev := c.prev
	c.prev = cur

	delta := func(a, b uint64) float64 {
		if a < b {
			return 0 // 计数器回绕保护
		}
		return float64(a - b)
	}
	total := delta(cur.user, prev.user) + delta(cur.nice, prev.nice) + delta(cur.system, prev.system) +
		delta(cur.idle, prev.idle) + delta(cur.iowait, prev.iowait) + delta(cur.irq, prev.irq) +
		delta(cur.softirq, prev.softirq) + delta(cur.steal, prev.steal)
	if total <= 0 {
		return nil, nil
	}

	now := time.Now().UnixMilli()
	pct := func(v float64) float64 { return v * 100 / total }
	return []Metric{
		{Name: "cpu.usage.user", Timestamp: now, Value: pct(delta(cur.user, prev.user) + delta(cur.nice, prev.nice))},
		{Name: "cpu.usage.system", Timestamp: now, Value: pct(delta(cur.system, prev.system))},
		{Name: "cpu.usage.iowait", Timestamp: now, Value: pct(delta(cur.iowait, prev.iowait))},
		{Name: "cpu.usage.steal", Timestamp: now, Value: pct(delta(cur.steal, prev.steal))},
		{Name: "cpu.usage.idle", Timestamp: now, Value: pct(delta(cur.idle, prev.idle))},
	}, nil
}

func readCPUTimes() (cpuTimes, error) {
	var t cpuTimes
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return t, fmt.Errorf("read /proc/stat: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 9 {
			return t, fmt.Errorf("unexpected /proc/stat format: %q", line)
		}
		vals := make([]uint64, 8)
		for i := 0; i < 8; i++ {
			vals[i], err = strconv.ParseUint(f[i+1], 10, 64)
			if err != nil {
				return t, fmt.Errorf("parse /proc/stat: %w", err)
			}
		}
		t = cpuTimes{vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]}
		return t, nil
	}
	return t, fmt.Errorf("cpu summary line not found in /proc/stat")
}

// Load 采集器：/proc/loadavg。
type Load struct{}

func NewLoad() *Load         { return &Load{} }
func (l *Load) Name() string { return "load" }

func (l *Load) Collect(ctx context.Context) ([]Metric, error) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return nil, fmt.Errorf("read /proc/loadavg: %w", err)
	}
	f := strings.Fields(string(data))
	if len(f) < 3 {
		return nil, fmt.Errorf("unexpected /proc/loadavg format")
	}
	now := time.Now().UnixMilli()
	out := make([]Metric, 0, 3)
	for i, name := range []string{"load.1m", "load.5m", "load.15m"} {
		v, err := strconv.ParseFloat(f[i], 64)
		if err != nil {
			return nil, fmt.Errorf("parse loadavg: %w", err)
		}
		out = append(out, Metric{Name: name, Timestamp: now, Value: v})
	}
	return out, nil
}
