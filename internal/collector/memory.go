//go:build linux

package collector

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Memory 采集器：/proc/meminfo 核心字段（总量/可用/缓存/swap）。
type Memory struct{}

func NewMemory() *Memory       { return &Memory{} }
func (m *Memory) Name() string { return "memory" }

func (m *Memory) Collect(ctx context.Context) ([]Metric, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, fmt.Errorf("read /proc/meminfo: %w", err)
	}
	kv := map[string]float64{}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			continue
		}
		kv[strings.TrimSuffix(f[0], ":")] = v * 1024 // kB → bytes
	}

	total, okT := kv["MemTotal"]
	avail, okA := kv["MemAvailable"]
	if !okT || !okA || total <= 0 {
		return nil, fmt.Errorf("meminfo missing MemTotal/MemAvailable")
	}

	now := time.Now().UnixMilli()
	out := []Metric{
		{Name: "mem.total.bytes", Timestamp: now, Value: total},
		{Name: "mem.available.bytes", Timestamp: now, Value: avail},
		{Name: "mem.used.bytes", Timestamp: now, Value: total - avail},
		{Name: "mem.usage.percent", Timestamp: now, Value: (total - avail) * 100 / total},
	}
	if v, ok := kv["Buffers"]; ok {
		out = append(out, Metric{Name: "mem.buffers.bytes", Timestamp: now, Value: v})
	}
	if v, ok := kv["Cached"]; ok {
		out = append(out, Metric{Name: "mem.cached.bytes", Timestamp: now, Value: v})
	}
	if st, ok := kv["SwapTotal"]; ok && st > 0 {
		sf := kv["SwapFree"]
		out = append(out,
			Metric{Name: "mem.swap.total.bytes", Timestamp: now, Value: st},
			Metric{Name: "mem.swap.used.bytes", Timestamp: now, Value: st - sf},
		)
	}
	return out, nil
}
