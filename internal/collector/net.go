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

// Net 采集器：/proc/net/dev 各网卡收发，计算周期间速率。
// 过滤 lo/veth/docker 等虚拟接口（基数控制）。
type Net struct {
	mu   sync.Mutex
	prev map[string]netCounters
	has  bool
	at   time.Time
}

type netCounters struct{ rxBytes, rxPackets, rxErrs, rxDrop, txBytes, txPackets, txErrs, txDrop uint64 }

func NewNet() *Net         { return &Net{prev: map[string]netCounters{}} }
func (n *Net) Name() string { return "net" }

func excludedIface(name string) bool {
	for _, p := range []string{"lo", "veth", "docker", "br-", "cni", "flannel", "kube"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func (n *Net) Collect(ctx context.Context) ([]Metric, error) {
	cur, err := readNetDev()
	if err != nil {
		return nil, err
	}
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.has {
		n.prev, n.has, n.at = cur, true, now
		return nil, nil // 首帧建基线
	}
	dt := now.Sub(n.at).Seconds()
	if dt <= 0 {
		return nil, nil
	}
	prev, prevAt := n.prev, n.at
	n.prev, n.at = cur, now

	ts := now.UnixMilli()
	var out []Metric
	rate := func(a, b uint64) float64 {
		if a < b {
			return 0
		}
		return float64(a-b) / dt
	}
	_ = prevAt
	for iface, c := range cur {
		p, ok := prev[iface]
		if !ok {
			continue // 新出现的接口，下一周期开始出数
		}
		labels := map[string]string{"iface": iface}
		out = append(out,
			Metric{Name: "net.rx.bps", Timestamp: ts, Value: rate(c.rxBytes, p.rxBytes), Labels: labels},
			Metric{Name: "net.tx.bps", Timestamp: ts, Value: rate(c.txBytes, p.txBytes), Labels: labels},
			Metric{Name: "net.rx.pps", Timestamp: ts, Value: rate(c.rxPackets, p.rxPackets), Labels: labels},
			Metric{Name: "net.tx.pps", Timestamp: ts, Value: rate(c.txPackets, p.txPackets), Labels: labels},
			Metric{Name: "net.rx.errps", Timestamp: ts, Value: rate(c.rxErrs, p.rxErrs), Labels: labels},
			Metric{Name: "net.tx.errps", Timestamp: ts, Value: rate(c.txErrs, p.txErrs), Labels: labels},
			Metric{Name: "net.rx.dropps", Timestamp: ts, Value: rate(c.rxDrop, p.rxDrop), Labels: labels},
			Metric{Name: "net.tx.dropps", Timestamp: ts, Value: rate(c.txDrop, p.txDrop), Labels: labels},
		)
	}
	return out, nil
}

func readNetDev() (map[string]netCounters, error) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return nil, fmt.Errorf("read /proc/net/dev: %w", err)
	}
	out := map[string]netCounters{}
	for _, line := range strings.Split(string(data), "\n") {
		i := strings.Index(line, ":")
		if i < 0 {
			continue
		}
		iface := strings.TrimSpace(line[:i])
		if excludedIface(iface) {
			continue
		}
		f := strings.Fields(line[i+1:])
		if len(f) < 16 {
			continue
		}
		var c netCounters
		vals := make([]uint64, 16)
		bad := false
		for j := 0; j < 16; j++ {
			vals[j], err = strconv.ParseUint(f[j], 10, 64)
			if err != nil {
				bad = true
				break
			}
		}
		if bad {
			continue
		}
		c = netCounters{vals[0], vals[1], vals[2], vals[3], vals[8], vals[9], vals[10], vals[11]}
		out[iface] = c
	}
	return out, nil
}
