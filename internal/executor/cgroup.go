//go:build linux

// cgroup.go — 指令执行的 cgroup v2 资源隔离（M5d，防御纵深第二层；第一层是进程组查杀）。
//
// 语义：把被控进程移入 exec-<cmd_id> 子组，限制 memory.max / cpu.max；
// 组内 OOM 由内核查杀，不波及 agent 自身（agent 在 systemd 的 200MB 配额内）。
//
// 可用性降级：cgroup v2 未挂载/无委托权限时记 WARN 并跳过（进程组查杀仍生效），
// 麒麟 V10（cgroup v1 默认）即为该路径——生产启用需 systemd Delegate=yes + 统一层级。
package executor

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	cgroupV2Root = "/sys/fs/cgroup"
	cgroupParent = "tom_ai_agent" // systemd unit Delegate=yes 时委托给 agent 的子树
)

// cgroupConf 执行器资源限值。
type cgroupConf struct {
	Enabled     bool
	MemoryMaxMB int
	CPUQuotaPct int // 相对单核百分比，100=1 核
}

type cgroupManager struct {
	conf    cgroupConf
	baseDir string
	once    sync.Once
	usable  bool
}

func newCgroupManager(conf cgroupConf) *cgroupManager {
	return &cgroupManager{conf: conf}
}

// usable 惰性检测 cgroup v2 统一层级 + 写权限。
func (m *cgroupManager) check() bool {
	m.once.Do(func() {
		if !m.conf.Enabled {
			return
		}
		st, err := os.Stat(filepath.Join(cgroupV2Root, "cgroup.controllers"))
		if err != nil || st.IsDir() {
			slog.Warn("cgroup v2 unified hierarchy not available; command isolation degraded to process-group kill")
			return
		}
		m.baseDir = filepath.Join(cgroupV2Root, cgroupParent, "exec")
		if err := os.MkdirAll(m.baseDir, 0o755); err != nil {
			slog.Warn("cgroup subtree not delegated (need Delegate=yes); isolation degraded", "err", err)
			return
		}
		// 父级开启子树控制器委托
		for _, c := range []string{"+memory", "+cpu"} {
			_ = os.WriteFile(filepath.Join(cgroupV2Root, cgroupParent, "cgroup.subtree_control"), []byte(c), 0o644)
		}
		m.usable = true
		slog.Info("cgroup v2 isolation enabled", "base", m.baseDir,
			"memory_max_mb", m.conf.MemoryMaxMB, "cpu_quota_pct", m.conf.CPUQuotaPct)
	})
	return m.usable
}

// confine 创建指令子组并返回约束应用函数；不可用时返回 no-op。
func (m *cgroupManager) confine(cmdID string) (apply func(pid int), cleanup func()) {
	noop := func(int) {}
	if !m.check() {
		return noop, func() {}
	}
	// cmd_id 过滤路径不安全字符
	safe := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, cmdID)
	dir := filepath.Join(m.baseDir, "exec-"+safe)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		slog.Warn("create command cgroup failed, degraded", "cmd_id", cmdID, "err", err)
		return noop, func() {}
	}
	if m.conf.MemoryMaxMB > 0 {
		_ = os.WriteFile(filepath.Join(dir, "memory.max"),
			[]byte(strconv.Itoa(m.conf.MemoryMaxMB<<20)), 0o644)
	}
	if m.conf.CPUQuotaPct > 0 {
		// cpu.max 格式 "<quota> <period>"，period 100ms
		quota := m.conf.CPUQuotaPct * 1000 // 100% -> 100000/100000
		_ = os.WriteFile(filepath.Join(dir, "cpu.max"),
			[]byte(fmt.Sprintf("%d 100000", quota)), 0o644)
	}
	apply = func(pid int) {
		if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644); err != nil {
			slog.Warn("move process to cgroup failed", "cmd_id", cmdID, "pid", pid, "err", err)
		}
	}
	cleanup = func() {
		if err := os.Remove(dir); err != nil {
			slog.Debug("remove command cgroup", "dir", dir, "err", err)
		}
	}
	return apply, cleanup
}
