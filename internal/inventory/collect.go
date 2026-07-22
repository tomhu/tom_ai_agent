//go:build linux

// Package inventory 实现资产信息采集与上报（设计文档 §8.2–8.4，M2 版）。
//
// 范围：静态基础配置、网络、存储、软件包（按模式过滤）、进程清单（脱敏）。
// 上报：全量（首启 + 周期校验）；进程信息默认仅采集缓存、不上送（缓建决策）。
package inventory

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
)

// Report 资产报告（full/delta）。
type Report struct {
	ReportType    string            `json:"report_type"` // full / delta
	CollectedAt   int64             `json:"collected_at"`
	Static        *StaticInfo       `json:"static,omitempty"`
	Interfaces    []IfaceInfo       `json:"interfaces,omitempty"`
	Mounts        []MountInfo       `json:"mounts,omitempty"`
	Packages      []PackageInfo     `json:"packages,omitempty"`
	Processes     []ProcessInfo     `json:"processes,omitempty"` // 仅 upload_enabled=true 时填充
	Runtime       *RuntimeInfo      `json:"runtime,omitempty"`
}

type StaticInfo struct {
	Hostname   string `json:"hostname"`
	MachineID  string `json:"machine_id,omitempty"`
	BootID     string `json:"boot_id,omitempty"`
	OS         string `json:"os"`
	Kernel     string `json:"kernel"`
	Arch       string `json:"arch"`
	CPUModel   string `json:"cpu_model"`
	CPUCores   int    `json:"cpu_cores"`
	MemTotalMB int64  `json:"mem_total_mb"`
	BoardSN    string `json:"board_sn,omitempty"` // dmidecode 需特权，失败留空
}

type IfaceInfo struct {
	Name  string   `json:"name"`
	MAC   string   `json:"mac"`
	Addrs []string `json:"addrs"`
}

type MountInfo struct {
	MountPoint string  `json:"mount_point"`
	Device     string  `json:"device"`
	FSType     string  `json:"fstype"`
	TotalGB    float64 `json:"total_gb"`
}

type PackageInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Arch    string `json:"arch"`
}

type ProcessInfo struct {
	PID     int    `json:"pid"`
	PPID    int    `json:"ppid"`
	User    string `json:"user"`
	Cmdline string `json:"cmdline"` // 已脱敏
}

type RuntimeInfo struct {
	UptimeSec  float64 `json:"uptime_sec"`
	TimeSource string  `json:"time_source,omitempty"`
}

// Collector 资产采集器（采集与上报解耦；进程默认仅缓存）。
type Collector struct {
	cfg *config.InventoryConf

	mu        sync.RWMutex
	lastProcs []ProcessInfo
}

func NewCollector(cfg *config.InventoryConf) *Collector {
	return &Collector{cfg: cfg}
}

// CollectFull 采集全量资产报告。
func (c *Collector) CollectFull(ctx context.Context) *Report {
	r := &Report{ReportType: "full", CollectedAt: time.Now().UnixMilli()}
	r.Static = c.collectStatic()
	r.Interfaces = collectInterfaces()
	r.Mounts = collectMounts()
	if c.cfg.Packages.Enabled {
		r.Packages = collectPackages(ctx, c.cfg.Packages.Patterns)
	}
	if c.cfg.Processes.Enabled {
		procs := c.collectProcesses()
		if c.cfg.Processes.UploadEnabled {
			r.Processes = procs
		}
	}
	r.Runtime = collectRuntime()
	return r
}

// LastProcesses 返回最近采集的进程清单（本地缓存，供后续上送策略使用）。
func (c *Collector) LastProcesses() []ProcessInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastProcs
}

func (c *Collector) collectStatic() *StaticInfo {
	s := &StaticInfo{}
	if hn, err := os.Hostname(); err == nil {
		s.Hostname = hn
	}
	s.MachineID = readTrimmed("/etc/machine-id")
	s.BootID = readTrimmed("/proc/sys/kernel/random/boot_id")
	s.Kernel = readTrimmed("/proc/sys/kernel/osrelease")
	s.Arch = runtimeGOARCH()

	if data, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "PRETTY_NAME=") {
				s.OS = strings.Trim(strings.TrimPrefix(line, "PRETTY_NAME="), `"`)
			}
		}
	}

	// CPU 型号与核数
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					s.CPUModel = strings.TrimSpace(parts[1])
				}
				break
			}
		}
	}
	s.CPUCores = countCPUCores()

	// 内存总量
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				f := strings.Fields(line)
				if len(f) >= 2 {
					if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil {
						s.MemTotalMB = kb / 1024
					}
				}
				break
			}
		}
	}

	// 主板 SN（需特权；无权限降级留空，M5 经 Privileged Helper）
	if out, err := exec.Command("dmidecode", "-s", "system-serial-number").Output(); err == nil {
		sn := strings.TrimSpace(string(out))
		if sn != "" && !strings.Contains(sn, "O.E.M.") && sn != "None" {
			s.BoardSN = sn
		}
	}
	return s
}

func collectInterfaces() []IfaceInfo {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []IfaceInfo
	for _, ifa := range ifaces {
		if ifa.Flags&net.FlagLoopback != 0 {
			continue
		}
		info := IfaceInfo{Name: ifa.Name, MAC: ifa.HardwareAddr.String()}
		addrs, err := ifa.Addrs()
		if err == nil {
			for _, a := range addrs {
				info.Addrs = append(info.Addrs, a.String())
			}
		}
		out = append(out, info)
	}
	return out
}

var pseudoFSTypes = map[string]bool{
	"tmpfs": true, "devtmpfs": true, "overlay": true, "squashfs": true,
	"proc": true, "sysfs": true, "cgroup": true, "cgroup2": true,
	"devpts": true, "mqueue": true, "shm": true, "securityfs": true,
	"debugfs": true, "tracefs": true, "pstore": true, "bpf": true,
	"autofs": true, "hugetlbfs": true, "configfs": true, "fusectl": true,
	"ramfs": true, "selinuxfs": true, "nsfs": true, "efivarfs": true,
}

func collectMounts() []MountInfo {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []MountInfo
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 || pseudoFSTypes[fields[2]] {
			continue
		}
		var st syscall.Statfs_t
		if err := syscall.Statfs(fields[1], &st); err != nil {
			continue
		}
		out = append(out, MountInfo{
			MountPoint: fields[1],
			Device:     fields[0],
			FSType:     fields[2],
			TotalGB:    float64(st.Blocks) * float64(st.Bsize) / (1 << 30),
		})
	}
	return out
}

// collectPackages 通过 rpm 查询并按模式过滤（麒麟 V10 为 rpm 系）。
func collectPackages(ctx context.Context, patterns []string) []PackageInfo {
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, "rpm", "-qa", "--qf", "%{NAME} %{VERSION}-%{RELEASE} %{ARCH}\n")
	out, err := cmd.Output()
	if err != nil {
		slog.Warn("rpm -qa failed", "err", err)
		return nil
	}
	var pkgs []PackageInfo
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		p := PackageInfo{Name: f[0], Version: f[1]}
		if len(f) >= 3 {
			p.Arch = f[2]
		}
		if matchAny(p.Name, patterns) {
			pkgs = append(pkgs, p)
		}
	}
	return pkgs
}

// matchAny shell 风格前缀/通配匹配（简化：支持尾部 *）。
func matchAny(name string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}
	for _, p := range patterns {
		if strings.HasSuffix(p, "*") {
			if strings.HasPrefix(name, strings.TrimSuffix(p, "*")) {
				return true
			}
		} else if name == p {
			return true
		}
	}
	return false
}

// collectProcesses 采集进程清单并按规则脱敏。
func (c *Collector) collectProcesses() []ProcessInfo {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var out []ProcessInfo
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		pi := ProcessInfo{PID: pid}
		if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
			// stat 字段：pid (comm) state ppid ...（comm 可能含空格，取最后一个 ')' 后）
			s := string(data)
			if i := strings.LastIndex(s, ")"); i > 0 && i+4 < len(s) {
				f := strings.Fields(s[i+1:])
				if len(f) >= 2 {
					pi.PPID, _ = strconv.Atoi(f[1])
				}
			}
		}
		if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
			cmdline := strings.ReplaceAll(strings.TrimRight(string(data), "\x00"), "\x00", " ")
			pi.Cmdline = redact(cmdline, c.cfg.Processes.RedactPatterns)
		}
		if fi, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err == nil {
			if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
				pi.User = strconv.Itoa(int(stat.Uid))
			}
		}
		out = append(out, pi)
	}

	c.mu.Lock()
	c.lastProcs = out
	c.mu.Unlock()
	return out
}

// redact 按模式脱敏命令行（如 --password=xxx → --password=***）。
func redact(cmdline string, patterns []string) string {
	for _, p := range patterns {
		// 模式形如 "--password=*"，匹配前缀后遮蔽值
		if !strings.HasSuffix(p, "*") {
			continue
		}
		prefix := strings.TrimSuffix(p, "*")
		idx := strings.Index(cmdline, prefix)
		if idx < 0 {
			continue
		}
		start := idx + len(prefix)
		end := start
		for end < len(cmdline) && cmdline[end] != ' ' {
			end++
		}
		cmdline = cmdline[:start] + "***" + cmdline[end:]
	}
	return cmdline
}

func collectRuntime() *RuntimeInfo {
	r := &RuntimeInfo{}
	if data, err := os.ReadFile("/proc/uptime"); err == nil {
		f := strings.Fields(string(data))
		if len(f) >= 1 {
			r.UptimeSec, _ = strconv.ParseFloat(f[0], 64)
		}
	}
	if out, err := exec.Command("timedatectl", "show", "-p", "NTPSynchronized", "--value").Output(); err == nil {
		r.TimeSource = "ntp_synced=" + strings.TrimSpace(string(out))
	}
	return r
}

func readTrimmed(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func runtimeGOARCH() string {
	var uts syscall.Utsname
	if err := syscall.Uname(&uts); err != nil {
		return "unknown"
	}
	var b []byte
	for _, c := range uts.Machine {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}

func countCPUCores() int {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return 0
	}
	return strings.Count(string(data), "processor\t:")
}
