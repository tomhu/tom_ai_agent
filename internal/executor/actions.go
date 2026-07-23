//go:build linux

// actions.go — 动作目录（设计文档 §5.3）。
// 原则：结构化动作 + 参数值域校验 + 固定二进制 argv，绝不经过 shell。
package executor

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// Action 动作定义。两种实现：固定 argv 外部命令 / 内部 Go 函数。
type Action struct {
	ID       string
	Risk     string // low / medium / high
	Validate func(params map[string]string) error
	// Exec 外部命令模板：Binary 绝对路径 + Args 模板（{param} 占位符，已校验后替换）
	Binary string
	Args   []string
	// Func 内部动作（不 fork 外部进程）
	Func func(ctx context.Context, params map[string]string) (string, error)
}

var (
	reServiceName = regexp.MustCompile(`^[a-zA-Z0-9_.@-]{1,64}$`)
	reSafePath    = regexp.MustCompile(`^/[a-zA-Z0-9_./@+-]{1,256}$`)
	reLines       = regexp.MustCompile(`^[1-9][0-9]{0,3}$`) // 1-9999
	reSleepSec    = regexp.MustCompile(`^([1-9]|[1-5][0-9]|60)$`)
)

// logPathAllowed 日志读取路径白名单前缀（防路径穿越）。
var logPathAllowed = []string{"/var/log/", "/var/lib/tom_ai_agent/"}

func validateService(params map[string]string) error {
	svc := params["service"]
	if !reServiceName.MatchString(svc) {
		return fmt.Errorf("invalid service name: %q", svc)
	}
	return nil
}

func validateLogTail(params map[string]string) error {
	path := params["path"]
	if !reSafePath.MatchString(path) || strings.Contains(path, "..") {
		return fmt.Errorf("invalid path: %q", path)
	}
	ok := false
	for _, p := range logPathAllowed {
		if strings.HasPrefix(path, p) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("path not in whitelist: %q", path)
	}
	if v := params["lines"]; v != "" && !reLines.MatchString(v) {
		return fmt.Errorf("invalid lines: %q", v)
	}
	return nil
}

// Hooks 内部动作依赖（由 main 注入）。
type Hooks struct {
	AgentStatus      func(ctx context.Context, params map[string]string) (string, error)
	InventoryRefresh func(ctx context.Context, params map[string]string) (string, error)
	AllowTestActions bool
}

// catalog 一期动作目录（只读诊断类）。
func catalog(h *Hooks) map[string]*Action {
	c := map[string]*Action{
		"diagnose.disk_usage": {
			ID: "diagnose.disk_usage", Risk: "low",
			Binary: "/usr/bin/df", Args: []string{"-h"},
		},
		"diagnose.memory_summary": {
			ID: "diagnose.memory_summary", Risk: "low",
			Binary: "/usr/bin/free", Args: []string{"-m"},
		},
		"diagnose.cpu_top": {
			ID: "diagnose.cpu_top", Risk: "low",
			Binary: "/usr/bin/top", Args: []string{"-b", "-n", "1"},
		},
		"diagnose.network_connections": {
			ID: "diagnose.network_connections", Risk: "low",
			Binary: "/usr/bin/ss", Args: []string{"-s"},
		},
		"diagnose.process_list": {
			ID: "diagnose.process_list", Risk: "low",
			Binary: "/usr/bin/ps", Args: []string{"-eo", "pid,ppid,user,pcpu,pmem,comm", "--sort=-pcpu"},
		},
		"diagnose.service_status": {
			ID: "diagnose.service_status", Risk: "low",
			Validate: validateService,
			Binary:   "/usr/bin/systemctl", Args: []string{"status", "--no-pager", "--", "{service}.service"},
		},
		"diagnose.read_log_tail": {
			ID: "diagnose.read_log_tail", Risk: "low",
			Validate: validateLogTail,
			Binary:   "/usr/bin/tail", Args: []string{"-n", "{lines}", "--", "{path}"},
		},
		"agent.status": {
			ID: "agent.status", Risk: "low",
			Func: h.AgentStatus,
		},
		"inventory.refresh": {
			ID: "inventory.refresh", Risk: "low",
			Func: h.InventoryRefresh,
		},
	}
	if h.AllowTestActions {
		// 仅联调：超时/查杀路径验证
		c["diagnose.test_sleep"] = &Action{
			ID: "diagnose.test_sleep", Risk: "medium",
			Validate: func(p map[string]string) error {
				if !reSleepSec.MatchString(p["seconds"]) {
					return fmt.Errorf("invalid seconds: %q", p["seconds"])
				}
				return nil
			},
			Binary: "/usr/bin/sleep", Args: []string{"{seconds}"},
		}
	}
	return c
}

// buildArgv 参数替换（校验已通过）；返回最终 argv（Binary + 替换后的 Args）。
func buildArgv(a *Action, params map[string]string) []string {
	argv := []string{a.Binary}
	for _, arg := range a.Args {
		for k, v := range params {
			arg = strings.ReplaceAll(arg, "{"+k+"}", v)
		}
		// 未填充的占位符使用默认值
		arg = strings.ReplaceAll(arg, "{lines}", "100")
		argv = append(argv, arg)
	}
	return argv
}
