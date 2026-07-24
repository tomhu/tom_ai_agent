//go:build linux

// plugin.go — 受管 Exec 插件框架（M7，agent-design.md §8.3 方案 C）。
//
// 治理原则：
//   - 插件 = 目录 /usr/libexec/tom_ai_agent/plugins/<name>/，含 manifest.yaml + 可执行文件
//   - manifest 与可执行文件必须 root 所有且组/其他人不可写（防低权篡改注入）
//   - exec 路径必须解析在插件目录内（禁绝对路径与 ..）
//   - 动作注册为 plugin.<name>.<action>，复用执行器的进程组查杀/cgroup/输出截断
//   - 二期：制品通道下发 + 签名验章（Artifact Service），本版只做静态装载
package plugin

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/tomhu/tom_ai_agent/internal/executor"
)

// Manifest 插件清单。
type Manifest struct {
	Name    string           `yaml:"name"`
	Version string           `yaml:"version"`
	Actions []ManifestAction `yaml:"actions"`
}

type ManifestAction struct {
	ID         string   `yaml:"id"`
	Risk       string   `yaml:"risk"`        // low / medium / high
	Exec       string   `yaml:"exec"`        // 相对插件目录的可执行文件
	Args       []string `yaml:"args"`        // 固定参数模板（支持 {param} 占位，值域同内置动作）
	MaxTimeout string   `yaml:"max_timeout"` // 如 30s；超过执行器上限按上限封顶
	Output     string   `yaml:"output"`      // result=透传 stdout；metrics=JSON 指标契约（二期）
}

var (
	reName   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	reActID  = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)
	reOutput = regexp.MustCompile(`^(result|metrics)$`)
)

// LoadDir 扫描插件目录，返回通过全部校验的动作（注册名 plugin.<name>.<id>）。
// 单个插件损坏只跳过并记 ERROR，不影响 agent 启动。
func LoadDir(root string) []*executor.Action {
	ents, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("read plugin dir failed", "dir", root, "err", err)
		}
		return nil
	}
	var out []*executor.Action
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		acts, err := loadOne(filepath.Join(root, e.Name()))
		if err != nil {
			slog.Error("plugin rejected", "plugin", e.Name(), "err", err)
			continue
		}
		out = append(out, acts...)
	}
	if len(out) > 0 {
		slog.Info("plugins loaded", "dir", root, "actions", len(out))
	}
	return out
}

func loadOne(dir string) ([]*executor.Action, error) {
	mfPath := filepath.Join(dir, "manifest.yaml")
	if err := rootOwnedStrict(mfPath); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	data, err := os.ReadFile(mfPath)
	if err != nil {
		return nil, err
	}
	var mf Manifest
	if err := yaml.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if !reName.MatchString(mf.Name) {
		return nil, fmt.Errorf("invalid plugin name %q", mf.Name)
	}
	if filepath.Base(dir) != mf.Name {
		return nil, fmt.Errorf("dir %s != manifest name %s", filepath.Base(dir), mf.Name)
	}
	if len(mf.Actions) == 0 || len(mf.Actions) > 16 {
		return nil, fmt.Errorf("actions count %d out of range 1..16", len(mf.Actions))
	}

	var out []*executor.Action
	for i, a := range mf.Actions {
		act, err := buildAction(dir, &mf, &a)
		if err != nil {
			return nil, fmt.Errorf("action[%d] %q: %w", i, a.ID, err)
		}
		out = append(out, act)
	}
	return out, nil
}

func buildAction(dir string, mf *Manifest, a *ManifestAction) (*executor.Action, error) {
	if !reActID.MatchString(a.ID) {
		return nil, fmt.Errorf("invalid action id")
	}
	switch a.Risk {
	case "low", "medium", "high":
	default:
		return nil, fmt.Errorf("invalid risk %q", a.Risk)
	}
	if a.Output == "" {
		a.Output = "result"
	}
	if !reOutput.MatchString(a.Output) {
		return nil, fmt.Errorf("invalid output %q", a.Output)
	}

	// exec 必须解析在插件目录内
	if filepath.IsAbs(a.Exec) || strings.Contains(a.Exec, "..") {
		return nil, fmt.Errorf("exec must stay inside plugin dir: %q", a.Exec)
	}
	execPath := filepath.Join(dir, a.Exec)
	resolved, err := filepath.EvalSymlinks(execPath)
	if err != nil {
		return nil, fmt.Errorf("exec resolve: %w", err)
	}
	if !strings.HasPrefix(resolved, dir+string(os.PathSeparator)) {
		return nil, fmt.Errorf("exec symlink escapes plugin dir")
	}
	if err := rootOwnedStrict(resolved); err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}
	if st, err := os.Stat(resolved); err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
		return nil, fmt.Errorf("exec not executable")
	}

	maxTO := 0 * time.Second
	if a.MaxTimeout != "" {
		d, err := time.ParseDuration(a.MaxTimeout)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid max_timeout %q", a.MaxTimeout)
		}
		maxTO = d
	}

	return &executor.Action{
		ID:       "plugin." + mf.Name + "." + a.ID,
		Risk:     a.Risk,
		Binary:   resolved,
		Args:     append([]string(nil), a.Args...),
		PluginTO: maxTO,
	}, nil
}

// rootOwnedStrict 文件必须 uid 0 所有且组/其他人无任何写权限。
func rootOwnedStrict(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("stat_t unavailable")
	}
	if stat.Uid != 0 {
		return fmt.Errorf("must be owned by root (uid=%d)", stat.Uid)
	}
	if st.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("must not be group/world writable (mode=%o)", st.Mode().Perm())
	}
	return nil
}
