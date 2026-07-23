//go:build linux

// engine.go — 指令执行引擎（设计文档 §5.4）。
// Worker 池 + 有界队列 + 超时两段式查杀（SIGTERM→宽限→SIGKILL 整个进程组）+
// 输出截断（头512K+尾512K）+ 按 cmd_id 取消 + 结果走可靠队列。
package executor

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
)

// Command 平台下发指令（M4 开发态 HTTP 协议；proto 冻结后切换信封）。
type Command struct {
	CmdID      string            `json:"cmd_id"`
	Action     string            `json:"action"`
	Params     map[string]string `json:"params"`
	TimeoutSec int               `json:"timeout_sec"`
}

// Result 执行结果。
type Result struct {
	CmdID      string  `json:"cmd_id"`
	Status     string  `json:"status"` // SUCCEEDED/FAILED/TIMEOUT_KILLED/CANCELLED/REJECTED_BUSY/REJECTED_POLICY
	ExitCode   int     `json:"exit_code"`
	Stdout     string  `json:"stdout,omitempty"`
	Stderr     string  `json:"stderr,omitempty"`
	Truncated  bool    `json:"truncated"`
	KillReason string  `json:"kill_reason,omitempty"` // timeout/cancel
	DurationMs int64   `json:"duration_ms"`
	StartedAt  int64   `json:"started_at"`
	FinishedAt int64   `json:"finished_at"`
}

// Engine 执行引擎。
type Engine struct {
	cfg     *config.ExecutorConf
	rep     *reporter.Reporter
	catalog map[string]*Action

	queue   chan Command
	running sync.Map // cmd_id -> cancel context.CancelFunc
	wg      sync.WaitGroup
}

func NewEngine(cfg *config.ExecutorConf, rep *reporter.Reporter, h *Hooks) *Engine {
	return &Engine{
		cfg:     cfg,
		rep:     rep,
		catalog: catalog(h),
		queue:   make(chan Command, cfg.QueueSize),
	}
}

func (e *Engine) Name() string { return "executor" }

// reportImmediate 回传未入队指令的终态（策略拒绝/队列满）。
func (e *Engine) reportImmediate(res Result) {
	if err := e.rep.SubmitReliable(reporter.QueueResults, res.CmdID, res); err != nil {
		slog.Error("submit immediate result failed", "cmd_id", res.CmdID, "err", err)
	}
}

// Submit 提交指令；队列满返回 REJECTED_BUSY（平台侧可重试，非终态）。
func (e *Engine) Submit(cmd Command) Result {
	if err := e.validate(&cmd); err != nil {
		return Result{CmdID: cmd.CmdID, Status: "REJECTED_POLICY", ExitCode: -1,
			Stderr: err.Error(), FinishedAt: time.Now().UnixMilli()}
	}
	select {
	case e.queue <- cmd:
		return Result{CmdID: cmd.CmdID, Status: "QUEUED", ExitCode: -1}
	default:
		return Result{CmdID: cmd.CmdID, Status: "REJECTED_BUSY", ExitCode: -1,
			Stderr: "executor queue full", FinishedAt: time.Now().UnixMilli()}
	}
}

func (e *Engine) validate(cmd *Command) error {
	a, ok := e.catalog[cmd.Action]
	if !ok {
		return fmt.Errorf("unknown action: %q", cmd.Action)
	}
	if cmd.Params == nil {
		cmd.Params = map[string]string{}
	}
	if a.Validate != nil {
		if err := a.Validate(cmd.Params); err != nil {
			return fmt.Errorf("param validation failed: %w", err)
		}
	}
	return nil
}

// Cancel 中止执行中任务（进程组查杀）。
func (e *Engine) Cancel(cmdID string) {
	if cancel, ok := e.running.Load(cmdID); ok {
		cancel.(context.CancelFunc)()
		slog.Info("cancel issued", "cmd_id", cmdID)
	}
}

// Stats 自监控。
func (e *Engine) Stats() (running, queued int) {
	n := 0
	e.running.Range(func(_, _ any) bool { n++; return true })
	return n, len(e.queue)
}

func (e *Engine) Start(ctx context.Context) error {
	workers := e.cfg.Workers
	if workers <= 0 {
		workers = 4
	}
	for i := 0; i < workers; i++ {
		e.wg.Add(1)
		go func(id int) {
			defer e.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cmd := <-e.queue:
					e.execute(ctx, cmd)
				}
			}
		}(i)
	}
	slog.Info("executor started", "workers", workers)
	return nil
}

// execute 执行单条指令并回传结果（可靠队列，WAL 背书）。
func (e *Engine) execute(parent context.Context, cmd Command) {
	res := Result{CmdID: cmd.CmdID, StartedAt: time.Now().UnixMilli()}
	a := e.catalog[cmd.Action]

	timeout := time.Duration(cmd.TimeoutSec) * time.Second
	maxTimeout := e.cfg.MaxTimeout
	if maxTimeout <= 0 {
		maxTimeout = 300 * time.Second
	}
	if timeout <= 0 || timeout > maxTimeout {
		timeout = maxTimeout // 平台超时不被信任为无上限
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	e.running.Store(cmd.CmdID, cancel)
	defer func() {
		cancel()
		e.running.Delete(cmd.CmdID)
	}()

	start := time.Now()
	var stdout, stderr *cappedBuffer
	var execErr error

	if a.Func != nil {
		out, err := a.Func(ctx, cmd.Params)
		stdout = newCappedBuffer(e.cfg.OutputLimitKB)
		stdout.Write([]byte(out))
		stderr = newCappedBuffer(e.cfg.OutputLimitKB)
		execErr = err
	} else {
		stdout, stderr, execErr = e.runExternal(ctx, cmd, a)
	}

	res.DurationMs = time.Since(start).Milliseconds()
	res.FinishedAt = time.Now().UnixMilli()
	res.Stdout = stdout.String()
	res.Stderr = stderr.String()
	res.Truncated = stdout.Truncated() || stderr.Truncated()

	switch {
	case ctx.Err() == context.DeadlineExceeded:
		res.Status = "TIMEOUT_KILLED"
		res.KillReason = "timeout"
		res.ExitCode = -1
	case ctx.Err() == context.Canceled && parent.Err() == nil:
		res.Status = "CANCELLED"
		res.KillReason = "cancel"
		res.ExitCode = -1
	case execErr != nil:
		res.Status = "FAILED"
		if exitErr, ok := execErr.(*exec.ExitError); ok {
			res.ExitCode = exitErr.ExitCode()
		} else {
			res.ExitCode = -1
			res.Stderr = res.Stderr + "\n" + execErr.Error()
		}
	default:
		res.Status = "SUCCEEDED"
		res.ExitCode = 0
	}

	if err := e.rep.SubmitReliable(reporter.QueueResults, cmd.CmdID, res); err != nil {
		slog.Error("submit result failed", "cmd_id", cmd.CmdID, "err", err)
	}
	slog.Info("command finished", "cmd_id", cmd.CmdID, "action", cmd.Action,
		"status", res.Status, "duration_ms", res.DurationMs)
}

// runExternal 外部命令执行：进程组隔离 + 两段式查杀。
func (e *Engine) runExternal(ctx context.Context, cmd Command, a *Action) (*cappedBuffer, *cappedBuffer, error) {
	argv := buildArgv(a, cmd.Params)
	c := exec.Command(argv[0], argv[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // 独立进程组
	// 环境最小化：不继承 agent 环境（防凭据泄露到子进程）
	c.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C"}

	stdout := newCappedBuffer(e.cfg.OutputLimitKB)
	stderr := newCappedBuffer(e.cfg.OutputLimitKB)
	c.Stdout = stdout
	c.Stderr = stderr

	if err := c.Start(); err != nil {
		return stdout, stderr, fmt.Errorf("start: %w", err)
	}
	pgid := c.Process.Pid

	done := make(chan error, 1)
	go func() { done <- c.Wait() }()

	grace := e.cfg.KillGrace
	if grace <= 0 {
		grace = 3 * time.Second
	}

	select {
	case err := <-done:
		return stdout, stderr, err
	case <-ctx.Done():
		// 两段式查杀：先 SIGTERM 进程组，宽限后 SIGKILL
		slog.Warn("killing command process group", "cmd_id", cmd.CmdID, "pgid", pgid, "reason", ctx.Err())
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		select {
		case err := <-done:
			return stdout, stderr, err
		case <-time.After(grace):
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			<-done
			return stdout, stderr, ctx.Err()
		}
	}
}

// cappedBuffer 输出截断：保留头 1/2 + 尾 1/2，超限置 truncated。
type cappedBuffer struct {
	limit     int
	head      []byte
	tail      []byte
	truncated bool
}

func newCappedBuffer(limitKB int) *cappedBuffer {
	if limitKB <= 0 {
		limitKB = 1024
	}
	return &cappedBuffer{limit: limitKB * 1024}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	half := b.limit / 2
	total := len(b.head) + len(b.tail)
	if total+len(p) <= b.limit {
		if len(b.tail) > 0 {
			b.tail = append(b.tail, p...)
		} else {
			b.head = append(b.head, p...)
		}
		return len(p), nil
	}
	b.truncated = true
	// 头满后写尾部环形
	if len(b.head) < half {
		need := half - len(b.head)
		b.head = append(b.head, p[:min(need, len(p))]...)
		if len(p) > need {
			b.tail = append(b.tail, p[need:]...)
		}
	} else {
		b.tail = append(b.tail, p...)
	}
	if len(b.tail) > half {
		b.tail = b.tail[len(b.tail)-half:]
	}
	return len(p), nil
}

func (b *cappedBuffer) String() string {
	if !b.truncated {
		return string(append(b.head, b.tail...))
	}
	return string(b.head) + "\n...[TRUNCATED]...\n" + string(b.tail)
}

func (b *cappedBuffer) Truncated() bool { return b.truncated }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
