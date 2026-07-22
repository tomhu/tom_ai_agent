// Package core 提供模块注册与生命周期管理（设计文档 §2.3 Core 核心调度）。
package core

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Module 是可由 Core 统一编排的组件。
type Module interface {
	Name() string
	// Start 在 ctx 取消后应尽快自行收尾返回。
	Start(ctx context.Context) error
}

// App 持有全部模块并负责启动/停止编排。
type App struct {
	modules []Module
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func New() *App { return &App{} }

func (a *App) Add(m ...Module) { a.modules = append(a.modules, m...) }

func (a *App) Names() []string {
	n := make([]string, 0, len(a.modules))
	for _, m := range a.modules {
		n = append(n, m.Name())
	}
	return n
}

// Start 顺序启动各模块；任一模块启动失败则回滚已启动模块。
func (a *App) Start(ctx context.Context) error {
	ctx, a.cancel = context.WithCancel(ctx)
	for _, m := range a.modules {
		if err := m.Start(ctx); err != nil {
			a.cancel()
			a.wg.Wait()
			return fmt.Errorf("start module %s: %w", m.Name(), err)
		}
		slog.Debug("module started", "module", m.Name())
	}
	return nil
}

// Wait 供模块在后台 goroutine 中登记，Stop 时等待全部退出。
func (a *App) Wait(fn func()) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		fn()
	}()
}

// Stop 取消上下文并等待所有登记的 goroutine 退出。
func (a *App) Stop(ctx context.Context) {
	if a.cancel != nil {
		a.cancel()
	}
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("module stop timeout, forcing exit")
	}
}
