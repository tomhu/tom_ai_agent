// tom_ai_agent — AIOps 主机智能代理
// M1 骨架：Core 生命周期 + 配置 + 结构化日志 + 基础采集器 + 上报通道
// 设计文档：docs/agent-design.md (v0.2)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/collector"
	"github.com/tomhu/tom_ai_agent/internal/config"
	"github.com/tomhu/tom_ai_agent/internal/core"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
	"github.com/tomhu/tom_ai_agent/internal/watchdog"
)

var (
	version   = "0.1.0-dev"
	buildTime = "unknown"
)

func main() {
	cfgPath := flag.String("config", "/etc/tom_ai_agent/agent.yaml", "配置文件路径")
	showVersion := flag.Bool("version", false, "打印版本并退出")
	flag.Parse()

	if *showVersion {
		fmt.Printf("tom_ai_agent %s (built %s)\n", version, buildTime)
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	// 结构化日志（slog），M2 接文件轮转
	level := slog.LevelInfo
	switch cfg.Agent.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	slog.Info("agent starting", "version", version, "config", *cfgPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 上报器：缓冲 + 批量发送（sink 由配置决定，M1: stdout/http；gRPC uplink 在协议冻结后接入）
	rep, err := reporter.New(cfg)
	if err != nil {
		slog.Error("init reporter", "err", err)
		os.Exit(1)
	}

	// 核心调度：注册模块
	app := core.New()
	app.Add(rep)
	app.Add(collector.NewScheduler(cfg, rep))
	app.Add(watchdog.NewSelfMonitor(cfg, rep, version))

	if err := app.Start(ctx); err != nil {
		slog.Error("start modules", "err", err)
		os.Exit(1)
	}
	slog.Info("agent started", "modules", app.Names())

	// 信号处理：SIGTERM/SIGINT 优雅退出；SIGHUP 预留热更新
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	for {
		s := <-sigCh
		if s == syscall.SIGHUP {
			slog.Info("SIGHUP received, config hot-reload TODO(M2)")
			continue
		}
		slog.Info("shutdown signal", "signal", s.String())
		break
	}

	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	app.Stop(stopCtx)
	slog.Info("agent stopped")
}
