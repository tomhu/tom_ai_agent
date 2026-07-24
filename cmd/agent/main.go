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
	"github.com/tomhu/tom_ai_agent/internal/executor"
	"github.com/tomhu/tom_ai_agent/internal/inventory"
	"github.com/tomhu/tom_ai_agent/internal/plugin"
	"github.com/tomhu/tom_ai_agent/internal/register"
	"github.com/tomhu/tom_ai_agent/internal/reporter"
	"github.com/tomhu/tom_ai_agent/internal/uplink"
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
	sched := collector.NewScheduler(cfg, rep)
	inv := inventory.NewModule(&cfg.Inventory, rep)

	startAt := time.Now()
	hooks := &executor.Hooks{
		AgentStatus: func(ctx context.Context, params map[string]string) (string, error) {
			depth, dropped := rep.Stats()
			return fmt.Sprintf("version=%s uptime=%.0fs degraded=%v buffer_depth=%d buffer_dropped=%d asset_id=%s",
				version, time.Since(startAt).Seconds(), sched.Degraded(), depth, dropped, rep.AssetID()), nil
		},
		InventoryRefresh: func(ctx context.Context, params map[string]string) (string, error) {
			inv.Refresh(ctx)
			return "inventory full report triggered", nil
		},
		AllowTestActions: cfg.Executor.AllowTestActions,
	}
	engine, err := executor.NewEngine(&cfg.Executor, rep, hooks)
	if err != nil {
		slog.Error("init executor", "err", err)
		os.Exit(1)
	}
	if cfg.Executor.PluginDir != "" {
		engine.AddActions(plugin.LoadDir(cfg.Executor.PluginDir))
	}

	reg := register.New(cfg, rep.SetAssetID)
	app.Add(reg)
	app.Add(rep)
	app.Add(sched)
	app.Add(watchdog.NewSelfMonitor(cfg, rep, sched, version))
	app.Add(watchdog.NewSentinel(&cfg.Watchdog, sched, rep))
	app.Add(inv)

	// gRPC bootstrap 模式（P1）：未配置证书且未指定 asset_id 时，先同步注册拿证书再建上行。
	// 注册成功后 mTLS 三件套指向 data_dir/pki 下平台签发材料。
	if cfg.Uplink.Mode == "grpc" && cfg.Uplink.CertFile == "" && cfg.Agent.AssetID == "" {
		if err := reg.EnsureIdentity(ctx); err != nil {
			slog.Error("bootstrap register", "err", err)
			os.Exit(1)
		}
		if pki := reg.PKIPaths(); pki != nil {
			cfg.Uplink.CAFile, cfg.Uplink.CertFile, cfg.Uplink.KeyFile = pki.CA, pki.Cert, pki.Key
			slog.Info("mTLS identity from bootstrap", "cert", pki.Cert)
		}
	}
	if cfg.Executor.Enabled {
		app.Add(engine)
		switch cfg.Uplink.Mode {
		case "http":
			app.Add(executor.NewPoller(cfg, engine, rep.AssetID))
		case "grpc":
			up, err := uplink.NewGRPC(&cfg.Uplink, version, "v1", engine, rep.AssetID)
			if err != nil {
				slog.Error("init grpc uplink", "err", err)
				os.Exit(1)
			}
			rep.SetSink(up)
			app.Add(up)
		}
	} else if cfg.Uplink.Mode == "grpc" {
		up, err := uplink.NewGRPC(&cfg.Uplink, version, "v1", nil, rep.AssetID)
		if err != nil {
			slog.Error("init grpc uplink", "err", err)
			os.Exit(1)
		}
		rep.SetSink(up)
		app.Add(up)
	}

	if err := app.Start(ctx); err != nil {
		slog.Error("start modules", "err", err)
		os.Exit(1)
	}
	watchdog.NotifyReady()
	if watchdog.StartWatchdog(ctx.Done()) {
		slog.Info("systemd watchdog enabled")
	}
	// 启动审计事件（可靠队列，验证 results/audit 链路）
	if err := rep.SubmitReliable(reporter.QueueAudit,
		fmt.Sprintf("agent.start-%d", time.Now().UnixNano()),
		map[string]any{"event": "agent.start", "version": version}); err != nil {
		slog.Warn("emit start event failed", "err", err)
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
	rep.Close()
	slog.Info("agent stopped")
}
