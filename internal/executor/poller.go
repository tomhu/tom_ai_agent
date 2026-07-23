//go:build linux

// poller.go — 指令通道（M4 开发态：HTTP 长轮询；gRPC 控制流随 proto 冻结替换）。
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/config"
)

type pollResponse struct {
	Commands []Command `json:"commands"`
	Cancels  []string  `json:"cancels"`
}

// Poller 长轮询平台指令并分发给执行引擎。
type Poller struct {
	addr     string
	engine   *Engine
	assetID  func() string
	client   *http.Client
	interval time.Duration
}

func NewPoller(cfg *config.Config, engine *Engine, assetID func() string) *Poller {
	return &Poller{
		addr:     cfg.Uplink.Addr,
		engine:   engine,
		assetID:  assetID,
		client:   &http.Client{Timeout: 40 * time.Second},
		interval: 5 * time.Second,
	}
}

func (p *Poller) Name() string { return "command-poller" }

func (p *Poller) Start(ctx context.Context) error {
	go p.loop(ctx)
	return nil
}

func (p *Poller) loop(ctx context.Context) {
	backoff := p.interval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		id := p.assetID()
		if id == "" {
			if !sleep(ctx, p.interval) {
				return
			}
			continue // 未注册完成不轮询
		}

		resp, err := p.poll(ctx, id)
		if err != nil {
			slog.Debug("command poll failed", "err", err)
			if !sleep(ctx, backoff) {
				return
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = p.interval

		for _, cmdID := range resp.Cancels {
			p.engine.Cancel(cmdID)
		}
		for _, cmd := range resp.Commands {
			res := p.engine.Submit(cmd)
			if res.Status == "QUEUED" {
				slog.Info("command accepted", "cmd_id", cmd.CmdID, "action", cmd.Action)
			} else {
				slog.Warn("command not queued", "cmd_id", cmd.CmdID, "status", res.Status, "reason", res.Stderr)
				// 策略拒绝/队列满也要回传终态
				p.engine.reportImmediate(res)
			}
		}
	}
}

func (p *Poller) poll(ctx context.Context, assetID string) (*pollResponse, error) {
	url := fmt.Sprintf("%s/v1/commands?asset_id=%s&wait=25", p.addr, assetID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("poll status %s", resp.Status)
	}
	var pr pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}
	return &pr, nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
