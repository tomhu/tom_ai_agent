// Package reporter 实现指标缓冲、批量发送（设计文档 §4 Reporter，M1 版）。
// M1 范围：内存环形缓冲 + 批量编码 + stdout/http sink；WAL 与三类队列分级在 M3 落地。
package reporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/collector"
	"github.com/tomhu/tom_ai_agent/internal/config"
)

// Batch 是发送单元（proto 冻结后切换为 HostMetricBatch）。
type Batch struct {
	AssetID  string             `json:"asset_id,omitempty"`
	SentAt   int64              `json:"sent_at"`
	Sequence uint64             `json:"sequence"`
	Samples  []collector.Metric `json:"samples"`
}

// Sink 发送目标。
type Sink interface {
	Send(ctx context.Context, b *Batch) error
}

// Reporter 缓冲采集结果并批量发送。
type Reporter struct {
	sink          Sink
	assetID       string
	bufferSize    int
	batchSize     int
	batchInterval time.Duration

	mu     sync.Mutex
	buf    []collector.Metric
	seq    uint64
	dropped uint64

	wg sync.WaitGroup
}

func New(cfg *config.Config) (*Reporter, error) {
	var sink Sink
	switch cfg.Uplink.Mode {
	case "stdout", "":
		sink = &stdoutSink{}
	case "http":
		if cfg.Uplink.Addr == "" {
			return nil, fmt.Errorf("uplink.addr required when mode=http")
		}
		sink = &httpSink{url: cfg.Uplink.Addr, client: &http.Client{Timeout: 10 * time.Second}}
	default:
		return nil, fmt.Errorf("unknown uplink.mode: %s (gRPC 待 proto 冻结后接入)", cfg.Uplink.Mode)
	}
	return &Reporter{
		sink:          sink,
		assetID:       cfg.Agent.AssetID,
		bufferSize:    cfg.Reporter.BufferSize,
		batchSize:     cfg.Reporter.BatchSize,
		batchInterval: cfg.Reporter.BatchInterval,
	}, nil
}

func (r *Reporter) Name() string { return "reporter" }

// Submit 实现 collector.Sink。缓冲满则丢弃最老数据并计数（背压保护：宁丢数据不可 OOM）。
func (r *Reporter) Submit(metrics []collector.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range metrics {
		if len(r.buf) >= r.bufferSize {
			r.buf = r.buf[1:]
			r.dropped++
		}
		r.buf = append(r.buf, m)
	}
}

// Stats 供自监控上报。
func (r *Reporter) Stats() (depth, dropped uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return uint64(len(r.buf)), r.dropped
}

func (r *Reporter) Start(ctx context.Context) error {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.batchInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				r.flush(context.Background()) // 退出前尽力发送
				return
			case <-ticker.C:
				r.flush(ctx)
			}
		}
	}()
	return nil
}

func (r *Reporter) flush(ctx context.Context) {
	for {
		r.mu.Lock()
		if len(r.buf) == 0 {
			r.mu.Unlock()
			return
		}
		n := r.batchSize
		if len(r.buf) < n {
			n = len(r.buf)
		}
		samples := make([]collector.Metric, n)
		copy(samples, r.buf[:n])
		r.mu.Unlock()

		r.seq++
		b := &Batch{AssetID: r.assetID, SentAt: time.Now().UnixMilli(), Sequence: r.seq, Samples: samples}
		if err := r.sink.Send(ctx, b); err != nil {
			slog.Warn("send batch failed, will retry", "seq", b.Sequence, "err", err)
			return // 保留在缓冲中，下个周期重试
		}

		r.mu.Lock()
		r.buf = r.buf[n:]
		r.mu.Unlock()

		if n < r.batchSize {
			return
		}
	}
}

// stdoutSink 调试用：JSON 行输出。
type stdoutSink struct{}

func (s *stdoutSink) Send(ctx context.Context, b *Batch) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// httpSink 开发用模拟网关：gzip JSON POST。
type httpSink struct {
	url    string
	client *http.Client
}

func (s *httpSink) Send(ctx context.Context, b *Batch) error {
	payload, err := json.Marshal(b)
	if err != nil {
		return err
	}
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, &gz)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("gateway returned %s", resp.Status)
	}
	return nil
}
