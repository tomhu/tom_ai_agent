// Package reporter 实现三级队列上报（设计文档 §4.1，M3 版）。
//
// 三类数据、三种可靠性语义：
//   - metrics：内存环形缓冲，满则丢最老并计数（有界丢失，保护业务主机）
//   - results/audit：WAL 先持久化，发送成功后推进游标（至少一次，平台按 id 去重）
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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tomhu/tom_ai_agent/internal/collector"
	"github.com/tomhu/tom_ai_agent/internal/config"
)

// QueueKind 队列类型。
type QueueKind string

const (
	QueueMetrics   QueueKind = "metrics"
	QueueResults   QueueKind = "results"
	QueueAudit     QueueKind = "audit"
	QueueInventory QueueKind = "inventory"
)

// Batch 指标发送单元（proto 冻结后切换为 HostMetricBatch）。
type Batch struct {
	AssetID  string             `json:"asset_id,omitempty"`
	SentAt   int64              `json:"sent_at"`
	Sequence uint64             `json:"sequence"`
	Samples  []collector.Metric `json:"samples"`
}

// ReliableItem 可靠队列条目（结果/审计事件等，payload 由生产者定义）。
type ReliableItem struct {
	ID        string          `json:"id"`
	Kind      QueueKind       `json:"kind"`
	CreatedAt int64           `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

// ReliableBatch 可靠队列发送单元。
type ReliableBatch struct {
	AssetID string         `json:"asset_id,omitempty"`
	Kind    QueueKind      `json:"kind"`
	SentAt  int64          `json:"sent_at"`
	Items   []ReliableItem `json:"items"`
}

// Sink 发送目标。
type Sink interface {
	SendMetrics(ctx context.Context, b *Batch) error
	SendReliable(ctx context.Context, b *ReliableBatch) error
}

// Reporter 三级队列上报器。
type Reporter struct {
	sink          Sink
	assetID       string
	dataDir       string
	bufferSize    int
	batchSize     int
	batchInterval time.Duration
	walEnabled    bool
	walMaxBytes   int64

	// metrics：内存缓冲（可丢弃）
	mu      sync.Mutex
	buf     []collector.Metric
	seq     uint64
	dropped uint64

	// 可靠队列：WAL 背书
	wals map[QueueKind]*WAL
	// metrics WAL 兜底（可选，配额最小；MQ 故障时落盘）
	metricsWAL *WAL

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
		sink = &httpSink{base: cfg.Uplink.Addr, client: &http.Client{Timeout: 10 * time.Second}}
	case "grpc":
		sink = nil // 由 main 构建 uplink 后 SetSink 注入
	default:
		return nil, fmt.Errorf("unknown uplink.mode: %s", cfg.Uplink.Mode)
	}

	r := &Reporter{
		sink:          sink,
		assetID:       cfg.Agent.AssetID,
		dataDir:       cfg.Agent.DataDir,
		bufferSize:    cfg.Reporter.BufferSize,
		batchSize:     cfg.Reporter.BatchSize,
		batchInterval: cfg.Reporter.BatchInterval,
		walEnabled:    cfg.Reporter.WAL.Enabled,
		walMaxBytes:   int64(cfg.Reporter.WAL.MaxMB) << 20,
		wals:          map[QueueKind]*WAL{},
	}

	if r.walEnabled {
		if err := os.MkdirAll(filepath.Join(r.dataDir, "wal"), 0o700); err != nil {
			return nil, err
		}
		for _, kind := range []QueueKind{QueueResults, QueueAudit, QueueInventory} {
			w, err := OpenWAL(filepath.Join(r.dataDir, "wal", string(kind)), r.walMaxBytes)
			if err != nil {
				return nil, fmt.Errorf("open wal %s: %w", kind, err)
			}
			r.wals[kind] = w
		}
		if cfg.Reporter.WAL.MetricsFallback {
			w, err := OpenWAL(filepath.Join(r.dataDir, "wal", "metrics"), r.walMaxBytes)
			if err != nil {
				return nil, err
			}
			r.metricsWAL = w
		}
	}
	return r, nil
}

func (r *Reporter) Name() string { return "reporter" }

// SetSink 注入发送目标（gRPC 模式：由 uplink 实现 Sink，main 在 Start 前注入）。
func (r *Reporter) SetSink(s Sink) { r.sink = s }

// Submit 指标入口（metrics 队列，可丢弃）。
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

// SubmitReliable 可靠队列入口：先 WAL 落盘（fsync）再异步发送。
// WAL 写入失败时返回错误，由调用方按 fail-closed 策略处理（高危动作拒绝执行）。
func (r *Reporter) SubmitReliable(kind QueueKind, id string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	item := ReliableItem{ID: id, Kind: kind, CreatedAt: time.Now().UnixMilli(), Payload: raw}
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	w, ok := r.wals[kind]
	if !ok {
		return fmt.Errorf("reliable queue %s unavailable (wal disabled)", kind)
	}
	return w.Append(data)
}

// SetAssetID 注册完成后回填平台签发身份（register 模块回调）。
func (r *Reporter) SetAssetID(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.assetID = id
}

func (r *Reporter) currentAssetID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.assetID
}

// AssetID 当前身份（指令通道轮询用）。
func (r *Reporter) AssetID() string { return r.currentAssetID() }

// Stats 供自监控上报。
func (r *Reporter) Stats() (depth, dropped uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return uint64(len(r.buf)), r.dropped
}

// WALPending 各可靠队列积压字节数。
func (r *Reporter) WALPending() map[string]int64 {
	out := map[string]int64{}
	for kind, w := range r.wals {
		out[string(kind)] = w.PendingBytes()
	}
	return out
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
				r.flushMetrics(context.Background())
				return
			case <-ticker.C:
				r.flushMetrics(ctx)
			}
		}
	}()

	// 可靠队列重放循环（每类独立）
	for kind, w := range r.wals {
		kind, w := kind, w
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.replayLoop(ctx, kind, w)
		}()
	}

	// 指标 WAL 兜底重放（低速率：实时优先，历史限速补送）
	if r.metricsWAL != nil {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.replayMetricsWALLoop(ctx, r.metricsWAL)
		}()
	}
	return nil
}

// flushMetrics 发送 metrics 缓冲；失败时保留缓冲，可选写 WAL 兜底。
func (r *Reporter) flushMetrics(ctx context.Context) {
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
		b := &Batch{AssetID: r.currentAssetID(), SentAt: time.Now().UnixMilli(), Sequence: r.seq, Samples: samples}
		if err := r.sink.SendMetrics(ctx, b); err != nil {
			slog.Warn("send metrics failed", "seq", b.Sequence, "err", err)
			if r.metricsWAL != nil {
				if data, jerr := json.Marshal(b); jerr == nil {
					if werr := r.metricsWAL.Append(data); werr != nil {
						slog.Error("metrics wal fallback failed", "err", werr)
					}
				}
				// 已落 WAL：从缓冲移除，避免内存无限积压
				r.mu.Lock()
				r.buf = r.buf[n:]
				r.mu.Unlock()
			}
			return
		}

		r.mu.Lock()
		r.buf = r.buf[n:]
		r.mu.Unlock()
		if n < r.batchSize {
			return
		}
	}
}

// replayLoop 可靠队列：按游标读 WAL → 发送 → 成功才推进游标。限速退避。
func (r *Reporter) replayLoop(ctx context.Context, kind QueueKind, w *WAL) {
	cursor := w.LoadCursor()
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		items, next, err := w.ReadFrom(cursor, r.batchSize)
		if err != nil {
			slog.Error("wal read failed", "kind", kind, "err", err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			continue
		}
		if len(items) == 0 {
			if !sleepCtx(ctx, r.batchInterval) {
				return
			}
			continue
		}

		batch := &ReliableBatch{AssetID: r.currentAssetID(), Kind: kind, SentAt: time.Now().UnixMilli()}
		for _, raw := range items {
			var it ReliableItem
			if json.Unmarshal(raw, &it) == nil {
				batch.Items = append(batch.Items, it)
			}
		}

		if err := r.sink.SendReliable(ctx, batch); err != nil {
			slog.Warn("send reliable failed, retry later", "kind", kind, "items", len(items), "err", err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = time.Second
		cursor = next
		if err := w.SaveCursor(cursor); err != nil {
			slog.Error("save wal cursor failed", "kind", kind, "err", err)
		}
	}
}

// replayMetricsWALLoop 指标兜底 WAL 重放：实时优先，历史数据限速（每 2s 一批）补送。
func (r *Reporter) replayMetricsWALLoop(ctx context.Context, w *WAL) {
	cursor := w.LoadCursor()
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second): // 限速：不挤压实时流量
		}

		items, next, err := w.ReadFrom(cursor, r.batchSize)
		if err != nil || len(items) == 0 {
			continue
		}
		for _, raw := range items {
			var b Batch
			if err := json.Unmarshal(raw, &b); err != nil {
				continue
			}
			if err := r.sink.SendMetrics(ctx, &b); err != nil {
				if !sleepCtx(ctx, backoff) {
					return
				}
				if backoff < 60*time.Second {
					backoff *= 2
				}
				goto wait // 网关仍不可达，等待下一轮
			}
		}
		backoff = time.Second
		cursor = next
		if err := w.SaveCursor(cursor); err != nil {
			slog.Error("save metrics wal cursor failed", "err", err)
		}
		slog.Info("metrics wal replayed", "cursor_segment", cursor.Segment, "offset", cursor.Offset)
	wait:
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Close 关闭 WAL 文件句柄。
func (r *Reporter) Close() {
	for _, w := range r.wals {
		w.Close()
	}
	if r.metricsWAL != nil {
		r.metricsWAL.Close()
	}
}

// ---------- Sinks ----------

// stdoutSink 调试用：JSON 行输出。
type stdoutSink struct{}

func (s *stdoutSink) SendMetrics(ctx context.Context, b *Batch) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func (s *stdoutSink) SendReliable(ctx context.Context, b *ReliableBatch) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// httpSink 开发用模拟网关：gzip JSON POST，按路径分流。
type httpSink struct {
	base   string
	client *http.Client
}

func (s *httpSink) post(ctx context.Context, path string, payload []byte) error {
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+path, &gz)
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

func (s *httpSink) SendMetrics(ctx context.Context, b *Batch) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return s.post(ctx, "/v1/metrics", data)
}

func (s *httpSink) SendReliable(ctx context.Context, b *ReliableBatch) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return s.post(ctx, "/v1/"+string(b.Kind), data)
}
