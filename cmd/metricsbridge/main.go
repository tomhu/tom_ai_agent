// metricsbridge — aiops.metrics → VictoriaMetrics 桥（指标链路闭环，platform-architecture.md §4）。
// 消费 connector 写入的 protojson MetricBatch，转 VM /api/v1/import 原生 JSON 行格式。
// 至少一次：POST 成功才提交 consumer offset；失败退避重试不提交（Kafka 侧不丢）。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

func main() {
	brokers := flag.String("kafka-brokers", "127.0.0.1:9092", "Kafka broker 列表（逗号分隔）")
	topic := flag.String("topic", "aiops.metrics", "指标 topic")
	group := flag.String("group", "metricsbridge", "consumer group")
	vmURL := flag.String("vm-url", "http://127.0.0.1:8428", "VictoriaMetrics 地址")
	flag.Parse()

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		slog.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cli.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	b := &bridge{cli: cli, vmImport: strings.TrimRight(*vmURL, "/") + "/api/v1/import",
		hc: &http.Client{Timeout: 15 * time.Second}}
	slog.Info("metricsbridge started", "topic", *topic, "group", *group, "vm", *vmURL)
	b.loop(ctx)
	slog.Info("metricsbridge stopped")
}

type bridge struct {
	cli      *kgo.Client
	vmImport string
	hc       *http.Client
}

func (b *bridge) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := cli_PollRecords(ctx, b.cli, 500)
		var body bytes.Buffer
		var records []*kgo.Record
		samples := 0
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
			var batch agentv1.MetricBatch
			if err := protojson.Unmarshal(r.Value, &batch); err != nil {
				slog.Warn("skip undecodable record", "offset", r.Offset, "err", err)
				return
			}
			samples += writeImportLines(&body, &batch)
		})
		if len(records) == 0 {
			continue
		}
		if samples > 0 {
			if err := b.post(ctx, &body); err != nil {
				slog.Warn("vm import failed, retry later (offsets uncommitted)", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				continue // 不提交 offset：整批重放（VM 按 __name__+labels+ts 去重无副作用）
			}
		}
		if err := b.cli.CommitRecords(ctx, records...); err != nil {
			slog.Warn("commit offsets", "err", err)
		} else {
			slog.Debug("flushed", "records", len(records), "samples", samples)
		}
	}
}

// writeImportLines MetricBatch → VM /api/v1/import JSON 行：
// {"metric":{"__name__":...,"asset_id":...,...labels},"values":[v],"timestamps":[ms]}
// 指标名含 '.' 替换为 '_'（PromQL 标识符约束；原名保留在 __name__ 之外无必要——
// agent 侧命名即点分，VM 查询用下划线名）。
func writeImportLines(buf *bytes.Buffer, batch *agentv1.MetricBatch) int {
	n := 0
	for _, s := range batch.Samples {
		m := map[string]string{
			"__name__": strings.ReplaceAll(s.Metric, ".", "_"),
			"asset_id": batch.AssetId,
		}
		for k, v := range s.Labels {
			m[k] = v
		}
		line, err := json.Marshal(map[string]any{
			"metric": m, "values": []float64{s.Value}, "timestamps": []int64{s.Timestamp},
		})
		if err != nil {
			continue
		}
		buf.Write(line)
		buf.WriteByte('\n')
		n++
	}
	return n
}

func (b *bridge) post(ctx context.Context, body *bytes.Buffer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.vmImport, bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}
	resp, err := b.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("vm import status %s: %s", resp.Status, msg)
	}
	return nil
}

// cli_PollRecords 阻塞最多 maxWait 拉一批（封装以便单测替换）。
func cli_PollRecords(ctx context.Context, cli *kgo.Client, maxRecords int) kgo.Fetches {
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	fetches := cli.PollRecords(pollCtx, maxRecords)
	if errs := fetches.Errors(); len(errs) > 0 && pollCtx.Err() == nil {
		for _, e := range errs {
			slog.Warn("fetch error", "topic", e.Topic, "err", e.Err)
		}
	}
	return fetches
}
