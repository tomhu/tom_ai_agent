// sink_kafka.go — Kafka 数据出口（P2，platform-architecture.md §3 数据面）。
// 三个 topic：aiops.metrics（key=asset_id）/ aiops.reports（key=item id）/ aiops.events（key=event_type）。
// 投递失败返回 error：Reports/Metrics 流据此不给 agent ACK，agent 保 WAL 重发（至少一次语义不变）。
package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

const (
	TopicMetrics = "aiops.metrics"
	TopicReports = "aiops.reports"
	TopicEvents  = "aiops.events"
)

type KafkaSink struct {
	cli *kgo.Client
}

// NewKafkaSink 建立 producer（ACKS=all；franz-go 在 AllISRAcks 下自动启用幂等生产者，
// 配合平台侧按 id 去重兜底）。
func NewKafkaSink(brokers []string) (*KafkaSink, error) {
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DefaultProduceTopic(TopicMetrics),
	)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Ping(ctx); err != nil {
		cli.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	return &KafkaSink{cli: cli}, nil
}

func (k *KafkaSink) Close() { k.cli.Close() }

// produce 同步投递（5s 超时），失败由调用方决定是否向上传播（可靠流会传播以保住 agent WAL）。
func (k *KafkaSink) produce(topic string, key, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	return k.cli.ProduceSync(ctx, rec).FirstErr()
}

func (k *KafkaSink) MetricBatch(b *agentv1.MetricBatch) error {
	v, err := protojson.Marshal(b)
	if err != nil {
		return err
	}
	return k.produce(TopicMetrics, []byte(b.AssetId), v)
}

// Report 可靠条目：payload 已是 JSON，外包 kind/id/ts 信封。
func (k *KafkaSink) Report(kind, itemID string, payload []byte) error {
	v, err := json.Marshal(map[string]any{
		"kind": kind, "id": itemID, "received_at": time.Now().UnixMilli(),
		"payload": json.RawMessage(payload),
	})
	if err != nil {
		return err
	}
	return k.produce(TopicReports, []byte(itemID), v)
}

func (k *KafkaSink) Event(eventType string, attrs map[string]string) error {
	v, err := json.Marshal(map[string]any{
		"event_type": eventType, "attrs": attrs, "ts": time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return k.produce(TopicEvents, []byte(eventType), v)
}

// MultiSink 扇出：全部成功才算成功（日志在前，投递失败仍会重试，日志重复可接受）。
type MultiSink struct {
	Sinks []Sink
}

func (m MultiSink) MetricBatch(b *agentv1.MetricBatch) error {
	for _, s := range m.Sinks {
		if err := s.MetricBatch(b); err != nil {
			return err
		}
	}
	return nil
}

func (m MultiSink) Report(kind, itemID string, payload []byte) error {
	for _, s := range m.Sinks {
		if err := s.Report(kind, itemID, payload); err != nil {
			return err
		}
	}
	return nil
}

func (m MultiSink) Event(eventType string, attrs map[string]string) error {
	for _, s := range m.Sinks {
		if err := s.Event(eventType, attrs); err != nil {
			return err
		}
	}
	return nil
}
