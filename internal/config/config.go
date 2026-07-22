// Package config 负责 agent.yaml 加载与默认值填充。
// 依赖收敛原则：YAML 是当前唯一第三方依赖；如需零依赖可后换手写解析。
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type ReporterConf struct {
	BufferSize    int           `yaml:"buffer_size"`
	BatchSize     int           `yaml:"batch_size"`
	BatchInterval time.Duration `yaml:"batch_interval"`
	WAL           WALConf       `yaml:"wal"`
}

type WALConf struct {
	Enabled          bool `yaml:"enabled"`
	MaxMB            int  `yaml:"max_mb"`             // 单队列磁盘配额
	MetricsFallback  bool `yaml:"metrics_fallback"`   // 指标发送失败时是否落 WAL 兜底
}

// WatchdogConf 资源哨兵阈值（设计文档 §7.1 第 2 层）。
type WatchdogConf struct {
	RSSSoftMB     int  `yaml:"rss_soft_mb"`     // 软限：进入降级（降采集+GC）
	RSSHardMB     int  `yaml:"rss_hard_mb"`     // 硬限：自我退出交 systemd 重启
	FDLimit       int  `yaml:"fd_limit"`
	DegradedMode  bool `yaml:"degraded_mode"`   // 是否允许自动降级
}

type Config struct {
	Agent      AgentConf                `yaml:"agent"`
	Uplink     UplinkConf               `yaml:"uplink"`
	Collectors map[string]CollectorConf `yaml:"collectors"`
	Reporter   ReporterConf             `yaml:"reporter"`
	Watchdog   WatchdogConf             `yaml:"watchdog"`
}

type AgentConf struct {
	DataDir  string `yaml:"data_dir"`
	LogLevel string `yaml:"log_level"`
	AssetID  string `yaml:"asset_id"` // 注册流程落地前可为空（M2 由注册服务签发）
}

type UplinkConf struct {
	Mode string `yaml:"mode"` // stdout | http （gRPC 在 proto 冻结后接入）
	Addr string `yaml:"addr"` // http 模式的目标 URL（开发用模拟网关）
}

type CollectorConf struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
}

// Load 读取配置文件；文件不存在时使用内置默认值（零配置可启动，仅上行地址必须显式给）。
func Load(path string) (*Config, error) {
	cfg := &Config{
		Agent:    AgentConf{DataDir: "/var/lib/tom_ai_agent", LogLevel: "info"},
		Uplink:   UplinkConf{Mode: "stdout"},
		Reporter: ReporterConf{
			BufferSize: 10000, BatchSize: 500, BatchInterval: time.Second,
			WAL: WALConf{Enabled: true, MaxMB: 100, MetricsFallback: true},
		},
		Watchdog: WatchdogConf{RSSSoftMB: 150, RSSHardMB: 190, FDLimit: 1024, DegradedMode: true},
		Collectors: map[string]CollectorConf{
			"cpu":     {Enabled: true, Interval: 10 * time.Second},
			"memory":  {Enabled: true, Interval: 10 * time.Second},
			"diskcap": {Enabled: true, Interval: 60 * time.Second},
			"net":     {Enabled: true, Interval: 10 * time.Second},
			"load":    {Enabled: true, Interval: 10 * time.Second},
		},
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // 内置默认值
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}
