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
	Register   RegisterConf             `yaml:"register"`
	Inventory  InventoryConf            `yaml:"inventory"`
	Executor   ExecutorConf             `yaml:"executor"`
}

// ExecutorConf 指令执行器（设计文档 §5.4）。
type ExecutorConf struct {
	Enabled          bool          `yaml:"enabled"`
	Workers          int           `yaml:"workers"`
	QueueSize        int           `yaml:"queue_size"`
	MaxTimeout       time.Duration `yaml:"max_timeout"`
	KillGrace        time.Duration `yaml:"kill_grace"`
	OutputLimitKB    int           `yaml:"output_limit_kb"`
	AllowTestActions bool          `yaml:"allow_test_actions"` // 仅联调：test_sleep 等
	// 信封验签（M5c）：配置后未签名/验签失败/重放信封一律 REJECTED_POLICY（fail-closed）
	CommandPubkeyFile string `yaml:"command_pubkey_file"` // Ed25519 公钥（PKIX PEM）
	// cgroup v2 执行隔离（M5d）：不可用时自动降级为仅进程组查杀
	Cgroup CgroupConf `yaml:"cgroup"`
	// Exec 插件目录（M7）：空=不装载
	PluginDir string `yaml:"plugin_dir"`
}

// CgroupConf cgroup v2 资源限值。
type CgroupConf struct {
	Enabled     bool `yaml:"enabled"`
	MemoryMaxMB int  `yaml:"memory_max_mb"`  // 单指令内存上限（组内 OOM 查杀）
	CPUQuotaPct int  `yaml:"cpu_quota_pct"`  // 单指令 CPU 上限（100=1 核）
}

// RegisterConf 注册引导（设计文档 §8.1）。
type RegisterConf struct {
	BootstrapToken string `yaml:"bootstrap_token"`  // 一次性引导凭据（正式版从文件读取）
	BootstrapAddr  string `yaml:"bootstrap_addr"`   // gRPC Bootstrap 地址（P1；空=走 M2 HTTP 回退）
	BootstrapCAFile string `yaml:"bootstrap_ca_file"` // 校验注册服务端的 CA（空=TOFU 跳过校验，仅开发）
}

// InventoryConf 资产采集上报（设计文档 §8.2）。
type InventoryConf struct {
	Enabled      bool                   `yaml:"enabled"`
	FullInterval time.Duration          `yaml:"full_interval"`
	Packages     InventoryPackagesConf  `yaml:"packages"`
	Processes    InventoryProcessesConf `yaml:"processes"`
}

type InventoryPackagesConf struct {
	Enabled  bool     `yaml:"enabled"`
	Patterns []string `yaml:"patterns"`
}

type InventoryProcessesConf struct {
	Enabled        bool     `yaml:"enabled"`
	UploadEnabled  bool     `yaml:"upload_enabled"` // 缓建决策：默认 false，仅采集缓存
	RedactPatterns []string `yaml:"redact_patterns"`
}

type AgentConf struct {
	DataDir  string `yaml:"data_dir"`
	LogLevel string `yaml:"log_level"`
	AssetID  string `yaml:"asset_id"` // 注册流程落地前可为空（M2 由注册服务签发）
}

type UplinkConf struct {
	Mode     string `yaml:"mode"`      // stdout | http | grpc
	Addr     string `yaml:"addr"`      // http: URL；grpc: host:port
	HTTPAddr string `yaml:"http_addr"` // grpc 模式下注册引导的 HTTP 回退地址（M5b 切 gRPC Bootstrap）
	Insecure bool   `yaml:"insecure"`  // grpc 明文（仅开发；M5b 后生产必须 false）
	// mTLS（M5b）：三件套为空即不启用；身份注册落地后由 PKI 流程接管签发
	CAFile     string `yaml:"ca_file"`     // 根 CA 证书（验证对端）
	CertFile   string `yaml:"cert_file"`   // agent 客户端证书（CN=asset_id）
	KeyFile    string `yaml:"key_file"`    // agent 客户端私钥
	ServerName string `yaml:"server_name"` // 覆盖 TLS SNI/证书校验名（默认取 addr 主机名）
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
		Executor: ExecutorConf{
			Enabled: true, Workers: 4, QueueSize: 64,
			MaxTimeout: 300 * time.Second, KillGrace: 3 * time.Second,
			OutputLimitKB: 1024,
			Cgroup:    CgroupConf{Enabled: true, MemoryMaxMB: 256, CPUQuotaPct: 100},
			PluginDir: "/usr/libexec/tom_ai_agent/plugins",
		},
		Register: RegisterConf{},
		Inventory: InventoryConf{
			Enabled:      true,
			FullInterval: 24 * time.Hour,
			Packages: InventoryPackagesConf{
				Enabled:  true,
				Patterns: []string{"kylin-*", "bes*", "goldendb*", "gaussdb*", "polardb*"},
			},
			Processes: InventoryProcessesConf{
				Enabled:        true,
				UploadEnabled:  false,
				RedactPatterns: []string{"--password=*", "--token=*", "--secret=*"},
			},
		},
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
