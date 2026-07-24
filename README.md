# tom_ai_agent

AIOps 主机智能代理（Host Agent）——单二进制 Linux 采集与受控执行代理。

## 文档

- `docs/agent-design.md` — agent 设计（v0.2，架构基线）
- `docs/platform-architecture.md` — 管控平台 > Gateway > Agent 三层总体架构（v0.3，含表结构）

## 当前状态：M4 指令执行器（agent 侧）

- [x] Core 模块生命周期（优雅启停、信号处理）
- [x] YAML 配置（零配置默认可启动）
- [x] 采集器框架（独立周期、失败隔离、5s 超时、panic recover、降级模式）
- [x] 基础采集器：cpu / memory / load / diskcap / net（纯 /proc 解析，无 CGO）
- [x] Reporter 三级队列：metrics（环形缓冲可丢弃）/ results+audit+inventory（WAL 背书至少一次）
- [x] WAL：32MB 分段、长度前缀+CRC32、原子游标、磁盘配额、损坏段隔离、限速重放
- [x] 指标发送失败 → WAL 兜底 → 恢复后限速补送（麒麟 V10 实测通过）
- [x] 资源哨兵：RSS 软限降级（暂停非关键采集器+GC）/ 硬限自我退出交 systemd
- [x] sd_notify：READY + WATCHDOG（stdlib 实现，零依赖）
- [x] 注册：bootstrap token + 幂等 enrollment_id，身份持久化与恢复（mock 注册服务联调通过）
- [x] 资产采集：静态配置/网卡/挂载/软件包（rpm 模式过滤）/进程清单（脱敏），可靠队列上送
- [x] 进程信息：采集能力具备，上送默认关闭（缓建决策）
- [x] 自监控指标：agent.uptime / rss / goroutines / buffer / wal.pending / degraded
- [x] 麒麟 V10 x86_64 systemd 常驻验证（tomagent 低权运行，RSS ~9MB，CPU <1%）
- [x] 指令执行器（M4）：动作目录（固定 argv 无 shell、参数值域校验）+ worker 池 4/队列 64
- [x] 进程组隔离（Setpgid）+ 两段式查杀（SIGTERM→3s 宽限→SIGKILL 全组），超时上限封顶
- [x] 输出截断（头 512K+尾 512K）、按 cmd_id 取消、结果走 WAL 可靠队列
- [x] 指令通道：HTTP 长轮询（开发态；gRPC 控制流随 proto 冻结替换）
- [x] M4 端到端麒麟实测 10/10：service_status / 超时查杀无残留 / 取消 / 策略拒绝 / 内部动作
- [x] proto v1 冻结（`proto/agent/v1/agent.proto`：Control/Metrics/Reports 三流 + Bootstrap）
- [x] gRPC 上行（M5a）：Hello/Welcome 握手、流式 ACK（MetricAck 推进缓冲 / ReportAck 全覆盖推进 WAL）、断线 waiter 即时失败 + 退避重连、指令信封经控制流下推（替代 HTTP 长轮询）
- [x] mTLS（M5b）：自研 dev PKI（Ed25519，tools/devca）、TLS1.3 双向认证、网关强制客户端证书 CN=asset_id 复核（本机 probe 正/反测试通过）
- [x] 信封验签（M5c）：规范序列化 + Ed25519 签名 + nonce 防重放，agent fail-closed（authenv 单测：篡改 8 种场景全拒）
- [x] cgroup v2 执行隔离（M5d）：memory.max/cpu.max 单指令限额，不可用时降级进程组查杀
- [x] M5 全栈麒麟 E2E 9/9（gRPC+mTLS+签名信封）：六组指令用例 + 握手 + RSS；网关日志确认 signed=true、CN 复核通过；负向实测：未签名信封 fail-closed 拒绝、网关重启后 agent 自动重连
- [x] Exec 插件框架（M7）：manifest 治理（root 所有/组不可写/目录内解析/符号链接防逃逸）、plugin.* 动作注册、插件级超时上限；麒麟 E2E 6/6（含篡改负向）

## 构建

```bash
# linux/amd64（海光/兆芯/通用 x86）
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/tom_ai_agent-linux-amd64 ./cmd/agent
# linux/arm64（鲲鹏/飞腾）
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/tom_ai_agent-linux-arm64 ./cmd/agent
```

## 部署（麒麟 V10 验证）

```bash
sudo bash scripts/install.sh ./tom_ai_agent-linux-amd64
# 编辑 /etc/tom_ai_agent/agent.yaml 后：
sudo systemctl enable --now tom_ai_agent
journalctl -u tom_ai_agent -f
```

开发调试（前台 stdout 输出指标批次）：

```bash
./tom_ai_agent -config configs/agent.yaml.example
```
