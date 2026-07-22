# tom_ai_agent

AIOps 主机智能代理（Host Agent）——单二进制 Linux 采集与受控执行代理。

## 文档

- `docs/agent-design.md` — agent 设计（v0.2，架构基线）
- `docs/platform-architecture.md` — 管控平台 > Gateway > Agent 三层总体架构（v0.3，含表结构）

## 当前状态：M3 可靠性 + M2 注册/资产（agent 侧）

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
- [ ] gRPC 上行 + Protobuf（proto 冻结后）
- [ ] 指令执行器（M4）
- [ ] 安全体系：mTLS / 信封验签 / 动作目录 / cgroup 隔离（M5）
- [ ] Exec 插件（M7）

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
