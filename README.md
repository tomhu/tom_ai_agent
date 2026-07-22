# tom_ai_agent

AIOps 主机智能代理（Host Agent）——单二进制 Linux 采集与受控执行代理。

## 文档

- `docs/agent-design.md` — agent 设计（v0.2，架构基线）
- `docs/platform-architecture.md` — 管控平台 > Gateway > Agent 三层总体架构（v0.3，含表结构）

## 当前状态：M1 骨架

- [x] Core 模块生命周期（优雅启停、信号处理）
- [x] YAML 配置（零配置默认可启动）
- [x] 采集器框架（独立周期、失败隔离、5s 超时、panic recover）
- [x] 基础采集器：cpu / memory / load / diskcap / net（纯 /proc 解析，无 CGO）
- [x] Reporter：环形缓冲（背压丢最老+计数）+ 批量发送（stdout / http+gzip）
- [x] 自监控指标：agent.uptime / rss / goroutines / buffer
- [ ] WAL 磁盘兜底（M3）
- [ ] 注册与资产上报（M2）
- [ ] gRPC 上行 + Protobuf（proto 冻结后）
- [ ] 指令执行器（M4）
- [ ] 安全体系（M5）

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
