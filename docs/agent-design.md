# tom_ai_agent — AIOps 主机智能代理设计方案

> 版本：v0.2（讨论稿，已合并外部评审意见）
> 日期：2026-07-19
> 状态：规划中（本文档用于多轮讨论，未定稿前不启动编码）
> 项目目录：`aiops_tools/tom_ai_agent`
> 变更说明：v0.2 基于对 `agent-design-chatgpt.md` 评审意见的逐条评估修订（评估结论见 §13），并新增 **CMDB 首次注册与资产信息采集模块（§8）**

---

## 1. 项目概述

### 1.1 定位

tom_ai_agent 是 AIOps 平台的**主机侧智能代理（Host Agent）**，部署在每一台被管 Linux 服务器上（含麒麟 V10，x86 / ARM 架构），承担三类核心职责：

1. **数据采集者**：周期采集主机性能与容量指标，批量编码后经区域网关（Agent Gateway）上送平台消息队列（Kafka），供时序存储与算法分析消费。
2. **指令执行者**：通过与 Gateway 的出站长连接接收平台下发的运维指令（结构化动作、脚本制品等），在安全管控下执行并将结果回传。
3. **资产上报者**：首次运行及后续变更时，采集主机资产信息（静态配置、硬件、进程等）上送 CMDB，支撑平台资产台账与拓扑标签下发。

同时 agent 必须具备**自我监控与自愈能力**：作为 20 万台规模部署的基础设施组件，agent 自身的可靠性必须高于被监控系统——它不能成为新的故障源，自身异常要能自动恢复。

### 1.2 设计目标（非功能性要求）

| 目标 | 要求 | 说明 |
|---|---|---|
| 单文件部署 | 静态编译单二进制 | `CGO_ENABLED=0` 全静态链接，无运行时依赖，scp 上去即可运行 |
| 跨平台 | Linux 全平台 | x86_64、aarch64（鲲鹏/飞腾）为一期目标；海光（x86）、兆芯（x86）天然兼容；龙芯 loong64 视验证情况；申威 sw64 需单独评估 |
| 低资源占用 | CPU < 1%（闲时）、内存 < 100MB（硬上限 200MB） | 数据中心生产服务器，agent 必须"隐形" |
| 高可靠 | 7×24 无人值守 | 看门狗自愈、崩溃自动拉起、磁盘/内存泄漏自保护 |
| 安全性 | 零信任设计 | 无入站端口、mTLS 设备证书、平台签名授权、最小权限执行、全量审计 |
| 可管控 | 指令可控可杀 | 进程组 + cgroup 双重隔离，超时强制查杀，并发与速率限制 |
| 规模适配 | 20 万台在线 | 区域 Cell + Gateway 汇聚，agent 仅出站连接区域 VIP |
| 兼容性 | 麒麟 V10 优先验证 | 内核 4.19+，systemd 环境，cgroup v1/v2 均需适配 |

### 1.3 非目标（本期不做）

- 不做业务应用层 APM（链路追踪），那是 eBPF/探针方案的职责；
- 不做日志全文采集（一期预留插件接口，二期对接）；
- 中间件/数据库专项深度采集（宝兰德 BES、GoldenDB 等）作为插件二期扩展；
- 不内置 AI 推理能力（算法在平台侧，agent 保持轻量）；
- 不做跳板机/SSH 通道的替代品——跳板机保留为应急 break-glass 通道（见 §6.6）。

---

## 2. 总体架构

### 2.1 关键架构决策（v0.2 修订）

v0.1 中"agent 直接连接 Kafka（既生产指标也消费指令）"的方案在 20 万台规模下不成立，v0.2 采纳评审意见改为**网关汇聚架构**：

> **Agent 无入站端口、只主动出站；Gateway 管理在线连接与主机寻址；Kafka 只部署在平台内部，不直接面对几十万 Agent；跳板机只做人机入口和应急通道。**

为什么 agent 不能直接消费 Kafka 指令（评审意见核心论点，经评估成立）：

1. **每主机一 Topic 不可行**：20 万台 = 20 万 Topic、60 万+ 分区副本，元数据与运维复杂度爆炸；
2. **共享 Consumer Group 无法寻址**：同组内分区只会分配给组内一个 consumer，发给主机 A 的指令可能被主机 B 消费；
3. **每 agent 独立 Consumer Group 读放大**：每个 agent 收到全部指令再本地过滤，一条指令被读 20 万次；
4. **凭据治理灾难**：20 万台生产服务器都持有 Kafka 凭据，安全暴露面巨大。

### 2.2 上下文：agent 在平台中的位置（修订后）

```
┌──────────────────────────── 平台侧（中心） ────────────────────────────┐
│  ┌───────────┐  ┌───────────────┐  ┌─────────┐  ┌──────────────────┐ │
│  │ 运维门户   │  │ Command Svc   │  │ Session │  │ Kafka 集群        │ │
│  │ (IAM+MFA  │─▶│ 命令状态机/    │─▶│ Router  │  │ metrics/events/  │ │
│  │  审批/2FA) │  │ 持久化/Outbox │  │ host→   │  │ audit/results/   │ │
│  └───────────┘  └───────────────┘  │ gateway │  │ inventory        │ │
│                                     └────┬────┘  └────────▲─────────┘ │
│  ┌───────────┐  ┌───────────────┐       │ gRPC            │          │
│  │ CMDB/URM  │◀─┤ 注册服务       │       │                 │          │
│  │ (标签下发) │  │ (签发asset_id) │       │                 │          │
│  └───────────┘  └───────────────┘       │                 │          │
└─────────────────────────────────────────┼─────────────────┼──────────┘
                                          │                 │
        ┌───────────────── 区域 Cell（每 IDC/网络域，管 1–3 万台）──────────┐
        │                          │                 │                  │
        │              ┌───────────▼─────────────────┴───────────┐      │
        │              │      Agent Gateway 集群（≥3 实例）        │      │
        │              │  长连接管理/指令路由/流量汇聚/指标批量转发   │      │
        │              └───────────▲─────────────────────────────┘      │
        │                          │ mTLS + HTTP/2(gRPC)，agent 主动出站 │
        │     ┌────────────────────┼────────────────────┐               │
        │     ▼                    ▼                    ▼               │
        │  tom_ai_agent        tom_ai_agent        tom_ai_agent  ...    │
        └──────────────────────────────────────────────────────────────┘
```

数据通路：

- **指标**：`Agent → 区域 Gateway → Kafka(metrics) → 时序存储`
- **指令**：`运维门户(IAM+MFA+审批) → Command Service → Session Router → Gateway → Agent 长连接`
- **结果/审计**：`Agent → Gateway → Kafka(results/audit)`（与指标通道隔离配额）
- **资产信息**：`Agent → Gateway → Kafka(inventory) → CMDB`（见 §8）
- **标签下发**：CMDB 维护 `asset_id → idc/cluster/业务` 映射，在 Gateway 或消费侧补充进指标，**不依赖 agent 本地配置的业务标签**（防克隆漂移、防伪造身份）

### 2.3 内部模块划分（一核心、六模块）

| 模块 | 职责 | 关键设计 |
|---|---|---|
| **Core 核心调度** | 进程生命周期、配置管理、模块编排、优雅启停 | 主 goroutine + signal 处理 + 模块注册表 |
| **Register 注册与资产** | 首次注册（换取 asset_id/证书）、资产信息采集上送 CMDB | 见 §8（v0.2 新增） |
| **Collector 采集器** | 性能/容量指标周期采集 | 插件化采集器，独立调度间隔，失败隔离，基数治理 |
| **Reporter 上报器** | 指标/事件/结果/审计分类缓冲、批量压缩、经 Gateway 上送 | 三类可靠性语义分级队列 + WAL 落盘兜底 |
| **Executor 执行器** | 接收指令、校验授权信封、执行动作/制品、回传结果 | 动作目录 + 工作池 + 进程组与 cgroup 双重查杀 |
| **Security 安全管控** | 设备证书、信封验签、防重放、审计 | mTLS + Ed25519 签名信封，全操作审计 |
| **Watchdog 看门狗** | 自我监控、资源限额、自愈重启 | 内部健康检查 + systemd 协同 + 资源哨兵 |

### 2.4 技术选型（Go 生态，v0.2 修订）

| 领域 | 选型 | 理由 |
|---|---|---|
| 语言/运行时 | Go 1.22+ | 单静态二进制、跨平台编译、goroutine 并发、内存占用低 |
| 系统指标 | `shirou/gopsutil` v3 | 纯 Go（/proc 解析），无 CGO，跨平台 |
| 上行通道 | gRPC over mTLS（HTTP/2，TCP 443） | 长连接双向流：控制流 + 指标批量流；旧网络设备兼容模式 = HTTPS 长轮询 |
| MQ 客户端 | agent **不直接连 Kafka**；Kafka 客户端只在 Gateway | 凭据收敛，安全边界清晰 |
| 数据协议 | **Protobuf（一期即采用，不再 JSON 先行）** | 协议一旦铺到 20 万台迁移成本极高；JSON 仅用于调试 |
| 配置 | YAML + 环境变量覆盖 | 运维友好 |
| 日志 | `log/slog` + lumberjack 轮转 | 结构化、零重依赖 |
| 进程隔离 | `os/exec` + `Setpgid` 进程组 **+ systemd transient scope / cgroup** | 双层：进程组管普通场景，cgroup 防 setsid/daemonize 逃逸 |

**依赖收敛原则**：每引入一个第三方库都要评审——优先标准库，其次纯 Go 无 CGO 库，确保 `CGO_ENABLED=0 go build` 单文件产出。gRPC 是 v0.2 引入的最大依赖，需在 M0 验证其静态编译产物体积与内存开销可接受；不可接受则降级为自研 HTTP/2 帧协议或长轮询（M0 决策点）。

---

## 3. 采集器设计（Collector）

### 3.1 采集指标体系（性能 + 容量，含基数治理）

一期采集四大类主机指标。v0.2 修订：**默认配置以控制时间序列基数为先**，高基数维度默认关闭或降频，按需动态开启：

| 指标 | v0.1 默认 | v0.2 默认 | 说明 |
|---|---|---|---|
| CPU 总量（user/sys/iowait/steal/idle） | 10s | 10s | 保留 |
| 每核 CPU | 10s | **默认关闭 / 60s**，平台可按主机动态开启 | 128 核机器每核一组序列，基数大头 |
| 内存核心字段 | 10s | 10s | 保留，减少重复派生指标 |
| 每设备 IO | 10s | 10s，**过滤 loop/ram/无效设备** | |
| 每分区容量/inode | 60s | 60s，过滤伪文件系统与容器临时挂载 | 容量预测场景支撑 |
| 每网卡 | 10s | 10s，**过滤 veth/docker/短命虚拟口** | |
| TCP 状态统计 | 30s | 30s（总量），明细按需 | |
| 系统元信息 | 300s | **仅变化时上报** + 周期校验 | 与 §8 资产上报联动 |

**序列预算制度**：设计阶段即建立每主机序列预算（默认 ≤250 条活跃序列/主机，20 万台 ≈ 5000 万活跃序列的平台容量规划输入）、动态标签白名单、单主机最大 label 数上限。超限采集器自动降级并上报事件。

### 3.2 采集插件机制

```go
// 采集器接口（示意，讨论稿）
type Collector interface {
    Name() string
    Collect(ctx context.Context) ([]Metric, error)
    Interval() time.Duration   // 各采集器独立采集周期
}
```

- 每个采集器一个 goroutine，调度器按各自 `Interval()` 触发；
- **失败隔离**：单采集器 panic/超时（如读 /proc 卡死）不拖垮整体——每次 Collect 包 recover + 独立 context 超时（默认 5s）；
- **可开关、可调频**：配置 + 平台动态指令均可调整（动态调频走控制通道，不重启进程）；
- 二期预留：Exec 插件（脚本采集，约定 JSON/Protobuf 输出），与 §8 资产采集共用插件框架。

### 3.3 指标数据模型（v0.2：Protobuf 批量包）

评审意见成立：逐条 JSON、每条重复 host/arch/os/idc/cluster/agent_version，在 20 万台 × 10s 周期下编码/带宽/存储放大严重（每主机每 10s 若 5KB 压缩数据，平台入口即 100MB/s、8.6TB/天，Kafka 3 副本再 ×3）。v0.2 采用批量信封：

```protobuf
message HostMetricBatch {
  string  asset_id       = 1;   // 平台签发，整批一次
  int64   timestamp_ms   = 2;
  uint64  sequence       = 3;   // 单调递增，丢批可检测
  uint32  schema_version = 4;   // 协议版本协商
  repeated MetricSample samples = 5;
}
message MetricSample {
  uint32  metric_id = 1;        // 指标名字典ID，非字符串
  double  value     = 2;
  repeated uint32 label_refs = 3; // 设备/网卡/挂载点字典引用
}
```

要点：

- 公共字段（asset_id、时间戳）整批只出现一次；指标名映射为 `metric_id`；维度值字典化引用；
- agent 只上报**事实标签**（asset_id、arch、os 版本、boot_id、本地维度）；`idc/cluster/业务系统` 等拓扑标签由平台侧按 CMDB 补充（见 §2.2），agent 配置中**不再设置业务标签**；
- 协议必须具备版本协商能力（`schema_version`），为后续演进留路；
- JSON 仅保留为调试/人工排查的输出格式（本地命令行工具）。

---

## 4. 上报器设计（Reporter，v0.2 重写可靠性分级）

### 4.1 三类数据、三种可靠性语义

v0.1 单一"内存缓冲满丢最老"策略不适用于所有数据。v0.2 拆分三类队列：

| 队列 | 内容 | 允许丢弃？ | 策略 |
|---|---|---|---|
| Metrics | 性能容量指标 | **允许有界丢失** | 内存环形缓冲，满则丢最老并计数；WAL 兜底可选（默认开，配额最小） |
| Command Result | 指令 ACK/执行结果 | **不允许静默丢弃** | 独立 WAL，先持久化再发送，按 cmd_id 重试直至确认 |
| Audit / Security Event | 审计、认证失败、资产注册回执 | **不允许静默丢弃** | 独立 WAL；WAL 持久化失败时，高危指令执行直接 fail-closed |

v0.1 宣称的"至少一次"不严格（采集完成但没落盘也没收到 ACK 时崩溃会丢数据）。v0.2 明确：**指标 = 有界丢失的尽力交付（优先保护业务主机）；结果/审计 = 先持久化后确认的至少一次**，平台按 `cmd_id/event_id` 去重。

### 4.2 WAL 工程化要求

- 分段 + 长度前缀 + CRC 校验；损坏段隔离（跳过坏段继续，不全军覆没）；
- 原子索引 + fsync 策略（结果/审计类每次落盘 fsync，指标类批量 fsync）；
- 重放游标持久化；最大保留时长 + 磁盘配额双上限；
- **重放限速与流量配比**：Gateway 恢复后，实时数据优先，历史 WAL 保留固定带宽比例，避免积压数据挤爆实时流。

### 4.3 发送策略

- 批量：条数阈值（默认 500）或时间阈值（默认 1s）先到先发；
- 压缩：snappy / zstd（M0 对比测试定）；
- 上行目标 = 区域 Gateway VIP（非 Kafka）；失败指数退避（1s→60s 封顶）+ 抖动；
- 控制流与数据流分离：指令 ACK、安全事件走高优先级控制流，**指标洪峰不能阻塞命令 ACK**；
- 长连接治理：带抖动的重连、最大连接生命周期（配合 Gateway GOAWAY 排空，防 20 万连接同时重连风暴）。

---

## 5. 指令执行器设计（Executor，v0.2 安全模型重构）

这是 agent 中**风险最高**的模块。设计原则：**默认拒绝、最小权限、结构化授权、全程可审计、随时可杀**。

### 5.1 指令通道（v0.2 定案：Gateway 长连接）

v0.1 的三个候选（MQ 双向 / 轮询 / agent 开 HTTPS 端口）均存在规模或安全问题。v0.2 定案：

- agent 启动后向本区域 Gateway 建立 **mTLS gRPC 长连接（控制流）**，定时心跳；
- 平台侧：运维门户发起 → Command Service 持久化命令状态机 → Session Router 查 host 当前会话所在 Gateway → 经控制流下推；
- agent 离线时命令在 Command Service 持久化，上线后继续投递；
- agent **不开放任何入站端口**；仅保留 `127.0.0.1` 或 Unix Domain Socket 的本地健康/调试接口；
- 旧网络设备不支持 HTTP/2 时，降级 HTTPS 长轮询兼容模式（仍纯出站）。

### 5.2 授权模型（v0.2：双因素在平台侧，agent 验签名信封）

**对 v0.1 "agent 校验 TOTP" 的修正说明**：双因素认证的诉求保留，但实现位置从 agent 移到平台。原因（评审意见成立）：

- TOTP 是**用户身份因素**，把操作人/主机 TOTP 种子分发到 20 万台 agent 是严重的密钥分发与泄露风险；
- agent 无法、也不应判断"用户本人是否完成了 MFA"。

v0.2 双因素/多因素链路：

```
操作人 → 运维门户：IAM 认证 + MFA(TOTP/OTP)     ← 第一、第二因素在此完成
       → 审批服务：风险分级 / 高危命令双人审批
       → Command Service：生成签名授权信封(含 mfa_level 声明)
       → Agent：只验证平台签名 + 信封声明
```

**签名授权信封**（Ed25519 对规范化 Protobuf 信封签名，agent 持平台公钥验签）必须绑定：

| 字段 | 作用 |
|---|---|
| `cmd_id` / `nonce` | 防重放（agent 本地**持久化**去重，重启后仍有效——v0.1 内存去重有重启重放漏洞） |
| `asset_id` | 信封只对本机有效，他机截获不可用 |
| `action_id` / `payload_sha256` | 授权与载荷强绑定，payload 不可替换 |
| `not_before` / `expires_at` | 有效时间窗（分钟级） |
| `operator_id` / `approval_chain_hash` | 操作人与审批链留痕 |
| `mfa_level` / `risk_level` | agent 按策略要求高危动作必须 `mfa_level ≥ 2` |
| `policy_version` | 策略版本一致性 |

agent 离线或无法校验时效时**默认拒绝（fail-closed）**。

### 5.3 动作目录（替代字符串白名单）

v0.1 的 `"df *"`、`"cat /proc/*"` 字符串通配符白名单存在参数绕过、路径穿越、解析差异风险。v0.2 改为**结构化动作目录**：

```json
{
  "action": "diagnose.service_status",
  "params": { "service": "nginx" }
}
```

agent 内部映射为固定二进制绝对路径 + 固定参数模板：

```
/usr/bin/systemctl status --no-pager -- nginx.service
```

- 参数按值域/正则严格校验（如 service 名 `^[a-zA-Z0-9_.-]{1,64}$`），**绝不拼接进 shell**；一律直接 exec argv，无 `/bin/sh -c`；
- 一期动作目录（只读诊断类）：

| action_id | 说明 |
|---|---|
| `diagnose.disk_usage` / `diagnose.memory_summary` / `diagnose.cpu_top` | 资源快照 |
| `diagnose.service_status` | systemd 服务状态 |
| `diagnose.network_connections` | 连接统计 |
| `diagnose.read_log_tail` | 白名单路径日志 tail（路径前缀 + 大小上限） |
| `diagnose.process_list` / `diagnose.process_info` | 进程信息（与 §8 进程采集共用代码） |
| `agent.status` / `agent.reload_config` / `agent.collect_now` | agent 自身管理 |
| `inventory.refresh` | 触发一次资产信息全量采集上送（见 §8） |

- `script.run`（制品脚本）与 `agent.upgrade` 为一期实现但**默认关闭**，开启需 `risk_level=high` + `mfa_level≥2` + 审批链完整；写操作类（文件下发、服务重启）二期开放。

### 5.4 执行引擎与超时查杀（v0.2：进程组 + cgroup 双重约束）

评审意见成立：仅靠 `Setpgid` 进程组可被 `setsid()`/double-fork/daemonize 逃逸。v0.2 每个执行任务进入**独立 systemd transient scope（cgroup）**：

```
指令 ─▶ 验签/防重放/动作校验 ─▶ 任务队列 ─▶ Worker 池(N=4)
                                                 │
                                    systemd-run --scope（或 cgroup API）
                                    MemoryMax / CPUQuota / TasksMax /
                                    RuntimeMaxSec / KillMode=control-group
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                          正常完成            超时/取消          资源超限
                              │                  │                  │
                              ▼                  ▼                  ▼
                          回传结果      两段式查杀进程组      cgroup OOM/杀
                                       + systemd 杀 scope      回传事件
```

关键机制：

1. **双重隔离**：进程组（`Setpgid`，普通子树）+ cgroup scope（`KillMode=control-group` 杀整个 cgroup，setsid 逃逸无效）；
2. **两段式查杀**：超时先 SIGTERM 进程组，宽限 3s（可配）后 SIGKILL 进程组 + 终止 scope；
3. **资源限额随任务下发**：MemoryMax（默认 512MB）、CPUQuota、TasksMax（防 fork 炸弹，默认 256）、RuntimeMaxSec（与 timeout 联动）；网络访问默认关闭、按动作授权；
4. **超时封顶**：指令 `timeout_sec` 与 agent 配置 `max_timeout`（默认 300s）取小；
5. **并发限制**：Worker 池 4、队列 64，超出回传 `REJECTED_BUSY`；
6. **输出截断**：stdout/stderr 各 1MB 上限（头 512K + 尾 512K 标注截断）；
7. **取消**：平台按 `cmd_id` 下发 cancel，走同一查杀路径；
8. **cgroup v1/v2 适配**：麒麟 V10 各版本不一，M0 验证，systemd 不可用环境降级为纯进程组模式并记录能力降级事件。

### 5.5 脚本制品（不内嵌消息体）

脚本不塞进指令消息，走**制品引用**：

```
指令 = { artifact_id, sha256, 签名, entrypoint, params }
agent → 区域制品服务下载 → 验签 → 校验 SHA256 → 解压临时隔离目录
      → 降权 + 独立 cgroup 执行 → 到期清理
```

易审计、可复用、可撤销、可限流；大文件（升级包）同样走制品通道。

---

## 6. 安全设计（Security，v0.2 修订）

### 6.1 威胁模型

1. 指令通道被伪造 → 恶意命令下发；
2. 流量被窃听/篡改 → 指标与指令泄露劫持；
3. agent 二进制被替换/提权 → 持久化后门；
4. agent 自身漏洞（注入、路径穿越）被利用；
5. 凭据泄露被拷贝到他机冒用；
6. **单台 agent 被控后横向**：伪造身份标签、刷爆平台（基数/流量攻击）。

### 6.2 认证体系

- **设备层**：每主机专属 mTLS 客户端证书（平台 CA 签发，SAN 绑定 asset_id + 主机指纹），注册时签发（见 §8），支持在线轮转、过期前 30 天上报告警；agent 同时校验 Gateway 证书防假冒平台；
- **指令层**：§5.2 签名授权信封；**用户 MFA 只在平台完成**，agent 侧不保存任何用户因素种子；
- **本地接口**：仅 127.0.0.1/UDS，且只读状态查询。

### 6.3 权限模型（v0.2：默认非 root）

评审意见成立："一期默认 root" 风险过高。v0.2：

- agent 主进程运行于**专用低权用户 `tomagent`**；/proc、/sys 绝大多数指标采集无需 root；
- 少数特权操作（dmidecode、部分 netlink、cgroup 管理）由**极小的 Privileged Helper** 完成：Helper 只接受固定操作码，不接受任意命令字符串；可经 sudoers 精细授权或 setcap 实现；
- systemd unit 收缩能力：`CapabilityBoundingSet` 最小化、`NoNewPrivileges=true`、`ProtectSystem=strict`、`ProtectHome=true`、`PrivateTmp=true`、`MemoryMax=200M`、`MemoryHigh=100M`、`CPUQuota`、`TasksMax`；
- 执行任务的降权：动作默认以 `tomagent` 运行，个别诊断动作需要特权的单独声明并经 Helper 中转；
- **OOM 策略修正**：删除 v0.1 的 `OOMScoreAdjust=-900`（那会让内存紧张时内核先杀业务进程保 agent，与设计目标相反）。改为 OOMScoreAdjust 默认或略调高，agent 超限由 systemd 重启。

### 6.4 审计（v0.2 修订）

- 全量审计：指令接收、验签结果、执行、完成/被杀、每次认证失败——本地 jsonl（0600，轮转 30 天）**并实时上送** `audit` 队列（见 §4.1 最高可靠级）；
- **保留优先级明确**：审计 > 命令结果 > 指标 WAL > 运行日志；清理顺序反之——运行日志 → 指标 WAL → 非关键输出；**审计绝不因指标积压被删**；
- 本地 0600 防不住已获 root 的攻击者 → 审计**实时送中心不可变存储**，批次附哈希链/签名，本地仅为兜底；
- 字段：cmd_id、operator_id、信封摘要、动作+参数（脱敏规则可配）、退出码、耗时、查杀方式、mfa_level/risk_level 快照。

### 6.5 二进制完整性与升级

- 发布包 SHA256 + Ed25519 签名；
- **A/B 双槽自升级**：下载新二进制到 B 槽 → 验签 → 切换 → systemd 重启 → 健康检查失败自动回滚 A 槽；
- 升级指令 = 最高危等级：信封 `risk_level=high` + mfa_level≥2 + 平台灰度批次控制；agent 侧校验目标版本必须高于当前（防降级攻击）。

### 6.6 跳板机角色（保留为应急通道）

跳板机保留但限定为：人员登录入口、运维门户访问入口、应急 SSH/Ansible 通道、agent 平台整体不可用时的 break-glass 手段。**不允许**跳板机直接调用 agent（agent 无入站端口，天然做不到）、不允许其绕过 Command Service 下发命令。这样 Gateway/Kafka/控制面大面积故障时仍有受控应急路径，且无需为 agent 留高风险后门。

---

## 7. 自我监控与自愈（Watchdog，v0.2 修订）

### 7.1 三层自愈体系

```
第1层：内部健康检查（agent 进程内 watchdog goroutine）
  - 每 10s 检查各模块心跳：Collector 是否按时产出、Reporter 缓冲是否长期不降、
    Executor 队列是否长期打满、Gateway 连接是否假死
  - 异常处理：先模块级重启（不退出进程）

第2层：资源哨兵（自我限流，配合 systemd 硬限额）
  - 监控自身 RSS / CPU / goroutine / FD，接近阈值（RSS>150MB、FD>1024）：
    ① 降采集频率 ② 暂停非关键采集器 ③ 触发 GC ④ 仍恶化 → 自我退出（交第3层拉起）
  - 自我退出是"安全姿势"：宁可 agent 重启，不可拖垮生产服务器

第3层：外部守护（systemd）
  - Restart=always, RestartSec=5(带抖动), StartLimitBurst=5
  - MemoryMax/MemoryHigh/CPUQuota/TasksMax 硬限额（替代 v0.1 的 OOMScoreAdjust 方案）
  - WatchdogSec=30：agent 定期 sd_notify("WATCHDOG=1")，主循环死锁 → systemd 杀进程重启
```

### 7.2 自身可观测性

agent 把自身也当被监控对象，自监控指标走指标管道上送：`agent.uptime`、`agent.collect.duration/errors`、`agent.buffer.depth/dropped`、`agent.wal.size_bytes`、`agent.uplink.latency/errors`、`agent.exec.running/timeouts/rejected`、`agent.self.rss_bytes/cpu_percent/goroutines`、`agent.auth.failures`、`agent.inventory.last_report`（见 §8）。

**离线检测修正（v0.2）**：agent 离线后不会再上报 `agent.uptime`，所以离线判定必须由 Gateway 会话数据做：**CMDB 应存在资产 − Gateway 当前活跃会话 = 离线清单**，平台维护最近心跳、断开原因、boot_id、版本、证书状态——不能只查时序库还有没有指标。

### 7.3 磁盘与日志自保护

- 本地日志 + WAL + 审计统一在数据目录（默认 `/var/lib/tom_ai_agent/`），按 §6.4 优先级滚动清理；
- 数据分区使用率 > 90% 时主动降级：停指标 WAL（转丢弃+计数）→ 上报告警事件；审计 WAL 最后动。

---

## 8. 注册与资产信息采集模块（Register & Inventory，v0.2 新增）

### 8.1 功能定位

agent **首次运行**时完成两件事：

1. **注册（Register）**：向平台注册服务证明身份，换取不可变 `asset_id` 与设备证书——这是 agent 后续一切通信的身份基础；
2. **资产上报（Inventory）**：采集主机资产信息上送 CMDB，形成/更新资产台账，并作为平台侧拓扑标签补充的数据源。

后续运行中：资产信息**变更触发增量上报 + 周期全量校验 + 平台指令即时刷新**（`inventory.refresh` 动作）。

> 重要约束（评审意见采纳）：`/etc/machine-id` 可能因镜像克隆重复，**不能直接作为 agent ID**。注册流程必须以平台签发的 `asset_id` 为最终身份，machine-id、SN、MAC 等只作为注册时的"证明材料"和冲突检测输入。

### 8.2 采集内容（首版范围，后续可配置扩展）

**（1）静态基础配置**（注册 + 首报必采）
- 主机名、FQDN；OS 发行版与版本（麒麟 V10 具体 SP 小版本）、内核版本、架构（x86_64/aarch64/…）
- 硬件：CPU 型号/物理核/逻辑核、内存总量、主板/整机 SN 与厂商（dmidecode，经 Privileged Helper，无权限时降级留空）
- 标识材料：machine-id、boot_id、主网卡 MAC、磁盘序列号（可选）
- agent 自身：版本、构建信息、安装时间

**（2）网络配置**
- 网卡清单（名称、MAC、速率）、IP 地址（v4/v6）、路由概要、DNS 配置

**（3）存储配置**
- 块设备清单（型号、容量、分区）、挂载点与文件系统类型、LVM/RAID 概要（可降级）

**（4）软件与进程信息**
- 关键软件包清单（rpm：`kylin-*`、`bes*`、`goldendb*` 等按模式过滤，**不全量上千个包**，可配过滤规则）
- 进程清单快照：PID、PPID、用户、命令行（截断脱敏）、启动时间、端口监听映射（ss 等价物）——支撑 CMDB"该主机跑了什么服务"与后续的宝兰德/数据库实例自动发现

**（5）运行状态摘要**
- uptime、负载、时间同步状态（chrony/ntp）、时区

### 8.3 实现形式评估：内置 Go 采集 vs 脚本 vs Exec 插件

| 方案 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| **A. 内置 Go 采集器** | 静态编译无依赖；输出结构稳定可版本化；超时/失败隔离与采集框架复用；无注入口 | 新增采集项要发版 | **核心资产项采用**（§8.2 全部默认项） |
| **B. 外挂脚本（shell/python）** | 不改二进制即可扩展采集项；现场灵活 | 目标机无 python 保证；脚本是注入口（验签/权限/审计成本）；输出格式漂移难治理；与"单文件部署"相悖 | **不作为默认机制**；仅作为受管 Exec 插件的一种内容形态 |
| **C. Exec 插件（agent 托管子进程）** | 扩展性与管控兼得：插件=签名制品，复用制品通道（§5.5）、独立 cgroup、超时查杀、输出契约校验 | 需要插件协议与治理机制 | **扩展项采用**：二期落地，一期定接口与输出契约 |

**结论：核心采集走内置 Go（A），扩展采集走受管 Exec 插件（C），裸脚本（B）不作为独立机制存在**——所有"脚本形式"的扩展都必须包装成签名制品经插件框架运行，杜绝"配置文件里写个脚本路径就执行"的裸奔模式。

### 8.4 上送通道与接口设计（与 CMDB 通信）

不单独建设 agent↔CMDB 直连通道，**复用 Gateway 上行通道**，以消息类型区分——即"保持一个上送通道，通过不同消息类型/指令安排不同上报"：

```
Agent ──(同一 mTLS gRPC 通道)──▶ Gateway ──▶ Kafka: aiops.inventory.v1.<region>
                                                    │
                                                    ▼
                                              CMDB 消费入库
                                              （冲突检测/变更审计/标签计算）
```

消息类型（同一 Protobuf 信封体系，`InventoryReport`）：

| 消息/指令 | 方向 | 说明 |
|---|---|---|
| `RegisterRequest/Response` | agent→平台→agent | 首启注册：提交证明材料，换取 asset_id + 证书（一次性引导 token 或预置引导证书鉴权） |
| `InventoryReport(full)` | agent→CMDB | 首启全量 + 周期全量校验（默认 24h 可配） |
| `InventoryReport(delta)` | agent→CMDB | 变更触发增量（监听项：网卡/IP/挂载/关键进程集合变化） |
| `inventory.refresh` 动作 | 平台→agent | 经指令通道即时触发全量上报（走 §5.3 动作目录） |
| `InventoryAck / conflict` | CMDB→agent（经 Gateway 控制流） | 入库回执；身份冲突（如 machine-id 撞车）时要求重新注册 |

- 上报内容字段化、带 `schema_version`，**采集项与上报逻辑后续可配置修改**：配置项控制各采集段开关（如 `inventory.packages.enabled`），新增字段靠协议版本演进，agent 老版本字段缺失时 CMDB 侧容忍；
- 注册与资产上报属于**审计级可靠性**（§4.1 第三类）：先落 WAL 再发送，失败重试，不允许静默丢弃；
- CMDB 接口契约（topic、schema、冲突处理语义）单独成文《cmdb-interface.md》（二期补），agent 侧只依赖消息定义 `pkg/protocol`。

### 8.5 安全与隐私

- 进程命令行可能含敏感参数（密码、token）→ 采集时按可配规则**脱敏**（如 `--password=*` 打码）后再上送；
- 资产上报内容走与指标相同的 mTLS 加密通道；字段级差异不影响通道安全；
- 注册引导凭据（bootstrap token）一次性使用、短时效、可按批次吊销——20 万台批量部署的引导凭据治理是重点风险项，部署方案中专项设计。

---

## 9. 部署、配置与工程化

### 9.1 单文件部署

```bash
# 构建（发布流水线）
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o tom_ai_agent-linux-amd64
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o tom_ai_agent-linux-arm64

# 目标机部署（一个二进制 + 一个配置 + 一个 unit 文件）
/usr/local/bin/tom_ai_agent
/etc/tom_ai_agent/agent.yaml
/etc/systemd/system/tom_ai_agent.service
# 运行账户：tomagent（install.sh 创建）；数据目录 /var/lib/tom_ai_agent/
```

- 二进制内置默认值；**首启必需**：Gateway 区域 VIP 地址、引导凭据（注册用）；业务标签不再需要本地配置（平台侧 CMDB 补充）；
- 安装包：tar.gz（二进制 + 配置模板 + unit + install.sh），适配麒麟 V10，后续可打 RPM；
- 20 万台分发走 Ansible/批量运维平台，agent 不做 P2P 分发。

### 9.2 配置示例（agent.yaml 骨架，v0.2）

```yaml
agent:
  data_dir: /var/lib/tom_ai_agent
  log_level: info

uplink:                        # 唯一出站通道
  gateway_addrs: ["gw-vip.dc1:443"]
  tls: { ca: /etc/tom_ai_agent/ca.crt, cert: ..., key: ... }   # 注册后签发
  fallback_longpoll: false     # 旧网络兼容模式
  reconnect: { min: 1s, max: 60s, jitter: true }

register:
  bootstrap_token_file: /etc/tom_ai_agent/bootstrap.token  # 一次性，注册成功后删除
  inventory:
    full_interval: 24h
    on_change: true
    packages: { enabled: true, patterns: ["kylin-*", "bes*", "goldendb*", "gaussdb*"] }
    processes: { enabled: true, redact_patterns: ["--password=*", "--token=*"] }

collectors:
  cpu:       { enabled: true, interval: 10s, per_core: false }
  memory:    { enabled: true, interval: 10s }
  disk:      { enabled: true, interval: 10s, exclude_devices: ["loop*", "ram*"] }
  diskcap:   { enabled: true, interval: 60s, exclude_fstypes: ["tmpfs","overlay","squashfs"] }
  net:       { enabled: true, interval: 10s, exclude_ifaces: ["veth*","docker*"] }
  tcp:       { enabled: true, interval: 30s }
  series_budget: 250           # 每主机序列预算

reporter:
  metrics: { buffer_size: 10000, batch_size: 500, batch_interval: 1s, wal_mb: 100 }
  results: { wal_mb: 100, fsync: true }
  audit:   { wal_mb: 100, fsync: true }

executor:
  workers: 4
  queue_size: 64
  max_timeout: 300s
  kill_grace: 3s
  output_limit_kb: 1024
  cgroup: { memory_max: 512M, tasks_max: 256, network: deny }

security:
  platform_pubkey: /etc/tom_ai_agent/platform_pub.pem   # Ed25519 信封验签
  dedup_store: boltdb            # 持久化防重放
  actions_policy: catalog_only   # 仅动作目录，无任意 shell

watchdog:
  self_rss_soft_mb: 150
  self_fd_limit: 1024
  degraded_mode: true
```

### 9.3 源码目录规划（v0.2）

```
tom_ai_agent/
├── cmd/agent/main.go          # 入口：flag、信号、优雅启停
├── internal/
│   ├── core/                  # 模块注册、生命周期
│   ├── register/              # 注册流程 + 资产采集 + inventory 上报（v0.2 新增）
│   ├── collector/             # cpu.go mem.go disk.go net.go
│   ├── reporter/              # queues.go wal.go uplink.go（三类队列）
│   ├── uplink/                # gRPC 控制流/数据流、重连、心跳
│   ├── executor/              # engine.go worker.go cgroup.go actions.go
│   ├── security/              # mtls.go envelope.go dedup.go audit.go
│   ├── watchdog/              # health.go resource.go selfmetric.go
│   └── config/                # 加载、校验、热更新(SIGHUP)
├── pkg/protocol/              # Protobuf：metrics/commands/inventory/events（平台共享）
├── configs/agent.yaml.example
├── scripts/install.sh
├── systemd/tom_ai_agent.service
└── docs/
    ├── agent-design.md            # 本文档
    ├── agent-design-chatgpt.md    # 外部评审意见（存档）
    └── cmdb-interface.md          # CMDB 接口契约（二期补）
```

### 9.4 麒麟 V10 / 跨平台验证矩阵

| 平台 | 架构 | 优先级 | 备注 |
|---|---|---|---|
| 麒麟 V10 SP3 | x86_64 | P0 | 主力验证环境 |
| 麒麟 V10 SP3 | aarch64（鲲鹏/飞腾） | P0 | 信创主力 |
| CentOS 7.9 / openEuler | x86_64 | P1 | 兼容性兜底 |
| 海光/兆芯 | x86_64 | P1 | 走 x86 二进制，验证 /proc 字段差异 |
| 龙芯 | loong64 | P2 | Go 已支持，需实机验证 |
| 申威 | sw_64 | 暂缓 | Go 官方不支持，需评估 gccgo 或独立构建链 |

验证注意点：/proc/cpuinfo 字段架构差异、gopsutil ARM 完整性、麒麟 kysec/SELinux 对低权 agent 的拦截、**cgroup v1/v2 差异**（执行器隔离的关键依赖）、systemd transient scope 在各版本可用性。

---

## 10. 测试与质量策略（v0.2 提升量级）

"20 台 agent 压测 Kafka" 只能验证功能，验证不了 20 万规模。v0.2 引入 **Agent Simulator（模拟器）** 作为正式测试基础设施：

1. **单测**：信封验签、防重放、动作参数校验、WAL 损坏恢复、注册冲突处理；
2. **集成测试**：测试环境 Gateway + Kafka + 模拟 CMDB，跑全链路（注册→资产上报→采集上送→指令→结果回传）；
3. **故障注入**：杀 Gateway、撑满磁盘、慢动作（`sleep 1000`）验证双重查杀、setsid 逃逸进程验证 cgroup 查杀、伪造签名/重放信封、时钟回拨；
4. **模拟器规模压测**（M0 核心）：20 万长连接、每 10s 两万批次、5 万同时断线重连、Gateway 滚动升级、Kafka 中断 30 分钟后 WAL 集中重放、1 万/5 万台批量指令、恶意 agent 高频大包；
5. **长稳测试**：7×24 两周，观察 RSS/FD/goroutine 泄漏（配合资源哨兵数据）。

---

## 11. 开发里程碑（v0.2 修订）

| 里程碑 | 内容 | 目标产出 |
|---|---|---|
| **M0 协议与容量验证（新增，先于一切）** | Protobuf 协议定稿、Gateway 原型、Agent Simulator、单 Gateway 实例连接/吞吐压测、每主机指标包大小实测 | 架构定型结论（连接模型、批次大小、Gateway 容量、是否用 gRPC） |
| M1 骨架 | Core + Config + 日志 + systemd + 基础采集 + Gateway 上行 | 可跑通的"采集 agent" |
| M2 注册与资产 | Register 流程 + Inventory 采集上送 + CMDB 接口联调 | 首启注册+资产上报打通 |
| M3 可靠性 | 三类队列 + WAL 工程化 + 自监控 + 三层自愈 | 可长稳运行 |
| M4 指令执行 | 动作目录 + 信封验签 + 双重查杀 + 取消 | 只读诊断执行能力 |
| M5 安全加固 | 低权运行 + Helper + cgroup 限额 + 审计中心化 + A/B 升级 | 灰度上线安全标准 |
| M6 信创验证 | 麒麟 V10 x86/ARM 实测 + cgroup v1/v2 + 性能调优 | 百台级灰度 |
| M7 扩展 | Exec 插件框架、BES/DB 专项采集（二期启动） | 对接数据底座 |

**工作量修正（评审意见采纳）**：v0.1 的"M1–M4 约 4–6k 行、1–2 人"只能做演示版。生产级需把 Gateway、模拟器、协议、PKI、WAL、cgroup、升级回滚一并计入，正式估算在 M0 结论后重排。

---

## 12. 开放问题（2026-07-19 讨论结论已回填，残余问题见 platform-architecture.md §10）

1. ~~指令通道选型~~ → 已定案 Gateway 长连接；~~Gateway 自研与否~~ → **已定案：NGINX stream(L4 接入) + 自研 Go Connector**（详见 platform-architecture.md §3.1）。
2. ~~TOTP 粒度~~ → 已定案平台侧 MFA + 签名信封；**已确认：部门现有平台无 MFA 能力 → 管控平台内置轻量 IAM/TOTP/审批模块**（platform-architecture.md §4.4），后续对接统一 IAM 时降级为适配层。
3. **执行权限**：低权主进程 + Privileged Helper 的拆分粒度——哪些操作必须经 Helper（dmidecode、cgroup 管理、……）？清单需在 M5 前冻结。
4. **动作目录覆盖度**：一期只读动作清单（§5.3）是否够用？`script.run` 一期默认关闭是否接受（现场排障诉求 vs 安全）？
5. **数据模型** → 已定案 Protobuf。**待 M0 实测**：每主机 10s 批次压缩后目标 ≤2KB，若超标需砍指标或降频。
6. ~~注册引导凭据治理~~ → **已定案：分批签发、分批部署**——每批次独立 token + 使用次数上限 + 有效期 + 绑定目标 Cell/网段，泄露可整批吊销（表结构见 platform-architecture.md `register_bootstrap`）。
7. ~~资产采集范围~~ → **已确认：进程信息先具备采集能力，上送开关默认关闭（`processes.upload_enabled=false`），存储模型后续专题讨论**（platform-architecture.md §5.3 预留草案、标注缓建）；进程命令行按可配规则脱敏。
8. **申威平台**：是否有存量 sw_64 服务器？决定是否投入特殊构建链。

---

## 13. 外部评审意见评估结论（v0.2 采纳记录）

对 `agent-design-chatgpt.md` 的逐条评估：

| # | 评审意见 | 评估 | 处理 |
|---|---|---|---|
| 1 | Kafka 不适合做 20 万台指令邮箱（三种消费模型均不可行） | **成立**，consumer group 语义与主机寻址矛盾，论据正确 | 采纳：改 Gateway 长连接（§2.1/§5.1） |
| 2 | Agent 不暴露端口、只出站连区域 Gateway | **成立**，安全暴露面与凭据治理均最优 | 采纳（§2.2/§5.1） |
| 3 | 区域 Cell 划分（1–3 万台/Cell，Gateway ≥3 实例） | **成立**，爆炸半径控制必要 | 采纳，归平台侧设计（§2.2），agent 只需感知区域 VIP |
| 4 | 指标量级被低估、一期即上 Protobuf 批量包 | **成立**，带宽测算与协议迁移成本论据充分 | 采纳（§3.3） |
| 5 | 降低默认采集基数（每核 CPU 默认关、过滤虚拟设备、序列预算） | **成立**，5000 万活跃序列需治理 | 采纳（§3.1） |
| 6 | 拓扑标签平台侧补充，machine-id 不作最终 ID | **成立**，克隆漂移与伪造身份风险真实存在 | 采纳（§3.3/§8.1），与新增 CMDB 模块正好衔接 |
| 7 | 删除 agent 侧 TOTP，改平台 MFA + 签名授权信封 | **成立**，种子分发到 20 万台是严重风险；用户双因素诉求在平台层等价实现 | 采纳（§5.2） |
| 8 | 信封绑定 host_id + payload_sha256，去重持久化 | **成立**，v0.1 内存去重有重启重放漏洞 | 采纳（§5.2） |
| 9 | 默认非 root + Privileged Helper | **成立**，最小权限原则 | 采纳（§6.3） |
| 10 | 动作目录替代字符串通配符白名单 | **成立**，通配符解析绕过是已知攻击面 | 采纳（§5.3） |
| 11 | 进程组查杀不够，需 cgroup scope 隔离 | **成立**，setsid/double-fork 逃逸路径真实 | 采纳（§5.4） |
| 12 | 脚本走制品服务（artifact_id+sha256+签名），不内嵌消息 | **成立**，审计/复用/撤销优势明显 | 采纳（§5.5） |
| 13 | 三类数据三种可靠性语义，WAL 工程化 | **成立**，v0.1 "至少一次" 宣称不严格 | 采纳（§4.1/§4.2） |
| 14 | 删除 OOMScoreAdjust=-900，改 systemd 内存硬限额 | **成立**，与 "agent 不能影响业务" 目标矛盾 | 采纳（§6.3/§7.1） |
| 15 | 审计实时中心化 + 哈希链，清理优先级明确 | **成立**，本地 0600 防不住 root 攻击者 | 采纳（§6.4） |
| 16 | 离线检测靠 Gateway 会话而非时序库 | **成立**，agent 离线即无指标可查 | 采纳（§7.2） |
| 17 | 跳板机限定为人机入口 + 应急通道 | **成立** | 采纳（§6.6） |
| 18 | 新增 M0 协议与容量验证、Agent Simulator | **成立**，规模假设必须先验证 | 采纳（§10/§11） |
| 19 | 控制面与数据面 Kafka 隔离、区域化 Topic | **成立**，属平台侧设计 | 记录，转总体方案数据底座章节联动 |
| 20 | 工作量低估（4–6k 行不含 Gateway/模拟器/PKI） | **成立** | 采纳，M0 后重排估算（§11） |

**未采纳/保留意见**：无原则性反对项。两点需 M0 实测后再定：① gRPC 依赖的静态编译体积与内存开销（不达标则降级自研协议，§2.4）；② 每主机 10s 批次 ≤2KB 的指标预算可行性（§3.3）。

---

*本文档为讨论稿 v0.2。下一轮讨论建议聚焦：§12 开放问题 1/2/6/7（Gateway 选型、IAM 现状、注册凭据治理、资产采集范围）。*
