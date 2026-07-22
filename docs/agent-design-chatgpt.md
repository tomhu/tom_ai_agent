总体判断
====

这份设计稿的模块拆分、单文件部署、WAL、进程超时控制、自监控、跨架构适配等方向基本正确；但以 **20 万台乃至几十万台服务器**为目标时，当前最大的风险不在 Agent 内部代码，而在于：

1. **连接拓扑不合理**；

2. **Kafka 被同时当成指标总线和主机点对点命令总线**；

3. **Agent 侧安全责任过重**；

4. **指标规模、序列基数和网络带宽明显低估**；

5. **仅靠进程组无法真正约束恶意或失控脚本**。

设计稿中提出的“Agent 直接消费 Kafka 指令”“`aiops.cmd.{host_id}`”“Agent 校验 TOTP”“一期默认 root”“20 台 Agent 做 Kafka 压测”等内容，在 20 万台规模下都需要调整。
核心结论
----

**不建议 Agent 对外暴露 HTTP 命令端口，也不建议几十万 Agent 直接消费 Kafka 指令。**

推荐采用：

> **Agent 主动出站连接区域 Agent Gateway；Gateway 负责连接管理、命令路由和流量汇聚；Kafka 只部署在平台内部，用于指标、事件、审计和结果流转。**

也就是：

* 指标：`Agent → 区域 Gateway → Kafka → 时序存储`

* 指令：`运维平台 → Command Service → Gateway → Agent`

* 结果：`Agent → Gateway → Kafka/结果存储`

* 跳板机：只作为人员登录、审批入口和应急 SSH 通道，不直接批量调用 Agent

* Agent：默认不开放任何外部监听端口，仅允许出站访问少量区域 VIP

* * *

一、三种方案的专业比较
===========

| 维度      | Agent 暴露 HTTPS，跳板机主动调用 | Agent 直接连接 Kafka            | Agent 出站连接 Gateway，Gateway 接 Kafka |
| ------- | ---------------------- | --------------------------- | ---------------------------------- |
| 主机侧网络   | 每台服务器开放入站端口            | 只需出站，但所有主机都要访问 Kafka Broker | 只需出站访问少量区域 VIP                     |
| 指令实时性   | 在线时较好                  | 可异步，但点对点寻址困难                | 长连接推送，实时性好                         |
| 离线任务    | 跳板机需自行持久化和重试           | Kafka 可持久化，但消费模型不适合单主机邮箱    | Command Service 持久化，Agent 上线后继续投递  |
| 大规模连接压力 | 跳板机承担瞬时 HTTP 扇出        | Kafka Broker 直接承担几十万客户端连接   | Gateway 消化长连接，Kafka 只面对少量 Gateway  |
| 安全暴露面   | 最大，每台主机都有监听端口          | Kafka 凭据、CA、ACL 下发到每台主机     | Broker 不对生产服务器暴露，安全边界最清晰           |
| 主机凭据风险  | 每台主机保存服务端私钥            | 每台主机保存 Kafka 生产和消费凭据        | 每台主机只保存自身设备证书                      |
| 批量操作    | 容易出现同步调用风暴             | 可异步，但主机定向困难                 | Gateway/调度器按批次、区域和并发度下发            |
| 运维成本    | 防火墙、ACL、端口、扫描治理成本很高    | Kafka、证书、ACL、配额和客户端治理成本高    | 增加 Gateway，但总运维成本最低                |
| 适用结论    | 不适合作为主通道               | 适合内部数据流，不适合作为 Agent 命令邮箱    | **推荐方案**                           |

如果只能在原始两个方案中二选一：

* **指标上报选 Kafka，但仅 Producer，不让 Agent 消费命令；**

* **指令通道选 Agent 主动出站的长轮询或长连接，而不是 Agent 暴露 HTTP Server。**

* * *

二、为什么 Kafka 不适合直接作为 20 万台 Agent 的指令邮箱
=====================================

Kafka 很适合高吞吐、批量、异步的数据流。Producer 原生具备异步发送、批量聚合和压缩能力，因此它非常适合作为 Gateway 后面的指标、审计和结果总线。([kafka.apache.org](https://kafka.apache.org/37/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html?utm_source=chatgpt.com "KafkaProducer (kafka 3.7.2 API)"))

但 Kafka 的 Consumer Group 语义与“给指定主机下发命令”并不匹配：

* 同一个 Consumer Group 内，一个分区只会分配给一个 Consumer；

* 不同 Consumer Group 会分别消费该 Topic 中的全部消息；

* Consumer 加入、退出或者分区变化会触发重新分配。([kafka.apache.org](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html?utm_source=chatgpt.com "KafkaConsumer (kafka 4.3.1 API)"))

因此当前设计会遇到三个难题。

### 1. 每台主机一个 Topic

例如：
    aiops.cmd.host-000001
    aiops.cmd.host-000002
    ...

20 万台主机就对应约 20 万个 Topic，至少 20 万个分区；如果副本数为 3，则是约 60 万个分区副本。即使技术上可以继续扩容，这种模型的元数据、控制器、ACL、监控和运维复杂度也非常高。

**不应使用每主机一个 Topic。**

### 2. 所有 Agent 使用同一个 Consumer Group

这种情况下 Kafka 会把分区分摊给不同 Agent。一个目标为主机 A 的命令，很可能被分配给主机 B 所在的 Consumer，不能保证主机寻址。

### 3. 每个 Agent 使用独立 Consumer Group

这样每个 Agent 都会看到该 Topic 的全部命令，然后在本地过滤 `host_id`。20 万个 Consumer Group 会产生极大的读取放大：一条命令可能被读取近 20 万次。

因此，Kafka 可以承担：

* 命令事件的持久化；

* 命令生命周期事件；

* 审计事件；

* 执行结果；

* 区域 Dispatcher 的输入。

但 Kafka **不应该直接承担“Agent 在线会话和精确主机寻址”**。

* * *

三、推荐的几十万节点总体架构
==============

                        ┌──────────────────────────────┐
                        │ 运维门户 / 跳板机 / API       │
                        └──────────────┬───────────────┘
                                       │ 用户认证、MFA、审批
                                       ▼
                        ┌──────────────────────────────┐
                        │ Command Service              │
                        │ 命令状态机 / 持久化 / Outbox │
                        └──────────────┬───────────────┘
                                       │ 查询 host 当前会话
                                       ▼
                        ┌──────────────────────────────┐
                        │ Session Router               │
                        │ host_id → cell/gateway       │
                        └──────────────┬───────────────┘
                                       │ 内部 gRPC
               ┌───────────────────────┴────────────────────────┐
               │                区域 / IDC Cell                  │
               │                                                │
    Agent ─────┼─mTLS + HTTP/2──▶ Agent Gateway 集群            │
    主动出站   │   控制长连接        │                           │
               │   指标批量上传       ├────▶ Kafka Metrics        │
               │   结果/审计回传      ├────▶ Kafka Audit/Result   │
               │                      └────▶ 对象存储/制品服务     │
               └────────────────────────────────────────────────┘

Agent 与 Gateway 之间建议使用的连接
-------------------------

建议使用 **mTLS + HTTP/2/gRPC**：

* 一条长期存在的控制流，用于心跳、指令、取消、ACK；

* 一条指标上传流或批量 Unary RPC；

* 文件和脚本不要塞进控制消息，改用制品 ID 和对象存储下载；

* Agent 始终主动发起连接，不开放外部监听端口。

gRPC 官方建议复用 Channel，并在长期逻辑流中使用 Streaming RPC；同时需要注意长期 Stream 建立后不会重新参与负载均衡，因此 Gateway 必须实现连接排空、GOAWAY、带抖动重连和最大连接生命周期。([gRPC](https://grpc.io/docs/guides/performance/?utm_source=chatgpt.com "Performance Best Practices | gRPC"))

企业网络中可以统一走 TCP 443。若部分旧网络设备不能稳定支持 HTTP/2，再提供 HTTPS 长轮询作为兼容模式，而不是开放 Agent 入站端口。
为什么需要区域 Cell
------------

不要建设一个覆盖 20 万节点的单一连接集群。建议按 IDC、地域或网络域划分 Cell：

* 每个 Cell 管理约 1 万到 3 万台主机；

* 每个 Cell 部署不少于 3 个 Gateway 实例；

* Kafka、Gateway 或网络故障只影响当前 Cell；

* Agent 只连接本区域 VIP；

* 中心平台保留统一控制面。

初始容量规划可以保守按每个 Gateway 实例承载 5,000 条在线连接：

* 20 万台约需 40 个活动实例；

* 加 30% 容量余量约为 52 个实例。

若压测证实单实例稳定承载 10,000 条连接，则约为 20 个活动实例，加余量约 26 个。这里的瓶颈通常不是空闲 TCP 连接数，而是消息速率、TLS、内存缓冲、重连风暴和批量编码，必须通过模拟器压测确定。

Gateway 增加了几十台轻量服务器，但会显著减少：

* Kafka Broker 的直接连接数；

* 每主机 Kafka 凭据管理；

* Kafka ACL 数量；

* Broker TLS 握手和重连风暴；

* 主机侧防火墙规则；

* 生产网到 Kafka 网的网络暴露。

从总体拥有成本看，通常比“20 万 Kafka 客户端”或者“20 万 HTTP Server”更低。

* * *

四、指标量级目前被明显低估
=============

当前设计按单条指标 JSON 上送，每条都重复：

* host；

* arch；

* os；

* idc；

* cluster；

* agent_version；

* metric name；

* timestamp。

在 20 万台规模下，这会造成非常高的编码、网络和存储放大。

以每台主机每 10 秒上传一个压缩后的指标批次为例：

| 每主机每 10 秒压缩数据量 | 平台入口带宽   | 每天入口数据     |
| -------------- | -------- | ---------- |
| 2 KB           | 40 MB/s  | 3.46 TB/天  |
| 5 KB           | 100 MB/s | 8.64 TB/天  |
| 10 KB          | 200 MB/s | 17.28 TB/天 |

如果 Kafka 副本数为 3，还要考虑约 3 倍的 Broker 写入和副本网络量，不包括时序数据库写入、索引和保留空间。

因此设计稿中“数十 MB/s”的估计，只有在**每台主机每 10 秒的压缩批次控制在约 2 KB 左右**时才成立。
建议从第一版直接使用 Protobuf
-------------------

不建议“一期 JSON、二期 Protobuf”。协议一旦被几十万 Agent 部署，后续迁移成本非常高。

推荐类似：
    message HostMetricBatch {
      string host_id = 1;
      int64 timestamp_ms = 2;
      uint64 sequence = 3;
      uint32 schema_version = 4;
      repeated MetricSample samples = 5;
    }

    message MetricSample {
      uint32 metric_id = 1;
      double value = 2;
      repeated LabelRef labels = 3;
    }

关键点：

* `host_id`、时间戳和公共标签在一个批次中只出现一次；

* 常见指标名映射成 `metric_id`；

* 设备、网卡、挂载点等变化标签使用字典引用；

* JSON 仅用于日志调试和人工排查；

* Agent 协议必须具备版本协商能力。

OTLP 已经定义了基于 Protobuf 的 gRPC 和 HTTP 传输、压缩、部分成功和指数退避机制，可以直接采用 OTLP，或者至少借鉴其协议设计。([OpenTelemetry](https://opentelemetry.io/docs/specs/otlp/index.md?utm_source=chatgpt.com "https://opentelemetry.io/docs/specs/otlp/index.md"))
降低默认采集基数
--------

建议调整默认采集策略：

| 指标     | 当前设计  | 建议                             |
| ------ | ----- | ------------------------------ |
| CPU 总量 | 10 秒  | 保留                             |
| 每核 CPU | 10 秒  | 默认关闭或调整到 60 秒，按需动态开启           |
| 内存     | 10 秒  | 保留核心字段，减少重复派生指标                |
| 每设备 IO | 10 秒  | 保留活跃设备，过滤 loop、ram、无效设备        |
| 每分区容量  | 60 秒  | 保留，但过滤伪文件系统和容器临时挂载             |
| 每网卡    | 10 秒  | 过滤 veth、docker、短生命周期虚拟接口，或单独分层 |
| TCP 状态 | 30 秒  | 保留总量，详细连接数据按需开启                |
| 元信息    | 300 秒 | 仅变化时上报，另加周期性校验                 |

假设平均每台主机有 250 条时间序列，20 万台就是约 5,000 万条活跃序列。必须在设计阶段建立明确的：

* 每主机序列预算；

* 每类设备序列预算；

* 动态标签白名单；

* 单主机最大 label 数；

* 总平台 cardinality 上限。

拓扑标签应在平台侧补充
-----------

`idc`、`cluster`、`业务系统`、`负责人`等标签建议由 CMDB 在 Gateway 或消费侧补充，而不是主要依赖 Agent 本地配置。

Agent 只应稳定上报：

* 平台签发的 `asset_id`；

* Agent 版本；

* boot_id；

* 操作系统和硬件事实；

* 本地采集维度。

这样可以避免：

* 配置漂移；

* 克隆主机携带错误标签；

* Agent 被入侵后伪造租户或集群身份；

* 业务标签变化导致重新部署 Agent。

`/etc/machine-id` 可能因为镜像克隆发生重复，不应直接作为最终 Agent ID。应由注册服务签发不可变的 `asset_id`。

* * *

五、安全模型需要重点重构
============

1. 不要让 Agent 校验用户 TOTP

----------------------

TOTP 属于“用户身份认证因素”，应在统一 IAM、运维门户或审批平台完成。

不应：

* 给每台 Agent 分发操作人的 TOTP 种子；

* 给每台主机分发主机级 TOTP 种子；

* 在命令消息中把用户输入的 TOTP 再传到 Agent；

* 让 Agent 自己判断用户是否完成 MFA。

这会带来严重的密钥分发和泄露问题。

正确模型是：
    用户 → IAM 完成 MFA
           ↓
    审批服务完成风险校验/双人审批
           ↓
    命令服务生成签名授权信封
           ↓
    Agent 只验证平台签名、目标主机、命令摘要和有效期

命令签名信封至少应绑定：

* `cmd_id`

* `host_id`

* `action_id`

* `payload_sha256`

* `issued_at`

* `not_before`

* `expires_at`

* `nonce`

* `operator_id`

* `approval_chain_hash`

* `mfa_level`

* `risk_level`

* `policy_version`

可以继续使用短期 JWT/JWS，但必须把 `host_id` 和 `payload_sha256` 放入签名声明，不能只签一个可替换 payload 的 Token。更稳妥的是对规范化 Protobuf 命令信封做 Ed25519 签名。

Agent 的去重记录必须持久化。只放在内存缓存中，Agent 重启后可能再次执行重放命令。

2. Agent 默认不能以 root 执行命令

------------------------

文档中“一期默认 root、后续再实现 run_as”风险过高。

建议从第一版采用：

* Agent 主进程运行在专用低权限用户下；

* `/proc`、`/sys` 中绝大多数基础指标并不要求 root；

* 极少数需要特权的操作通过一个很小的 Privileged Helper 完成；

* Helper 只接受固定操作码，不接受任意 Shell 字符串；

* systemd 使用 `CapabilityBoundingSet` 将能力收缩到最小；

* 启用 `NoNewPrivileges=true`、`ProtectSystem=strict`、`ProtectHome=true`、`PrivateTmp=true` 等隔离项。
3. 不建议以字符串通配符实现命令白名单

--------------------

例如：
    - "df *"
    - "cat /proc/*"
    - "systemctl status *"

这种策略容易出现参数绕过、路径穿越、敏感信息读取和解析差异。

更推荐“动作目录”：
    diagnose.disk_usage
    diagnose.memory_summary
    diagnose.service_status
    diagnose.network_connections
    diagnose.read_log_tail

平台下发的是结构化参数：
    {
      "action": "diagnose.service_status",
      "params": {
        "service": "nginx"
      }
    }

Agent 内部映射为固定绝对路径和固定参数：
    /usr/bin/systemctl status --no-pager nginx.service

并对 `service` 使用明确的值域或正则，而不是把原始字符串传给 shell。

4. 进程组查杀不够

----------

`Setpgid` 和负 PGID 查杀可以处理普通子进程树，但恶意或复杂脚本可以通过：

* `setsid()`；

* 重新设置进程组；

* daemonize；

* double-fork；

逃离原进程组。

建议每个执行任务进入独立的 systemd transient scope 或 cgroup：

* `MemoryMax`

* `CPUQuota`

* `TasksMax`

* `RuntimeMaxSec`

* `KillMode=control-group`

* 网络默认关闭或按动作授权

* 单独的文件系统访问范围

任务超时后直接杀整个 cgroup。麒麟 V10 不同版本可能使用 cgroup v1 或 v2，需要分别验证，但不能仅依赖进程组。

5. 脚本不要直接放进 Kafka 消息

--------------------

脚本执行建议采用：
    命令消息：
    artifact_id + sha256 + signature + entrypoint + parameters

Agent 再从区域制品服务下载不可变制品：

1. 校验平台签名；

2. 校验 SHA256；

3. 解压到临时隔离目录；

4. 降权运行；

5. 进入独立 cgroup；

6. 到期清理。

这比在命令消息中直接携带脚本文本更容易审计、复用、限流和撤销。

* * *

六、可靠性设计需要修正的地方
==============

1. 当前不能严格宣称“至少一次”

-----------------

设计稿中的流程是：
    内存队列 → 发送 Kafka
                失败后才写 WAL

如果 Agent 在“采集完成但尚未落盘、也尚未 Kafka ACK”时崩溃，数据会丢失。因此这只是“有 WAL 兜底的尽力交付”，不是严格的至少一次。

可以明确区分：

* 普通主机指标：允许有界丢失，优先保护业务主机；

* 安全事件、命令 ACK、执行结果、审计：必须先持久化再确认，提供至少一次；

* 平台按 `cmd_id/event_id` 去重。
2. 指标、审计和命令结果不能共用一个丢弃策略

-----------------------

建议拆成三类队列：

| 队列                   | 是否允许丢弃  | 策略                            |
| -------------------- | ------- | ----------------------------- |
| Metrics              | 允许有界丢弃  | 满后优先保留最新数据                    |
| Command Result       | 不允许静默丢弃 | 独立 WAL，按 cmd_id 重试            |
| Audit/Security Event | 不允许静默丢弃 | 独立 WAL，持久化失败时高危命令 fail-closed |

重放时要进行流量配比，例如优先实时数据，同时给历史 WAL 保留固定带宽，避免 MQ 恢复后历史数据挤压实时流量。

WAL 还需要：

* 分段；

* 长度前缀；

* CRC；

* 损坏段隔离；

* 原子索引；

* fsync 策略；

* 重放游标；

* 最大年龄；

* 磁盘配额。
3. 审计清理优先级要明确

-------------

文档中“审计 > WAL > 运行日志优先级滚动清理”容易产生歧义。建议明确为：

* **保留优先级：审计最高，命令结果其次，指标 WAL 再次，运行日志最低；**

* **清理顺序：运行日志 → 指标 WAL → 非关键诊断输出；**

* 审计不能因为指标积压被删除。

本地 0600 文件只能防止普通用户读取，无法抵御已获得 root 的攻击者。审计应实时送往中心不可变存储，并增加记录哈希链或批次签名。

4. 不建议设置 `OOMScoreAdjust=-900`

------------------------------

Agent 的定位是“不能影响业务”。给 Agent 设置接近最高等级的 OOM 保护，可能导致内存紧张时先杀业务进程而保留 Agent，与设计目标相反。

更合理的是：

* `MemoryHigh=100M`

* `MemoryMax=200M`

* `TasksMax`

* 合理的 `CPUQuota`

* OOMScoreAdjust 保持默认或略提高

* Agent 超限时由 systemd 重启
5. Agent 离线检测不能只依赖自身指标

----------------------

Agent 离线后不会再上报 `agent.uptime`，所以平台必须从 Gateway 维护：

* 预期资产集合；

* 当前在线会话；

* 最近心跳时间；

* 连接断开原因；

* boot_id；

* Agent 版本；

* 证书状态。

离线判定应是：
    CMDB 中应存在的 Agent - Gateway 当前活跃会话

而不是仅查询时序数据库是否还有 Agent 指标。

* * *

七、Kafka 在推荐架构中的正确使用方式
=====================

Topic 规划
--------

不使用每主机 Topic，而使用区域化、固定数量的 Topic：
    aiops.metrics.host.v1.<region>
    aiops.events.agent.v1.<region>
    aiops.audit.agent.v1.<region>
    aiops.command.result.v1.<region>

分区键使用稳定的 `host_id`，保证同一主机数据在单个 Topic 内有序。

分区数不要凭经验直接写死，应根据以下三个数值计算：
    分区数 >= 最大入口吞吐 / 单分区实测吞吐
    分区数 >= 下游所需最大消费并发
    分区数 >= 故障恢复和热点隔离要求

可以在 PoC 阶段从每区域 256 或 512 个分区开始压测，但最终必须由实测决定。Kafka 官方也提醒，按 `hash(key) % partition_count` 分区时，动态增加分区会改变 Key 到分区的映射，因此扩容前应预留容量，或者通过版本化 Topic 迁移。([kafka.apache.org](https://kafka.apache.org/43/operations/basic-kafka-operations/?utm_source=chatgpt.com "Basic Kafka Operations | Apache Kafka"))
数据面与控制面建议隔离
-----------

至少应做到：

* 指标 Topic 与审计、结果 Topic 使用不同配额；

* Gateway 内部使用不同 Producer；

* 控制消息优先级高于指标；

* 指标洪峰不能阻塞命令 ACK 和安全事件。

更稳妥的生产方案是：

* Telemetry Kafka 集群：高吞吐，允许短暂积压；

* Control/Audit Kafka 集群：低吞吐、高可靠、严格权限。

Kafka 支持 SSL/SASL、加密和 ACL，但 TLS 本身会增加计算开销；将几十万端点凭据收敛为几十个或几百个 Gateway 身份，会明显简化权限和证书治理。([kafka.apache.org](https://kafka.apache.org/43/security/security-overview/?utm_source=chatgpt.com "Security Overview | Apache Kafka"))

* * *

八、跳板机应承担什么角色
============

跳板机集群建议保留，但角色应限制为：

1. 人员登录入口；

2. 运维门户或 Command API 的访问入口；

3. 应急 SSH/Ansible 通道；

4. Agent 平台整体不可用时的 break-glass 手段。

不建议：

* 跳板机直接调用每台 Agent HTTP Server；

* 跳板机绕过 Command Service 下发命令；

* 跳板机保存 Agent 全网通用客户端证书；

* 将跳板机当成 20 万节点的同步 RPC 调度器。

这样即使 Agent Gateway、Kafka 或控制平台出现大面积故障，仍可以通过原有 SSH/Ansible 体系进行应急处置，而不需要给 Agent 再增加一个高风险“备用 HTTP 后门”。

* * *

九、对当前研发计划的具体修改建议
================

P0：编码前必须修改
----------

1. 将 §5.1 改为“Agent 出站连接 Gateway”，删除 Agent 直接消费 Kafka 的主路径。

2. 删除 `aiops.cmd.{host_id}` 每主机 Topic 方案。

3. 删除 Agent 侧 TOTP 种子和 TOTP 校验，改为中心 MFA + 签名授权信封。

4. `run_as`、低权限主进程、特权 Helper 和 cgroup 隔离进入一期。

5. Protobuf 进入一期，不再作为二期优化。

6. 明确 Metrics、Result、Audit 三类不同可靠性语义。

7. 删除 `OOMScoreAdjust=-900`。

8. Agent 默认不对外监听端口，只保留 Unix Domain Socket 或 `127.0.0.1` 本地健康接口。

P1：百台灰度前完成
----------

1. 自动化设备注册和证书轮换；

2. 区域 Gateway 与 Session Router；

3. 持久化命令状态机；

4. Agent 本地持久化去重；

5. A/B 双槽升级和原子回滚；

6. CMDB 服务端标签补充；

7. 审计中心化与防篡改；

8. 批量指令分批、限速、暂停和熔断。

P2：规模上线前完成
----------

1. 多 Cell 灾难隔离；

2. Gateway 连接排空和无损升级；

3. 全区域重连风暴测试；

4. CA、平台签名密钥轮换；

5. 指标基数治理；

6. 异构平台和 cgroup v1/v2 验证；

7. 插件和脚本沙箱。

* * *

十、测试策略也需要提升一个数量级
================

“20 台 Agent 对 Kafka 压测”只能验证功能，不能验证 20 万规模。

需要开发专门的 Agent Simulator，至少模拟：

* 20 万条长期连接；

* 每 10 秒 20,000 个主机批次；

* 20 万证书或等价身份；

* 5 万 Agent 同时断线重连；

* Gateway 滚动升级；

* 整个 Cell Gateway 重启；

* Kafka 中断 30 分钟后恢复；

* WAL 集中重放；

* 证书批量到期和轮换；

* 1 万台、5 万台批量命令；

* 单个恶意 Agent 高频发送、超大包、非法签名；

* 时钟回拨和严重漂移；

* CMDB 与实际主机身份冲突。

建议新增一个 **M0：协议与容量验证**，排在现有 M1 之前。先验证连接模型、指标大小、序列数和 Gateway 容量，再开始大规模实现。

另外，“M1–M4 约 4–6k 行 Go、1–2 人”可以做出演示版本，但很难覆盖生产级 PKI、WAL、协议兼容、cgroup、安全策略、升级回滚、Gateway 和大规模测试。正式生产投入应把 Gateway、模拟器、测试工具、协议代码和安全代码一并纳入工作量。

* * *

最终推荐方案
======

对于几十万服务器规模，建议正式确定以下架构原则：

> **Agent 无入站端口、只主动出站；Gateway 管理在线连接和主机寻址；Kafka 负责平台内部的高吞吐数据流，不直接面对几十万 Agent 的命令消费；跳板机只做人机入口和应急通道。**

具体选择是：

* **指标通道**：Agent → Gateway → Kafka；

* **指令通道**：Command Service → Session Router → Gateway → Agent 长连接；

* **结果与审计**：Agent → Gateway → 独立可靠队列/Kafka；

* **数据协议**：第一版即采用 Protobuf 批量包；

* **身份认证**：每主机独立 mTLS 证书；

* **用户 MFA**：只在平台完成；

* **命令授权**：Agent 校验绑定主机和 payload 摘要的平台签名；

* **命令执行**：低权限、动作目录、独立 cgroup，而不是默认 root 任意 shell；

* **应急兜底**：继续使用受控的 SSH/Ansible 跳板机体系，不给 Agent 开备用公网或管理网 HTTP 命令端口。
