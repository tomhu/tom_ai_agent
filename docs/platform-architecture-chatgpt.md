总体评估结论
======

修改后的 Agent 设计已经从“单机程序设计”提升为面向几十万节点的完整 Agent 体系设计：无入站端口、区域 Gateway、Protobuf 批量协议、分级可靠性、平台侧 MFA、签名授权信封、低权限运行、cgroup 隔离、注册与资产采集等关键方向都已修正。

管控平台设计稿的总体方向也正确，特别是三层架构、Cell 隔离、NGINX + Go Connector、Command Service、Session Router、注册服务、CMDB、制品服务、审计中心和升级管理等组件划分，已经具备进入 M0 原型验证的基础。

但目前还**不建议直接冻结 Proto、数据库表结构并进入生产编码**。主要存在六类 P0 问题：

1. NGINX TLS 终止与 Agent 身份传递方案不可直接按文档实现；

2. gRPC 接口方向和 ACK 模型存在明显错误；

3. Command Service 缺少事务 Outbox、投递尝试和短期信封重新签发机制；

4. Session Router 缺少会话世代号和重复连接栅栏；

5. 结果、审计、资产消息的“平台已确认”边界不明确；

6. 部分数据库 DDL 会直接执行失败，或在规模上产生较高成本。

我的判断是：

> **平台总体架构约有 75% 已经正确，可以进入 M0；但必须先修正以下 P0 项，再将文档升级为 v0.2。**

* * *

一、必须优先修改的 P0 问题
===============

| 问题                            | 当前风险                                                         | 建议                                         |
| ----------------------------- | ------------------------------------------------------------ | ------------------------------------------ |
| NGINX `stream` 终止 TLS 后传递证书身份 | 文档写“PROXY protocol + 自定义头”，但 L4 `stream` 层不存在 HTTP/gRPC 自定义头 | 默认改成 TLS 透传，由 Connector 终止 mTLS            |
| `ControlStream` 方向写反          | 请求流和响应流与消息注释矛盾，代码生成后接口语义错误                                   | 改为 `stream UplinkMsg → stream DownlinkMsg` |
| `MetricsStream` 仅返回一次 ACK     | 长期不关闭的客户端流无法逐批确认，Agent WAL 无法安全推进                            | 改为双向流，按连续序号返回 ACK                          |
| Command 无事务 Outbox            | 可能出现“数据库已审批但未投递”或“已经投递但数据库状态未更新”                             | 状态迁移与 Outbox 写入同一数据库事务                     |
| 审批后立即生成短期信封                   | Agent 离线数小时后，信封早已过期                                          | 审批时冻结命令，实际投递时再签发分钟级信封                      |
| 无 `session_epoch`             | 网络抖动时新旧两个 Connector 都可能认为自己拥有同一 Agent 会话                     | Redis CAS 注册会话，命令绑定 session_id 和 epoch     |
| 关键消息 ACK 边界不清                 | Connector 本地落盘后 ACK，但 Connector 随后损坏，Agent 已删除 WAL，数据永久丢失    | 结果、审计、资产仅在 Kafka 持久化成功后返回最终 ACK            |
| 自研 IAM 过于简单                   | 单角色、TOTP 挑战可复用、缺少资源范围和权限隔离                                   | 优先使用成熟 OIDC 身份服务；至少实现 RBAC+资源范围            |
| PostgreSQL 13+                | PostgreSQL 13 已停止官方支持                                        | 基线改为当前受支持版本，建议 16/17                       |
| Inventory 分区表主键               | `report_id PRIMARY KEY` 未包含分区键 `received_at`，DDL 会失败         | 主键改为 `(received_at, report_id)`，或不设全局主键    |

* * *

二、Gateway 接入层需要重新定型
===================

2.1 当前 TLS 方案的问题
----------------

文档目前建议：
    Agent
      → NGINX stream 终止 mTLS
      → 提取客户端证书指纹/CN
      → 通过 PROXY protocol + 自定义头传递给 Connector

这段设计不能直接落地。

NGINX `stream` 模块处理的是 TCP/TLS 会话，不理解 HTTP/2 和 gRPC metadata；`grpc_set_header` 属于 NGINX 的 HTTP gRPC 代理模块，而不是 `stream` 模块。换句话说，`stream` 可以代理 TCP，也可以终止 TLS，但不能按当前文档描述向后端插入一个普通 gRPC 请求头。 ([Nginx](https://nginx.org/en/docs/stream/stream_processing.html?utm_source=chatgpt.com "How nginx processes a TCP/UDP session"))
2.2 推荐的默认模式：TLS 透传
------------------

建议改成：
    Agent
       │ mTLS + HTTP/2
       ▼
    NGINX stream / 四层 VIP
       │ TLS 原样透传
       │ 可附加 PROXY protocol v2，仅传源 IP
       ▼
    Go Connector
       │ 终止 mTLS
       │ 从证书 SAN 提取 asset_id
       │ 校验证书序列号、指纹、吊销状态
       ▼
    Agent Gateway 业务逻辑

优点：

* Agent 身份直接由 Connector 从真实客户端证书中获取；

* 不依赖未经密码学绑定的自定义头；

* 不需要在 NGINX 和 Connector 间重建一套身份传递协议；

* NGINX 被攻破后仍不能伪造平台签发的 Agent 证书；

* 同一条 HTTP/2 连接上的控制流、指标流都会到达同一个 Connector；

* NGINX 只承担四层连接接入、负载均衡、源 IP 保留和基础限流。

Connector 监听端口必须只允许 NGINX 节点访问，防止攻击者直接伪造 PROXY protocol。
2.3 备选模式：NGINX HTTP/2 终止
------------------------

只有在明确要求 NGINX 终止 mTLS 时，才应使用：
    Agent
      → NGINX HTTP/2 + mTLS
      → grpc_set_header 注入证书指纹
      → NGINX 到 Connector 再使用 mTLS

这种模式能合法注入 gRPC metadata，但会带来：

* NGINX 成为完整的 Agent 身份信任边界；

* 必须防止客户端自行构造同名 metadata；

* Connector 只能信任来自 NGINX mTLS 身份的头；

* 不同 gRPC RPC 可能被分配到不同后端；

* NGINX 的读写超时必须覆盖长期流。

综合安全和复杂度，仍建议采用 **L4 透传 + Connector 终止 mTLS**。
2.4 Connector 不是“无状态”
---------------------

文档中“Connector 无状态，会话在 Redis”这个表述需要修改。

真实情况是：

* Agent 的 TCP/HTTP2/gRPC 连接对象只存在于当前 Connector 内存；

* Redis 只保存“这个 Agent 当前在哪个 Connector”；

* Redis 无法把一条活动连接迁移到另一个 Connector；

* Connector 宕机后，该实例上的 Agent 必须重连。

更准确的定义是：

> **Connector 是会话有状态、持久业务状态外置、节点可丢弃重建的服务。**

2.5 优雅排空不能只等待“自然迁移”
-------------------

长期 gRPC Stream 可能运行数天甚至数月，不会自然结束。gRPC Graceful Shutdown 会停止接收新 RPC 并等待进行中的 RPC 完成，但长期流本身可能一直不完成。因此，仅仅 GOAWAY 后等待自然迁移并不够，需要：

1. Connector 进入 `DRAINING`；

2. NGINX 停止向该实例分配新连接；

3. Connector 向 Agent 发送 `ReconnectHint`；

4. Agent 在随机延迟后主动重连；

5. 超过排空期限后，Connector 主动关闭剩余流。

还应设置随机化的最大连接生命周期，例如 6～24 小时范围内抖动，防止所有连接永久固化在最初的 Connector 上。gRPC 官方建议对长期逻辑流使用 Streaming RPC，并谨慎配置 Keepalive；优雅关闭还需要为长期调用设置明确的完成期限。([gRPC](https://grpc.io/docs/guides/keepalive/?utm_source=chatgpt.com "Keepalive - gRPC"))

* * *

三、建议优化后的总体平台架构
==============

    ┌──────────────────────────── 中心控制面 ────────────────────────────┐
    │                                                                  │
    │ 管控台 / OpenAPI                                                 │
    │       │                                                          │
    │       ▼                                                          │
    │ OIDC/IAM/MFA ──▶ Policy & Approval Service                       │
    │                         │                                        │
    │                         ▼                                        │
    │                 Command Service                                  │
    │          当前状态 + Event Log + Transactional Outbox             │
    │                         │                                        │
    │                         ▼                                        │
    │              Dedicated Signer / KMS / HSM                         │
    │                         │                                        │
    │                         ▼                                        │
    │              Dispatch Hub / Global Router                        │
    │                  asset_id → cell_id                               │
    │                         ▲                                        │
    │                         │ Connector 主动建立控制隧道               │
    │                                                                  │
    │ Register/PKI  CMDB  Config  Artifact  Audit  Telemetry Ingestor   │
    └─────────────────────────┬────────────────────────────────────────┘
                              │ mTLS ConnectorControlStream
                ┌─────────────┴────────── 区域 Cell ───────────────────┐
                │                                                      │
                │  L4 VIP / NGINX stream TLS 透传                       │
                │                   │                                  │
                │                   ▼                                  │
                │       Go Connector 集群                              │
                │       ├─ Agent mTLS 终止                             │
                │       ├─ 本地活动连接表                              │
                │       ├─ Session Fencing                            │
                │       ├─ 控制/指标/可靠消息分流                      │
                │       ├─ Kafka Producer                             │
                │       └─ Metrics 可选磁盘缓冲                       │
                │                   │                                  │
                │            Cell Session Directory                    │
                │          asset → connector/session/epoch              │
                │                                                      │
                │ Metrics Kafka       Critical Kafka                   │
                │                                                      │
                │ Bootstrap / Artifact / Result Blob Endpoint          │
                │      同一 VIP:443，不同 SNI 或路径                    │
                └────────────────────┬─────────────────────────────────┘
                                     │
                               1 条 gRPC Channel
                        ┌────────────┼─────────────┐
                        ▼            ▼             ▼
                     Agent         Agent          Agent

为什么建议 Connector 主动连接中心
----------------------

当前方案是中心平台主动调用每个区域 Connector。可以进一步优化为：
    Connector → 中心 Dispatch Hub 建立长期双向控制流

这样：

* 每个 Connector 只需主动出站到中心；

* 不必向中心开放 Connector 管控端口；

* 中心只维护几十条 Connector 控制连接；

* 会话增量、Connector 心跳、命令下发走同一控制隧道；

* 网络和防火墙策略更简单；

* 中心无法访问区域时，Connector 仍可维持 Agent 连接和数据缓冲。

* * *

四、gRPC 协议必须修改
=============

当前定义为：
    rpc ControlStream(stream DownlinkMsg) returns (stream UplinkMsg);
    rpc MetricsStream(stream HostMetricBatch) returns (MetricsAck);

第一条方向与消息注释相反；第二条只有在 Agent 关闭发送流后才得到最终响应，不适合永久运行的指标流。

建议改为：
    service AgentGateway {
      // Agent -> Gateway: Hello、Heartbeat、CommandAck、CommandEvent
      // Gateway -> Agent: Welcome、Command、Cancel、ConfigPush、ReconnectHint
      rpc Control(
          stream AgentControlFrame
      ) returns (
          stream GatewayControlFrame
      );

      // 每个批次均可得到独立或累计 ACK
      rpc Metrics(
          stream MetricBatch
      ) returns (
          stream MetricAck
      );

      // 结果、审计、资产报告的可靠流
      rpc Reports(
          stream ReliableReport
      ) returns (
          stream ReportAck
      );
    }

    service AgentBootstrap {
      rpc Register(RegisterRequest) returns (RegisterResponse);
      rpc RotateCertificate(RotateCertRequest) returns (RotateCertResponse);
    }
4.1 连接建立握手
----------

Agent 建立控制流后，第一条消息必须是：
    AgentHello
    - asset_id
    - boot_id
    - agent_version
    - protocol_min_version
    - protocol_max_version
    - supported_schema_versions
    - supported_compressions
    - supported_actions
    - capabilities
    - current_config_version
    - last_report_ack_cursor

Gateway 返回：
    GatewayWelcome
    - session_id
    - session_epoch
    - server_time
    - heartbeat_interval
    - offline_timeout
    - max_message_bytes
    - max_inflight_reports
    - target_config_version
    - reconnect_after

任何未完成 Hello/Welcome 的连接都不能发送指标和结果。
4.2 三类 ACK 语义必须明确区分
-------------------

| 数据类型             | Gateway 返回最终 ACK 的条件    | Agent 收到 ACK 后行为                    |
| ---------------- | ----------------------- | ----------------------------------- |
| Metrics          | 已进入 Gateway 有界队列或 Kafka | 可删除对应 Metrics WAL；允许小窗口丢失           |
| Command Result   | Kafka `acks=all` 成功     | 删除对应结果 WAL                          |
| Audit/Security   | Kafka `acks=all` 成功     | 删除审计 WAL                            |
| Inventory        | Kafka `acks=all` 成功     | 删除 Inventory WAL                    |
| Inventory 业务处理结果 | CMDB 已处理                | 作为独立 `InventoryResult` 下发，不影响传输 ACK |

不能把“Connector 本地收到了”与“平台已经持久化”混成一个 ACK。

对于 Result/Audit/Inventory：

* 如果 Kafka 不可用，Connector 返回可重试错误或暂不 ACK；

* Agent 保留 WAL 并重试；

* Connector 即使有本地磁盘缓冲，也不能立刻返回最终 `COMMITTED`；

* 除非 Connector 的磁盘缓冲本身是跨节点复制的，否则单节点 fsync 不能视为平台级持久化。

Kafka Producer 对关键 Topic 应显式配置：
    enable.idempotence=true
    acks=all
    min.insync.replicas>=2

Kafka 的幂等 Producer 可以避免 Producer 重试产生重复写入，但消费端仍需按 `event_id/cmd_id` 幂等处理。([Apache Kafka](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html?utm_source=chatgpt.com "KafkaProducer (clients 4.1.2 API) - kafka.apache.org"))
4.3 每条可靠消息应增加
-------------

    event_id
    asset_id
    session_id
    session_epoch
    sequence
    created_at
    schema_version
    payload_sha256

ACK 推荐支持累计确认：
    acked_through_sequence
    ack_level
    retryable
    retry_after_ms
    error_code

* * *

五、Command Service 需要增加一致性设计
===========================

5.1 状态机应拆分“投递”和“执行”
-------------------

建议状态机调整为：
    DRAFT
      ↓
    PENDING_MFA
      ↓
    PENDING_APPROVAL
      ↓
    APPROVED
      ↓
    QUEUED
      ↓
    DISPATCHING
      ↓
    DELIVERED
      ↓
    ACCEPTED
      ↓
    RUNNING
      ├─ SUCCEEDED
      ├─ FAILED
      ├─ TIMEOUT_KILLED
      ├─ CANCELLED
      └─ LOST / UNKNOWN

需要明确：

* `DELIVERED`：Connector 已把消息写入 Agent 控制流；

* `ACCEPTED`：Agent 完成验签、策略和资源检查；

* `RUNNING`：Agent 已真正启动任务；

* `REJECTED_BUSY`：通常是可重试状态，应回到 `QUEUED_RETRY`，不一定是终态；

* `REJECTED_POLICY`：终态；

* Agent 断线后结果不明时标记 `UNKNOWN`，不能对未来写操作盲目自动重试。

5.2 增加事务 Outbox
---------------

当前 DDL 中没有 Outbox 表。建议增加：
    cmd_outbox
    - event_id
    - cmd_id
    - event_type
    - payload_bytes
    - available_at
    - attempts
    - locked_by
    - locked_until
    - published_at
    - last_error

流程：
    数据库事务：
    1. cmd_command: APPROVED → QUEUED
    2. 插入 cmd_outbox(DISPATCH_REQUESTED)
    3. 提交

    Dispatcher：
    4. 读取 Outbox
    5. 查询当前 session
    6. 创建短期签名信封
    7. 推送 Connector
    8. 记录 dispatch_attempt
    9. 标记 Outbox 已处理

这样可以防止状态和投递行为不一致。
5.3 审批后不要立即生成最终信封
-----------------

当前状态机描述是审批通过后生成信封，然后目标离线则长期排队。目标离线时间可能超过信封有效期。

应改成：

1. 审批完成时冻结：
   
   * action；
   
   * 参数；
   
   * artifact；
   
   * target snapshot；
   
   * command spec hash；
   
   * policy version；
   
   * 审批链 hash。

2. Agent 在线且准备投递时：
   
   * 为具体 asset_id 生成短期信封；
   
   * 有效期 1～5 分钟；
   
   * 写入 `dispatch_attempt`；
   
   * 使用 KMS/HSM 签名。

3. 投递失败或信封过期时：
   
   * 增加 attempt；
   
   * 重新生成新信封；
   
   * 原审批结果继续有效，除非审批自身已过期。

5.4 不要把签名信封存成 JSONB
-------------------

当前设计：
    envelope JSONB

但 Agent 设计已经确定对规范化 Protobuf 信封进行 Ed25519 签名。JSONB 取出后字段顺序、数值格式或序列化方式变化，都可能导致签名验证不一致。

建议改成：
    command_spec_bytes BYTEA
    command_spec_sha256 BYTEA
    envelope_bytes BYTEA
    envelope_signature BYTEA
    signing_key_id VARCHAR
    envelope_expires_at TIMESTAMPTZ

JSONB 可以额外保留一份供查询和页面展示，但不能作为签名原文。
5.5 增加投递尝试表
-----------

    cmd_dispatch_attempt
    - attempt_id
    - cmd_id
    - attempt_no
    - session_id
    - session_epoch
    - connector_id
    - envelope_bytes
    - signing_key_id
    - envelope_expires_at
    - status
    - sent_at
    - delivered_at
    - accepted_at
    - error_code

这样才能正确分析：

* 命令到底投递过几次；

* 是否投递到了旧会话；

* 是 Connector NACK 还是 Agent 拒绝；

* 是否发生过重复投递；

* 哪一次投递产生了最终结果。

5.6 批量任务必须冻结目标快照
----------------

当前 `target_scope JSONB` 会产生一个安全问题：
    审批时目标是 1,000 台
    执行时 CMDB 查询结果变成 1,200 台

审批不能自动覆盖后加入的 200 台。

建议增加：
    cmd_batch_target
    - batch_id
    - asset_id
    - cell_id
    - wave_no
    - target_order
    - state
    - last_cmd_id

审批信封绑定：
    target_snapshot_sha256
    target_count

百分比灰度应使用确定性算法，例如：
    hash(batch_id + asset_id) % 10000 < percentage * 100

这样暂停、恢复、审计时目标集合不会漂移。

* * *

六、Session Router 要增加会话栅栏
========================

6.1 Redis 记录建议
--------------

    SESS:{asset_id}
    {
      cell_id,
      connector_id,
      session_id,
      session_epoch,
      connected_at,
      last_heartbeat,
      boot_id,
      agent_version,
      protocol_version,
      cert_serial,
      source_ip,
      config_version
    }
    TTL = 3 × heartbeat interval

连接注册使用 Redis Lua/CAS：

1. 新会话读取当前 epoch；

2. 原子递增 epoch；

3. 写入新 session；

4. 向旧 Connector 发送 fence；

5. 旧 Connector 后续消息因 epoch 过期被拒绝。

所有命令推送都必须携带：
    expected_session_id
    expected_session_epoch

Connector 只有在本地连接完全匹配时才能投递。
6.2 重复连接不能静默覆盖
--------------

以下情况应产生安全事件：

* 同一 asset_id 同时从不同 IP/Cell 连接；

* boot_id 不同；

* 证书相同但主机指纹明显不同；

* 同一证书被同时使用。

处理策略可以配置为：
    replace_old
    reject_new
    quarantine_both

默认建议：**新连接获得新 epoch，旧连接被断开，同时产生高优先级安全事件。**
6.3 不要把每次心跳写 Kafka
------------------

若 20 万 Agent 每 30 秒一次心跳：
    200,000 × 2,880 次/天 = 5.76 亿次心跳/天

把每次心跳都写 Redis尚可控制，但再写 Kafka 和 PostgreSQL 没有必要。

建议：

* Redis：刷新 TTL；

* Connector 本地：维护精确心跳；

* Kafka：只写连接、断开、版本变化、证书变化、Cell 变化等状态事件；

* PostgreSQL：仅持久化状态变化及低频批量快照；

* `gateway_node.current_conns` 走监控系统，不要每几秒更新关系数据库。

6.4 Session Router 只应是路由缓存
--------------------------

即使 Redis 不可用：

* 已建立的 Agent 连接不能断；

* 指标仍可以上送 Kafka；

* Connector 保留本地会话；

* 新命令下发暂停或走本地缓存；

* Redis 恢复后 Connector 批量重新注册会话。

进一步可采用两级路由：
    中心 Global Router：
    asset_id → cell_id

    Cell Session Directory：
    asset_id → connector_id/session_id/epoch

这样中心故障不会破坏 Cell 内的会话管理。

* * *

七、注册、证书和制品链路还有缺口
================

7.1 首次注册必须经过 Gateway
--------------------

Agent 首次启动时还没有设备证书，因此应定义独立 Bootstrap 接口：
    Agent
      → Gateway VIP:443
      → server-auth TLS
      → bootstrap token + CSR
      → Register Service

建议使用同一 VIP、不同 SNI：
    agent-bootstrap.dc1.example
    agent-gateway.dc1.example
    agent-artifact.dc1.example
    agent-result.dc1.example

这与“生产服务器只允许访问 Gateway VIP:443”的防火墙原则保持一致。

当前文档一方面要求 Agent 只能访问 Gateway VIP，另一方面又要求 Agent 就近访问区域制品缓存，但没有给出网络路径，需要补齐。
7.2 注册必须幂等
----------

`RegisterRequest` 增加：
    enrollment_request_id
    bootstrap_batch_id
    csr_public_key_sha256
    request_nonce

Register Service 保存注册幂等记录。若响应丢失后 Agent 重试，应返回原来的：
    asset_id
    certificate
    cell_gateway_addrs

不能重复创建资产、重复消耗 Token 或重复签发多个身份。
7.3 machine-id、MAC、SN 不是强身份凭证
-----------------------------

这些信息可以伪造，也可能因为虚拟机模板、硬件厂商默认值而重复。

因此应明确：

* Bootstrap Token 才是首次注册的主要信任根；

* machine-id、MAC、SN 是冲突检测和风险评分材料；

* 不应把每种材料都设为绝对唯一；

* `To Be Filled By O.E.M.`、空 SN、全零 MAC 等值必须过滤；

* machine-id 重复但 board SN/MAC 不同，往往是克隆场景，不应全部进入人工审核。

当前：
    CREATE UNIQUE INDEX uq_identity
    ON asset_identity(id_type, id_value);

过于严格。建议增加：
    normalized_value
    confidence
    verified
    binding_status
    first_seen_at
    last_seen_at

仅对高可信且有效的身份材料建立部分唯一约束。
7.4 补充证书生命周期
------------

设备证书表需要增加：
    issuer_id
    key_id
    revoked_at
    revocation_reason
    replaced_by_cert_id
    last_seen_at

证书轮换流程建议为：

1. Agent 用旧证书建立 mTLS；

2. Agent 生成新私钥和 CSR；

3. 使用旧身份提交轮换请求；

4. 平台签发新证书；

5. 新旧证书短时重叠；

6. Agent 使用新证书连接成功；

7. 旧证书吊销。

Connector 需要维护本地证书状态缓存，不能在每次连接时同步查询 PostgreSQL。

* * *

八、IAM、MFA 和签名密钥需要进一步加强
======================

8.1 不建议自己实现完整 IAM
-----------------

“部门现有平台没有 MFA”并不意味着必须从账号、密码、会话、TOTP 开始全部自研。

更安全的方式是：
    成熟 OIDC 身份服务
      ├─ 对接 AD/LDAP
      ├─ TOTP/WebAuthn
      ├─ 会话管理
      ├─ 账号锁定
      └─ 密码策略

    Agent 管控平台
      ├─ 保存 subject_id
      ├─ 角色和资源范围映射
      ├─ 指令审批
      └─ Step-up MFA 结果

若确实只能内置，当前单个 `role` 字段不够，应至少改成：
    user
    role
    permission
    user_role
    role_permission
    resource_scope
    user_scope

权限需要支持：
    用户 A：
    只能操作 dc1
    只能操作 order-service
    只能执行 diagnose.*
    不能执行 script.run
    最多批量 100 台

而不是只有 viewer/operator/approver/admin 四个全局角色。
8.2 MFA 挑战必须绑定具体操作
------------------

当前 `mfa_challenge` 只有 `scene/ref_id/verified`，存在验证结果被复用的风险。

建议增加：
    challenge_nonce
    bound_digest
    verified_at
    consumed_at
    source_ip
    attempts
    locked_until

其中：
    bound_digest =
    SHA256(action + params + artifact + target_snapshot + timeout)

MFA 成功后必须一次性消费，命令内容改变则 MFA 自动失效。

高危命令建议：

* 操作人执行 Step-up MFA；

* 审批人也执行 Step-up MFA；

* approver 不得等于 operator；

* 两个审批人必须互不相同；

* admin 不能默认绕过审计和审批。

8.3 签名密钥不能等到 M5 才进入 KMS/HSM
---------------------------

指令签名私钥一旦泄露，相当于攻击者获得了 20 万台服务器的远程命令能力。

因此：

> **Command Signer + KMS/HSM 应进入首次开放指令能力之前的 P0/P1，而不是后续安全加固阶段。**

建议分离：

* Agent 设备证书 CA；

* Connector 服务证书 CA；

* 平台内部服务 CA；

* Command Envelope Signing Key；

* Artifact Signing Key；

* Audit Anchor Signing Key。

Command Service 不应持有通用私钥文件，而应调用独立 Signer 服务。Signer 接口不接受任意字节，只接受已经审批的 `cmd_id`，自行从数据库读取冻结后的命令规格并生成规范化信封。

* * *

九、数据库模型需要修订
===========

9.1 PostgreSQL 版本
-----------------

文档写 PostgreSQL 13+，但 PostgreSQL 13 已结束官方支持。建议基线改为：
    PostgreSQL 16/17，或企业当前仍受支持的版本

并将版本升级策略纳入平台生命周期。PostgreSQL 官方对每个主版本提供约五年支持，13 已列入不再支持版本。([PostgreSQL](https://www.postgresql.org/support/versioning/?utm_source=chatgpt.com "Versioning Policy - PostgreSQL"))
9.2 Inventory 分区表主键错误
---------------------

当前：
    CREATE TABLE inventory_report (
        report_id BIGSERIAL PRIMARY KEY,
        ...
        received_at TIMESTAMPTZ
    ) PARTITION BY RANGE (received_at);

PostgreSQL 要求分区表上的主键或唯一约束包含所有分区键，因此该定义不能直接创建。 ([PostgreSQL](https://www.postgresql.org/docs/current/ddl-partitioning.html?utm_source=chatgpt.com "PostgreSQL: Documentation: 18: 5.12. Table Partitioning"))

应改成：
    PRIMARY KEY (received_at, report_id)

并自动创建：

* 当月分区；

* 下月预建分区；

* DEFAULT 分区；

* 过期分区归档与删除任务。

9.3 不要把 6 个月 Inventory 全文都放 JSONB
---------------------------------

假设：
    20 万台
    每天 1 次全量
    保留 180 天

就是 3,600 万份全量报告。若平均 20KB，原始数据就约 720GB，还不包括 TOAST、索引、MVCC 和副本。

建议：
    inventory_report
    - report_id
    - asset_id
    - revision
    - report_type
    - schema_version
    - payload_sha256
    - payload_ref          -- 对象存储位置
    - payload_size
    - received_at
    - processed_at
    - process_status

完整 Protobuf 原文存对象存储，PostgreSQL 只存索引和处理状态。
9.4 资产状态拆分
----------

当前：
    status = active/offline/decommissioned

把资产生命周期和在线状态混在一起。

建议拆成：
    lifecycle_status:
    provisioning/active/maintenance/decommissioned

    connectivity_status:
    online/offline/unknown

    health_status:
    healthy/degraded/unhealthy

    trust_status:
    trusted/quarantined/revoked

“离线”不能覆盖“已下线”或“维护中”的资产生命周期状态。
9.5 NIC 与 IP 建议规范化
------------------

当前：
    asset_nic.ipv4 INET[]
    asset_nic.ipv6 INET[]

不利于：

* 按 IP 查询资产；

* 检测 IP 冲突；

* 建立单 IP 索引；

* 保存 IP 变化历史。

建议拆为：
    asset_interface
    asset_ip_address
9.6 Package 主键需要调整
------------------

当前：
    PRIMARY KEY(asset_id, pkg_name)

同一主机可能同时安装同名不同架构或不同 slot/version 的软件包。

建议使用：
    PRIMARY KEY(
      asset_id,
      pkg_name,
      pkg_version,
      pkg_arch
    )

或生成规范化的 `package_key`。
9.7 进程主键不能只用 PID
----------------

后续进程模型不能采用：
    PRIMARY KEY(asset_id, pid)

PID 会复用。至少应使用：
    asset_id + boot_id + pid + process_start_time
9.8 Audit 全局哈希链不可扩展
-------------------

当前每条审计都依赖上一条的 `prev_hash`。在多个消费实例并发写入时，这会要求全局串行化。

建议改为：
    chain_id = topic_partition 或 asset_id shard
    sequence
    prev_hash
    entry_hash

每分钟或每批构造 Merkle Root，再把 Root 写入外部不可变存储。PostgreSQL 只追加权限不能抵御数据库超级管理员修改，最终审计原文仍应写入 WORM/Object Lock 存储。
9.9 建议新增的核心表
------------

至少增加：
    cmd_event
    cmd_outbox
    cmd_dispatch_attempt
    cmd_batch_target
    cmd_idempotency
    agent_config_profile
    agent_config_assignment
    agent_config_status
    register_enrollment_request
    certificate_revocation
    artifact_scan_result
    audit_anchor

其中 `cmd_event` 应是追加式命令生命周期记录，`cmd_command` 只保存当前状态快照。

* * *

十、Kafka 与数据底座优化
===============

10.1 分区数不应直接冻结为 256～512
-----------------------

按照当前容量假设：
    20 万台 × 2KB / 10 秒 = 40MB/s

如果平均分布到 10 个 Cell，则单 Cell 约 4MB/s。此时每 Cell 直接分配 256 或 512 个分区，可能远高于实际吞吐需求。

建议 M0 对以下档位分别压测：
    32 / 64 / 128 / 256 partitions

计算公式：
    P >= 峰值入口吞吐 / 单分区实测安全吞吐
    P >= 最大消费并行度
    P >= 热点与故障隔离要求

最终按最大值取整，而不是先设一个很大的固定值。
10.2 5 Broker 只是占位，不是容量结论
-------------------------

40MB/s 压缩后数据对应：
    每天逻辑写入约 3.46TB
    RF=3 后每天物理写入约 10.37TB

还需要考虑：

* Consumer 读取流量；

* Kafka Segment 和索引；

* Broker 故障后的副本重建；

* 磁盘 30%～40% 空闲空间；

* 24 小时保留窗口；

* 峰值和 WAL 重放。

因此“5 Broker 起步”必须附带：
    单 Broker 磁盘容量
    顺序写吞吐
    网络带宽
    故障时剩余 Broker 容量
    副本重建时间
10.3 Telemetry 与 Critical 建议物理隔离
--------------------------------

推荐：
    Telemetry Kafka
    - metrics
    - 普通 agent events
    - 允许有界丢失
    - 高吞吐

    Critical Kafka
    - command result
    - audit
    - inventory
    - registration/security events
    - acks=all
    - 严格配额

最少也要使用不同 Producer、不同队列、不同 Broker 配额，保证指标洪峰不会影响命令结果和审计。
10.4 大输出不能进 Kafka
-----------------

Agent stdout/stderr 最大可达约 2MB，批量排障时直接进入 Kafka 会迅速扩大 Broker 压力。

建议：
    小结果：
    head/tail + exit code → Kafka

    大结果：
    Agent → 区域 Result Blob Endpoint → 对象存储
    Kafka 只传 result_ref + sha256 + size

下载结果必须经过权限校验并记录审计。
10.5 CMDB 标签不要全部写入时序标签
----------------------

建议写入时序库的稳定标签：
    asset_id
    cell_id
    idc
    arch
    os
    cluster（若变化不频繁）

以下高变化标签更适合保留在 CMDB 或查询索引中：
    owner
    负责人
    值班组
    临时业务归属
    变更工单

这些字段频繁变化时，会造成时序序列重建和基数增长。

* * *

十一、容量规划需要改成“按 Cell N+1”
=======================

当前设计同时给出：

* 每 Cell 1～3 万台；

* 每 Connector 5,000 连接；

* 每 Cell Gateway ≥3 实例；

* 全局约 52 个实例。

这几项不能直接同时成立。

以 3 万台 Cell 为例：
    单实例安全容量 = 5,000
    正常承载至少需要 = 30,000 / 5,000 = 6 实例
    N+1 后至少 = 7 实例
    再考虑 30% 余量，建议约 8 实例

因此应改为：
    connector_count_per_cell =
    ceil(
      peak_online_agents
      × headroom_factor
      / tested_safe_capacity_per_connector
    )
    + failure_reserve

全局 52 台只能做预算参考，真正容量必须按 Cell 分配。某个 Cell 的空闲容量不能帮助另一个 Cell 承载连接。

如果希望每 Cell 只部署 3 个 Connector，且最大 Cell 为 3 万台，那么单实例必须在一个实例故障后仍承载：
    30,000 / 2 = 15,000 条连接

考虑余量后，M0 应证明单 Connector 可安全承载约 20,000 条连接，而不是 5,000 条。

此外，NGINX 四层代理为每个 Agent 同时持有：

* 一个 Agent→NGINX Socket；

* 一个 NGINX→Connector Socket。

所以 3 万 Agent 对 NGINX 至少是约 6 万个 FD，还未计算监听、日志和管理连接。

* * *

十二、建议补充的 SLO、RPO 和 M0 验收指标
==========================

12.1 建议平台 SLO
-------------

| 指标                     | 建议目标               |
| ---------------------- | ------------------ |
| 在线 Agent 指令投递延迟        | p99 ≤ 2 秒          |
| Agent 离线识别             | ≤ 90～120 秒         |
| Command 状态查询可用性        | ≥ 99.9%            |
| 控制流可用性                 | ≥ 99.95%           |
| Metrics 有界丢失           | 按配置明确，不隐性承诺零丢失     |
| Result/Audit/Inventory | 平台接收后不得丢失          |
| 证书吊销传播                 | ≤ 5 分钟             |
| 单 Cell 故障影响范围          | 不扩散到其他 Cell        |
| 批量命令失败熔断               | 达阈值后停止新投递，不影响已运行任务 |

12.2 建议 M0 必测场景
---------------

1. 3 万 Agent 单 Cell 长连接；

2. 单 Connector 故障，剩余实例承载全部连接；

3. 3 万 Agent 同时重连；

4. 5 万 Agent 跨 Cell 同时重连；

5. Redis 完全不可用，但现有 Agent 连接不受影响；

6. Kafka 中断 30 分钟；

7. Kafka 恢复后的限速重放；

8. Kafka 中断期间 Connector 重启；

9. Result/Audit/Inventory 零丢失验证；

10. 同一命令重复投递 100 次，仅执行一次；

11. Agent 执行成功但结果 ACK 丢失；

12. Agent 执行后立即重启；

13. 新旧两个 session 同时在线；

14. Connector 优雅排空；

15. 证书吊销和轮换；

16. Bootstrap RegisterResponse 丢失后重试；

17. 20 万资产批次目标快照；

18. 批量失败率熔断；

19. 签名密钥轮换；

20. Agent N-2 版本与新 Gateway 协议兼容。

* * *

十三、与 Agent 设计仍需补齐的接口对齐项
=======================

| Agent 设计要求       | 平台稿当前缺口                                          |
| ---------------- | ------------------------------------------------ |
| 首次注册经 Gateway    | Proto 中没有 Bootstrap/Register RPC                 |
| Inventory 属于可靠消息 | 未定义传输 ACK 与 CMDB 业务 ACK 的区别                      |
| Agent 持久化防重放     | 平台没有 dispatch attempt 和 session epoch            |
| 动作目录版本           | 缺少 Agent capabilities/action catalog 握手          |
| ConfigPush       | 没有配置签名、版本、应用 ACK、回滚状态                            |
| 制品下载             | 没有说明只允许 Gateway VIP 时如何访问区域缓存                    |
| 大结果回传            | `cmd_result.stdout_ref` 已预留，但没有 Result Blob 上传协议 |
| Agent 连接最大生命周期   | Gateway 只描述 GOAWAY，没有重连提示和排空期限                   |
| 进程上传默认关闭         | Agent 与平台配置字段命名尚未完全统一                            |
| HTTPS 长轮询兼容模式    | 平台侧没有对应协议和容量设计                                   |

* * *

十四、建议的文档 v0.2 修改顺序
==================

P0：冻结 Proto 和 DDL 前
-------------------

1. NGINX 默认改为 TLS 透传；

2. 修正三条 gRPC 接口；

3. 定义 Session Hello、epoch、fencing；

4. 定义 Metrics 与 Critical ACK 边界；

5. 增加 Command Outbox、Event、Dispatch Attempt；

6. 改为实际投递时生成短期信封；

7. Bootstrap/Register 幂等化；

8. 明确 Artifact 和 Result Blob 网络路径；

9. 修复 PostgreSQL 分区表 DDL；

10. 将 Command Signer/KMS 提前。

P1：百台灰度前
--------

1. 完整证书轮换和吊销；

2. RBAC + 资源范围；

3. MFA 一次性绑定具体命令；

4. 批量目标快照；

5. Config desired/observed 状态；

6. Audit WORM 存储；

7. 关键 Topic 幂等 Producer；

8. Agent/Gateway 协议版本协商。

P2：万台规模前
--------

1. Cell 两级 Session Router；

2. Connector 主动控制隧道；

3. Telemetry Ingestor 和基数治理；

4. 区域 Artifact/Result 缓存；

5. Cell 级配额与熔断；

6. 多 Cell 故障演练；

7. PostgreSQL PITR、Kafka 恢复和密钥灾备。

P3：20 万规模前
----------

1. 3 万连接单 Cell 的 N+1 验证；

2. 5 万连接重连风暴；

3. 全链路容量模型；

4. 多版本兼容和分批升级；

5. 灾难恢复演练；

6. CA、签名密钥和证书批量轮换演练。

* * *

最终建议
====

当前平台设计的**大方向不需要推翻**，应保留：

* Agent 主动出站；

* 区域 Cell；

* NGINX + Go Connector；

* Kafka 只位于平台内部；

* 中心 Command Service；

* 平台侧 MFA 和审批；

* CMDB、制品、审计、升级管理独立领域。

需要重点调整为：

> **NGINX 只做 L4 接入且默认 TLS 透传；Connector 终止 Agent mTLS；会话必须带 session epoch；命令采用数据库状态机 + Event Log + Transactional Outbox；审批冻结命令但信封在实际投递时签发；关键消息只有 Kafka 持久化后才最终 ACK；Session Router 是可重建路由缓存，不是活动连接本身；所有大文件通过同一 Gateway VIP 后的制品/结果服务传输。**

完成以上 P0 修改后，这份管控平台设计稿才适合升级为 **v0.2 架构基线**，并进入 Proto、表结构和 M0 原型的正式冻结阶段。
