# AIOps Agent 管控体系总体架构设计

> 版本：v0.3（架构基线，开放问题已全部决策回填）
> 日期：2026-07-19
> 状态：**基线冻结候选**（配套文档：`agent-design.md` v0.2、`platform-architecture-chatgpt.md` 评审存档）
> 项目目录：`aiops_tools/tom_ai_agent`
> 变更说明：v0.2 按评审意见完成 10 项 P0 修正（评估记录见 §11）；v0.3 回填 §10 全部七项决策（§12 决策记录）

---

## 1. 文档目的与范围

本文档定义 Agent 管控体系三层架构与平台侧详细设计，达到可指导编码的深度：

```
┌─────────────────────────────────────────────────────────┐
│  L1  Agent 管控平台（中心，唯一控制面）                     │
│      管控台/API · 命令服务(状态机+Outbox) · Signer/KMS ·    │
│      IAM/MFA/审批 · 注册服务 · Dispatch Hub · CMDB ·       │
│      制品服务 · 审计中心 · 升级管理                          │
├─────────────────────────────────────────────────────────┤
│  L2  Agent Gateway（区域 Cell，每 IDC/网络域一组集群）       │
│      NGINX(L4接入,TLS透传) + Go Connector(终止mTLS/会话/汇聚)│
├─────────────────────────────────────────────────────────┤
│  L3  tom_ai_agent（20 万台主机，纯出站，无入站端口）          │
└─────────────────────────────────────────────────────────┘
```

**已确认决策**（讨论结论 + 评审修正 + v0.3 决策回填）：
- Gateway = **NGINX stream(L4) + 自研 Go Connector**；**NGINX 默认 TLS 透传，mTLS 由 Connector 终止**（§3.1）；
- 部门无 MFA/OIDC 设施 → **IAM 全自研**（账号 + TOTP + RBAC/资源范围 + 审批，v0.3 决策 D3，§4.4）；
- 部门无 CMDB → **新建轻量 CMDB**（v0.3 决策 D1，§4.6、§5.3）；
- 部门无 CA/KMS 设施 → **内置自建 PKI 服务与 Signer/KMS**（v0.3 决策 D2，§4.4/§4.5）；
- 注册引导凭据分批签发、分批部署；注册幂等（§4.3/§4.5）；
- 进程信息：agent 先具备采集能力，上送/存储缓建（§5.3）；
- 时序库 = **VictoriaMetrics**（v0.3 决策 D5，与总体方案数据底座对齐）；
- 平台技术栈 = **统一 Golang**（管控台后端、全部平台服务、Connector，v0.3 决策 D6）；
- 对象存储 = 开发期 MinIO 一套共用，生产环境换新实例，**以 S3 兼容接口抽象 + 配置切换**（v0.3 决策 D7，§7.4）。

---

## 2. 总体架构

### 2.1 部署拓扑与规模模型

| 项 | 数值/规则 | 说明 |
|---|---|---|
| 主机规模 | 200,000 台 | agent 全量部署目标 |
| Cell 划分 | 按 IDC/网络域，每 Cell 1–3 万台 | 爆炸半径控制 |
| Connector 容量 | **按 Cell N+1 规划，不按全局摊派**（v0.2 修正，§7.1） | 单实例安全容量 M0 压测定案（目标 ≥10,000 连接） |
| 每 Cell | NGINX 接入层 + Connector 集群（≥3） | N+1 |
| Kafka | **遥测集群 + 关键集群物理隔离**（v0.2） | 只对 Gateway/平台消费服务暴露 |
| 管控平台 | 中心双活 | 控制面 |

### 2.2 数据通路总览（v0.2 修订）

```
操作人                  L1 管控平台（中心）                  L2 区域 Cell           L3 Agent
  │                         │                                 │                     │
  ▼                         ▼                                 │                     │
┌───────┐ ①登录      ┌────────────┐                            │                     │
│ 管控台 │──────────▶│ 自研 IAM    │                            │                     │
└───────┘            │ (TOTP/MFA) │                            │                     │
  │ ②发起指令        └────────────┘                            │                     │
  ▼                   ┌─────────┐                              │                     │
┌─────────────────┐  │ 审批服务  │                              │                     │
│ Command Service │◀─│(冻结命令) │                              │                     │
│ 状态机+EventLog │  └─────────┘                              │                     │
│ +事务Outbox     │                                           │                     │
└────┬───────▲──────┘                                           │                     │
     │③读取    │⑧结果回写                                        │                     │
     ▼         │                                                │                     │
┌─────────┐   │          ┌──────────────┐   ⑤控制隧道(Connector │                     │
│ Signer  │   │          │ Dispatch Hub │◀──主动出站建立,双向流)──┼──┐                  │
│(KMS/HSM)│───┘          │ Global Router│   (会话增量/心跳/下发)  │  │                  │
│④投递时   │             └──────────────┘                       │  │                  │
│ 签发短期 │                          ▲                          │  │                  │
│ 信封    │              ┌───────────┴───────────┐              │  │                  │
└─────────┘              │ Cell Session Directory│              │  │                  │
                         │ asset→connector/epoch │              │  │                  │
                         └───────────────────────┘              │  │                  │
   Register/PKI · CMDB · Artifact · Audit · Ingestor            │  │                  │
              ▲ ⑥Kafka(遥测/关键两集群)                          │  │                  │
              └────────────────────────────────┐                │  │                  │
                                               │                │  │                  │
                          ┌────────────────────┴───┐            │  │                  │
                          │ NGINX stream(L4,TLS透传) │            │  │                  │
                          │  +Go Connector 集群     │◀───────────┼──┼──mTLS gRPC───────┤
                          │  (终止mTLS/验签/分流/    │  同一VIP:443  │  (控制/指标/可靠流) │
                          │   Kafka Producer)      │  不同SNI:     │                     │
                          │  +Artifact/Result Blob │  gateway/     │                     │
                          └────────────────────────┘  bootstrap/   │                     │
                                                       artifact/   │                     │
                                                       result      │                     │
```

关键链路：

| # | 链路 | 说明 |
|---|---|---|
| ① | 管控台 → 自研 IAM | 登录认证（口令 + TOTP）；高危操作 Step-up TOTP |
| ② | 管控台 → Command Service | 指令创建，进入状态机；审批时**冻结命令规格**（不签信封） |
| ③ | Command → Signer | **实际投递时**才调用 Signer 签发分钟级短期信封（v0.2 修正） |
| ④ | Dispatcher（Outbox 驱动）→ Dispatch Hub | 状态迁移与 Outbox 同事务；Dispatcher 读 Outbox 投递 |
| ⑤ | **Connector 主动出站连 Dispatch Hub**（v0.2 修正） | 中心不向区域开端口；中心只维护几十条 Connector 控制隧道 |
| ⑥ | Connector → Kafka | 遥测/关键两集群；大结果走 Result Blob，Kafka 只传引用 |
| — | agent → 区域 VIP:443 | 唯一出站目标；SNI 区分 gateway/bootstrap/artifact/result 四类服务 |

### 2.3 故障域与降级策略

| 故障 | 影响 | 降级行为 |
|---|---|---|
| 单 Connector 宕 | 其连接 agent 重连 | agent 带抖动重连本 Cell 其他实例；会话经 Session Directory 重建（epoch 递增） |
| 整 Cell Gateway 宕 | Cell 内 agent 离线 | agent WAL 积压；跳板机应急通道兜底 |
| 遥测 Kafka 宕 | 指标缓冲 | Connector 磁盘缓冲（**指标可缓冲，但不给最终 ACK 语义承诺**）；恢复限速重放 |
| 关键 Kafka 宕 | 结果/审计/inventory 不能确认 | Connector 返回可重试错误，**agent 保留 WAL 重试**（v0.2：Connector 本地落盘 ≠ 平台持久化，不得返回最终 ACK） |
| Redis(Session Directory) 宕 | 新指令下发暂停 | **已建立连接不断、指标照上**（v0.2）；Connector 用本地会话表，恢复后批量重注册 |
| 管控平台宕 | 无法下发/注册/审批 | agent 与 Connector 自治：采集、缓冲、执行中任务继续 |
| CMDB 宕 | 资产不入库 | inventory 在 Kafka 积压，恢复后追平 |

---

## 3. L2：Agent Gateway 详细设计

### 3.1 接入模式定案（v0.2 P0 修正）：NGINX L4 + TLS 透传

v0.1 "NGINX 终止 mTLS 后经 PROXY protocol + 自定义头传证书指纹"**不可实现**：NGINX `stream` 模块工作在 TCP/TLS 层，不理解 HTTP/2 与 gRPC metadata，`grpc_set_header` 属于 HTTP 层 gRPC 代理模块而非 stream 模块。修正为：

```
agent ──mTLS+HTTP/2──▶ NGINX stream(L4)
                         ├─ TLS 原样透传(不终止)
                         ├─ PROXY protocol v2: 仅传源 IP
                         ├─ L4 能力: 连接限流/新建速率限制/被动健康检查/least_conn
                         └─▶ Go Connector
                               ├─ 终止 mTLS
                               ├─ 从证书 SAN 提取 asset_id
                               ├─ 校验序列号/指纹/吊销状态(本地证书状态缓存,非每次查库)
                               └─ 业务逻辑(§3.2)
```

- 身份直接来自密码学绑定的客户端证书，不依赖可伪造的中间头；NGINX 被攻破也无法伪造平台签发的 agent 证书；
- 同一 HTTP/2 连接上的控制流/指标流天然落在同一 Connector；
- **Connector 监听端口仅允许 NGINX 节点访问**（防伪造 PROXY protocol 源地址）；
- 备选模式（仅在明确要求 NGINX 终止 TLS 时）：NGINX HTTP/2 + mTLS + `grpc_set_header` 注入指纹 + NGINX→Connector 再 mTLS——信任边界扩大、流可能分散到不同后端，**不采用为默认**。

### 3.2 Connector 内部模块

| 模块 | 职责 |
|---|---|
| TLS/Auth | 终止 mTLS、证书校验（本地吊销缓存，≤5 分钟传播）、**Session Fencing（epoch 校验）** |
| ConnManager | 连接生命周期、本地活动连接表、心跳（间隔/超时由 Welcome 下发）、每连接读循环 |
| HelloHandler | Hello/Welcome 握手：协议版本协商、能力交换（supported_actions/schema/compression）、配置版本对齐 |
| UplinkTunnel | **主动出站**连接中心 Dispatch Hub：注册会话增量、上报心跳摘要、接收指令下推 |
| Downlink | Dispatch Hub 信封 → 校验 `expected_session_id/epoch` 与本地连接匹配 → 写控制流；不匹配回 NACK |
| Uplink | 数据流分流：metrics→批量聚合 Kafka producer；results/audit/inventory→关键集群即时 producer |
| Buffer | **仅指标**本地磁盘缓冲（分段+CRC+配额）；关键数据不本地落盘充 ACK（见 §3.4） |
| Governor | 全局限流：实例入口带宽、每 asset 消息频率、重连速率 |
| AdminAPI | 连接数/会话清单/排空控制/健康检查 |

**Connector 的定位修正（v0.2）**：不是"无状态"——活动连接对象只存在于当前 Connector 内存，Redis 只存路由映射，连接无法跨实例迁移。准确定义：**会话有状态、持久业务状态外置、节点可丢弃重建**。

### 3.3 优雅排空与连接生命周期（v0.2 补充）

长期 gRPC 流可能持续数天，GOAWAY 后"等自然结束"不可行。排空流程：

1. Connector 置 `DRAINING`，NGINX 停止分配新连接；
2. 向 agent 发送 `ReconnectHint{reconnect_after}`；
3. agent 随机延迟后主动重连（落到其他实例）；
4. 超过排空期限（默认 10 分钟）强断剩余流。

另设**随机化最大连接生命周期**（6–24h 抖动）：到期发 ReconnectHint，防连接永久固化在初始实例。

### 3.4 ACK 边界（v0.2 P0 修正）

| 数据类型 | Connector 返回最终 ACK 的条件 | agent 收到 ACK 后 |
|---|---|---|
| Metrics | 进入 Connector 有界队列或遥测 Kafka | 删指标 WAL（允许小窗口丢失） |
| Command Result | **关键 Kafka `acks=all` 成功** | 删结果 WAL |
| Audit/Security | **关键 Kafka `acks=all` 成功** | 删审计 WAL |
| Inventory | **关键 Kafka `acks=all` 成功** | 删 inventory WAL |
| Inventory 业务处理 | CMDB 消费入库后 | 独立 `InventoryResult` 经控制流下发，与传输 ACK 解耦 |

- "Connector 本地收到/落盘" ≠ "平台已持久化"：Connector 单节点 fsync 不构成平台级持久化，关键数据在 Kafka 不可用时**返回可重试错误或不 ACK**，agent 保留 WAL 重试；
- 关键 Topic producer 配置：`enable.idempotence=true`、`acks=all`、`min.insync.replicas≥2`；消费端按 `event_id/cmd_id` 幂等；
- 可靠消息统一携带：`event_id / asset_id / session_id / session_epoch / sequence / created_at / schema_version / payload_sha256`；ACK 支持累计确认（`acked_through_sequence / ack_level / retryable / retry_after_ms / error_code`）。

---

## 4. L1：Agent 管控平台详细设计

### 4.1 组件清单（v0.2）

| 组件 | 职责 | 部署 |
|---|---|---|
| 管控台 Web + OpenAPI | 人机入口 | 中心双节点 |
| **自研 IAM 服务**（含 RBAC/审批） | 账号/口令/TOTP/会话/锁定/密码策略 + RBAC/资源范围 + 指令审批流（v0.3 D3：部门无 OIDC 设施，按 §5.1 RBAC 模型全自研，Go 实现） | 中心 |
| Command Service | 指令状态机 + **Event Log + 事务 Outbox** + 批量编排 | 中心双节点 |
| **Signer 服务（独立，自研内置 KMS）** | 信封签名唯一出口；只接受已审批 cmd_id，自行读取冻结规格生成规范化信封；私钥由内置 KMS 保管（v0.3 D2：无现成 KMS/HSM 设施，自建，§4.4；**提前到 P0**） | 中心，高保障 |
| Dispatcher | 读 Outbox → 查会话 → 调 Signer 签短期信封 → 推送 → 记 dispatch_attempt | 中心 |
| Dispatch Hub / Global Router | asset_id→cell_id 路由；接收 Connector 控制隧道 | 中心 |
| Cell Session Directory | asset→connector/session/epoch（Redis CAS + 本地快照） | 区域 + 中心缓存 |
| Register/PKI Service | 注册（幂等）、asset_id/证书签发、引导批次、证书轮换吊销；**PKI 自研内置**（v0.3 D2：Go 自建 CA，§4.5） | 中心 |
| CMDB（新建轻量库） | 资产台账、inventory 入库、冲突检测、标签维护（v0.3 D1：部门无 CMDB，按 §5.3 新建） | 中心 |
| Artifact Service | 制品验签存储；区域缓存经同一 VIP SNI 暴露 | 中心 + 区域缓存 |
| Result Blob Endpoint | 大结果上传/下载（对象存储），权限校验 + 审计 | 区域 |
| Telemetry Ingestor | 指标消费、基数治理、拓扑标签补充、写时序库 | 中心 |
| Audit Center | 审计入库、**分片哈希链 + Merkle 锚定**、WORM 归档 | 中心 |
| Upgrade Manager | 升级批次、灰度、熔断 | 中心 |

### 4.2 指令生命周期（v0.2 扩展状态机）

```
DRAFT → PENDING_MFA → PENDING_APPROVAL → APPROVED → QUEUED
  → DISPATCHING → DELIVERED → ACCEPTED → RUNNING
     ├─ SUCCEEDED   ├─ FAILED          ├─ TIMEOUT_KILLED
     ├─ CANCELLED   └─ LOST / UNKNOWN
```

- **DELIVERED**=Connector 已写入 agent 控制流；**ACCEPTED**=agent 验签/策略/资源检查通过；**RUNNING**=任务真正启动——三段确认分别回传；
- `REJECTED_BUSY`（agent 队列满）**不是终态**→ 回 QUEUED 延迟重试（有上限）；`REJECTED_POLICY` 为终态；
- agent 断线导致结果不明 → `UNKNOWN`，**写操作类不自动重试**；
- `cmd_command` 只存当前状态快照；全生命周期迁移写 **`cmd_event` 追加表**。

### 4.3 事务 Outbox 与信封签发时机（v0.2 P0 修正）

**问题**：v0.1 审批通过即生成信封，目标离线数小时后信封早已过期；且"库已审批"与"已投递"之间无事务保护。

**修正**：

```
审批完成时(一个事务内):                    实际投递时(Dispatcher):
  冻结命令规格:                              1. 读 cmd_outbox(DISPATCH_REQUESTED)
    action/params/artifact/                  2. 查 Session Directory 取当前会话
    target_snapshot/policy_version/          3. 调 Signer: 按冻结规格+当前 asset_id
    approval_chain_hash                         签 1~5 分钟短期信封
  → cmd_command: APPROVED→QUEUED             4. 写 cmd_dispatch_attempt
  → 插 cmd_outbox  ──同事务提交──            5. 推送 Connector(expected session/epoch)
                                             6. 失败/过期: attempt+1 重新签发
```

- 信封永不入库 JSONB 当签名原文：库存 `command_spec_bytes(BYTEA)` + `command_spec_sha256` + 每次投递的 `envelope_bytes/envelope_signature/signing_key_id/envelope_expires_at`（在 attempt 表）；JSONB 仅展示用；
- **批量任务目标快照冻结**：审批时把目标解析为 `cmd_batch_target` 明细行，信封绑定 `target_snapshot_sha256 + target_count`——审批 1000 台，执行时 CMDB 变成 1200 台，多的 200 台不在授权内；百分比灰度用确定性算法 `hash(batch_id+asset_id) % 10000 < pct*100`，暂停/恢复/审计不漂移。

### 4.4 IAM/MFA/审批（v0.3：全自研定案）

**v0.3 决策 D3**：部门环境无 OIDC/统一身份设施，且按讨论结论不走 Keycloak 路线——**IAM 按 §5.1 RBAC 模型全自研（Go）**，范围收敛为最小必要集：

- **账号认证**：本地账号 + 口令（bcrypt/argon2id 哈希）、密码策略、失败锁定、会话管理（登录 token，§5.1 `login_session`）；AD/LDAP 对接预留适配接口，二期评估；
- **TOTP 第二因素**（RFC 6238）：操作人自助绑定（种子 AES-GCM 加密存 `op_user.totp_secret_enc`）；登录可选强制、**高危指令强制 Step-up**；
- **RBAC + 资源范围**：

```
用户A: 范围=dc1 ∧ biz=order-service, 动作=diagnose.*, 禁 script.run, 批量上限100台
```

表结构：`role / permission / user_role / role_permission / resource_scope / user_scope`（§5.1）。

**MFA 一次性绑定具体操作**（防验证结果复用）：

- 高危指令触发 Step-up TOTP：`bound_digest = SHA256(action+params+artifact+target_snapshot+timeout)`，验证与指令内容强绑定；
- 验证成功**一次性消费**（`consumed_at`），命令内容变更 MFA 自动失效；
- 高危命令：操作人 Step-up + 审批人 Step-up；approver ≠ operator；双审批人互不相同；**admin 不默认绕过审批与审计**。

**签名密钥体系（v0.3 决策 D2：内置自建 KMS）**：

- 部门无 KMS/HSM 设施 → **自建轻量 KMS**（Signer 服务内置密钥保管模块）：主密钥（KEK）双人分段保管 + 阈值合成（或口令分片），工作私钥（Ed25519）由 KEK 加密存储，运行期解密驻留 Signer 进程内存，不落明文盘；密钥轮换、使用全审计；**预留 HSM/KMS 适配接口**（PKCS#11 / KMS API），生产环境若有合规要求可切换到硬件方案；
- 六类密钥分离：agent 设备 CA / Connector 服务 CA / 平台内部服务 CA / **Command Envelope Signing Key** / Artifact Signing Key / Audit Anchor Key；
- Command Service 不持有私钥文件，只调 Signer 服务；Signer 接口不接受任意字节，仅接受已审批 `cmd_id`；
- 私钥泄露 = 攻击者获得 20 万台远程命令能力——**这是 P0 级控制点**。

### 4.5 注册、证书与制品链路（v0.2 补齐）

**首次注册经 Gateway VIP**（补齐网络路径）：agent 首启无设备证书，走同一 VIP:443、**不同 SNI**：

| SNI | 服务 |
|---|---|
| `agent-bootstrap.<cell>` | 注册/证书轮换（server-auth TLS + bootstrap token） |
| `agent-gateway.<cell>` | mTLS 控制/指标/可靠流 |
| `agent-artifact.<cell>` | 制品下载（mTLS） |
| `agent-result.<cell>` | 大结果上传（mTLS） |

与"生产服务器只出站到 Gateway VIP:443"防火墙原则一致，制品缓存访问路径同时补齐。

**注册幂等**（v0.2）：`RegisterRequest` 携带 `enrollment_request_id + bootstrap_batch_id + csr_public_key_sha256 + request_nonce`；Register Service 存幂等记录——响应丢失后 agent 重试**返回原 asset_id/证书/网关地址**，不重复建资产、不重复消耗 token、不重复签身份。

**证明材料定位修正**：bootstrap token 才是首次注册信任根；machine-id/MAC/SN 是**冲突检测与风险评分材料**，可伪造、可因模板/厂商默认值重复——不设绝对唯一约束，过滤 `To Be Filled By O.E.M.`/空 SN/全零 MAC；machine-id 重复但 SN/MAC 不同多为克隆，走规则引擎而非一律人工审核。

**PKI 自研内置（v0.3 决策 D2）**：部门无现成 CA → 平台内置 PKI 服务（Go 自建：根 CA 离线保管、中间 CA 在线签发，支持 CRL/OCSP 或短证书+吊销清单分发）；证书生命周期：轮换 = 旧证书 mTLS 连接 → 提交新 CSR → 签发新证书 → 新旧短时重叠 → 新证书连通 → 吊销旧证书；Connector 本地缓存证书状态，不同步查库；吊销传播 SLO ≤5 分钟。

### 4.6 CMDB 与资产数据流（v0.3：新建轻量库定案）

**v0.3 决策 D1**：部门无 CMDB → 按 §5.3 模型**新建轻量 CMDB**（ PostgreSQL `asset` 库 + 消费服务 + 查询 API），作为管控平台内建组件；对外预留标准资产同步接口，未来若接入企业级 CMDB 可降级为适配层。

- inventory 全文（Protobuf 原文）**存对象存储**，PostgreSQL 只存索引 + payload_ref + 处理状态（v0.2：20 万台 × 每日全量 × 180 天 ≈ 720GB JSONB，不可入库）；
- 资产状态拆四维（v0.2）：`lifecycle_status`（provisioning/active/maintenance/decommissioned）/ `connectivity_status`（online/offline/unknown）/ `health_status` / `trust_status`（trusted/quarantined/revoked）——"离线"不再覆盖"已下线"；
- **进程信息**（已确认决策）：agent 具备采集能力、`processes.upload_enabled=false` 默认关；存储表缓建，且未来主键必须含 `boot_id + pid + process_start_time`（PID 会复用）；
- 拓扑标签使用时序库只写**稳定标签**（asset_id/cell_id/idc/arch/os/cluster）；owner/值班组/工单等高变标签留 CMDB 查询侧，防时序基数爆炸（v0.2）。

### 4.7 审计中心（v0.2 修订）

- 全局单链哈希链在多消费者并发下需全局串行化，不可扩展 → 改为**分片链**：`chain_id`（按 topic partition 或 asset_id 分片）+ `sequence + prev_hash + entry_hash`；
- 每分钟/每批构造 **Merkle Root** 写外部 WORM/对象锁存储（`audit_anchor` 表记录锚定点）；
- PostgreSQL 只追加权限防不住超级管理员——**审计原文最终归 WORM 存储**，数据库仅为热检索层。

### 4.8 制品与升级（沿用 v0.1，补充）

- 制品/大结果均走区域端点（§4.5 SNI 表）；小结果（head/tail + exit_code）进 Kafka，大结果 agent 直传 Result Blob，Kafka 只传 `result_ref + sha256 + size`（v0.2：2MB 级输出不进 Kafka）；
- 制品增加安全扫描记录表（`artifact_scan_result`）；
- 升级批次沿用：冻结目标快照（同 §4.3）、灰度、健康观察窗、失败熔断。

---

## 5. 数据模型与表结构

数据库：**PostgreSQL 16/17**（v0.2 修正：13 已停止官方支持；升级策略纳入平台生命周期）。库按域分：`iam`、`cmd`、`asset`、`audit`、`infra`。

### 5.1 IAM 域（库 `iam`，v0.2 RBAC 化）

```sql
-- 操作人(v0.3 D3: 全自研 IAM, 账号口令与 TOTP 自管; AD/LDAP 对接预留)
CREATE TABLE op_user (
    user_id         BIGSERIAL PRIMARY KEY,
    username        VARCHAR(64)  NOT NULL UNIQUE,
    display_name    VARCHAR(128) NOT NULL,
    password_hash   VARCHAR(128) NOT NULL,            -- argon2id/bcrypt
    totp_secret_enc BYTEA,                            -- AES-GCM 加密的 TOTP 种子(未绑定为 NULL)
    totp_bound_at   TIMESTAMPTZ,
    failed_attempts SMALLINT NOT NULL DEFAULT 0,      -- 登录失败计数(锁定用)
    locked_until    TIMESTAMPTZ,
    status          VARCHAR(16)  NOT NULL DEFAULT 'active',
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE role (
    role_id     BIGSERIAL PRIMARY KEY,
    role_name   VARCHAR(64) NOT NULL UNIQUE,        -- viewer/operator/approver/admin/自定义
    description VARCHAR(256)
);

CREATE TABLE permission (
    permission_id BIGSERIAL PRIMARY KEY,
    perm_code     VARCHAR(64) NOT NULL UNIQUE,      -- command.create / command.approve / batch.manage / ...
    description   VARCHAR(256)
);

CREATE TABLE user_role (
    user_id BIGINT NOT NULL REFERENCES op_user(user_id),
    role_id BIGINT NOT NULL REFERENCES role(role_id),
    PRIMARY KEY(user_id, role_id)
);

CREATE TABLE role_permission (
    role_id       BIGINT NOT NULL REFERENCES role(role_id),
    permission_id BIGINT NOT NULL REFERENCES permission(permission_id),
    PRIMARY KEY(role_id, permission_id)
);

-- 资源范围: 动作模式/Cell/业务/批量上限
CREATE TABLE resource_scope (
    scope_id        BIGSERIAL PRIMARY KEY,
    scope_name      VARCHAR(64) NOT NULL UNIQUE,
    cell_ids        TEXT[],                          -- NULL=不限
    biz_systems     TEXT[],
    action_patterns TEXT[] NOT NULL,                 -- ['diagnose.*'] 或 ['*']
    deny_actions    TEXT[] DEFAULT '{}',             -- ['script.run']
    max_batch_size  INT NOT NULL DEFAULT 100
);

CREATE TABLE user_scope (
    user_id  BIGINT NOT NULL REFERENCES op_user(user_id),
    scope_id BIGINT NOT NULL REFERENCES resource_scope(scope_id),
    PRIMARY KEY(user_id, scope_id)
);

-- MFA 挑战(一次性,绑定具体操作摘要)
CREATE TABLE mfa_challenge (
    challenge_id    BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES op_user(user_id),
    scene           VARCHAR(32) NOT NULL,            -- command / approval
    ref_id          VARCHAR(64) NOT NULL,            -- cmd_id / approval_id
    challenge_nonce VARCHAR(64) NOT NULL,
    bound_digest    CHAR(64) NOT NULL,               -- SHA256(操作内容),变更即失效
    verified_at     TIMESTAMPTZ,
    consumed_at     TIMESTAMPTZ,                     -- 一次性消费
    attempts        SMALLINT NOT NULL DEFAULT 0,
    locked_until    TIMESTAMPTZ,
    source_ip       INET,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_mfa_user ON mfa_challenge(user_id, scene, created_at DESC);
```

### 5.2 指令域（库 `cmd`，v0.2 大幅修订）

```sql
-- 指令主表(当前状态快照; 历史在 cmd_event)
CREATE TABLE cmd_command (
    cmd_id                UUID PRIMARY KEY,
    batch_id              UUID,
    asset_id              VARCHAR(64) NOT NULL,
    cell_id               VARCHAR(32) NOT NULL,
    action_id             VARCHAR(64) NOT NULL,
    params                JSONB NOT NULL DEFAULT '{}',   -- 展示用
    artifact_id           VARCHAR(64),
    command_spec_bytes    BYTEA,                          -- 冻结的规范化命令规格(签名原文)
    command_spec_sha256   CHAR(64),
    risk_level            VARCHAR(8) NOT NULL,
    mfa_level_req         SMALLINT NOT NULL DEFAULT 1,
    operator_id           BIGINT NOT NULL,
    approval_chain_hash   CHAR(64),
    status                VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    timeout_sec           INT NOT NULL DEFAULT 60,
    expires_at            TIMESTAMPTZ NOT NULL,          -- 指令整体有效期
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_cmd_asset_time ON cmd_command(asset_id, created_at DESC);
CREATE INDEX idx_cmd_active ON cmd_command(status)
    WHERE status IN ('QUEUED','DISPATCHING','DELIVERED','ACCEPTED','RUNNING','UNKNOWN');
CREATE INDEX idx_cmd_batch ON cmd_command(batch_id) WHERE batch_id IS NOT NULL;

-- 指令生命周期事件(仅追加)
CREATE TABLE cmd_event (
    event_id    BIGSERIAL PRIMARY KEY,
    cmd_id      UUID NOT NULL,
    event_type  VARCHAR(32) NOT NULL,    -- created/mfa_verified/approved/dispatch_requested/
                                         -- delivered/accepted/running/result_received/...
    from_status VARCHAR(20),
    to_status   VARCHAR(20),
    actor       VARCHAR(64),
    detail      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_cmdevent_cmd ON cmd_event(cmd_id, event_id);

-- 事务 Outbox(状态迁移与投递请求同事务)
CREATE TABLE cmd_outbox (
    event_id      BIGSERIAL PRIMARY KEY,
    cmd_id        UUID NOT NULL,
    event_type    VARCHAR(32) NOT NULL,      -- DISPATCH_REQUESTED / CANCEL_REQUESTED
    payload_bytes BYTEA NOT NULL,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INT NOT NULL DEFAULT 0,
    locked_by     VARCHAR(64),
    locked_until  TIMESTAMPTZ,
    published_at  TIMESTAMPTZ,
    last_error    VARCHAR(256)
);
CREATE INDEX idx_outbox_pending ON cmd_outbox(available_at) WHERE published_at IS NULL;

-- 投递尝试(每次签发短期信封的记录)
CREATE TABLE cmd_dispatch_attempt (
    attempt_id          BIGSERIAL PRIMARY KEY,
    cmd_id              UUID NOT NULL,
    attempt_no          INT NOT NULL,
    session_id          VARCHAR(64),
    session_epoch       BIGINT,
    connector_id        VARCHAR(64),
    envelope_bytes      BYTEA NOT NULL,      -- 短期信封(签名原文)
    envelope_signature  BYTEA NOT NULL,
    signing_key_id      VARCHAR(64) NOT NULL,
    envelope_expires_at TIMESTAMPTZ NOT NULL,
    status              VARCHAR(20) NOT NULL, -- sent/delivered/accepted/rejected/expired/failed
    sent_at             TIMESTAMPTZ,
    delivered_at        TIMESTAMPTZ,
    accepted_at         TIMESTAMPTZ,
    error_code          VARCHAR(32),
    UNIQUE(cmd_id, attempt_no)
);

-- 指令结果(大输出走对象存储)
CREATE TABLE cmd_result (
    cmd_id        UUID PRIMARY KEY REFERENCES cmd_command(cmd_id),
    stdout_head   TEXT,                     -- 前 4KB 内联速览
    stderr_head   TEXT,
    result_ref    VARCHAR(256),             -- 大结果对象存储 key
    result_sha256 CHAR(64),
    result_size   BIGINT,
    truncated     BOOLEAN NOT NULL DEFAULT FALSE,
    kill_reason   VARCHAR(32),
    duration_ms   INT,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 审批
CREATE TABLE cmd_approval (
    approval_id     BIGSERIAL PRIMARY KEY,
    cmd_id          UUID NOT NULL REFERENCES cmd_command(cmd_id),
    step            SMALLINT NOT NULL,
    approver_id     BIGINT NOT NULL,
    decision        VARCHAR(8),
    mfa_verified    BOOLEAN NOT NULL DEFAULT FALSE,   -- 审批人 Step-up
    comment         VARCHAR(512),
    decided_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(cmd_id, step),
    CHECK (decision IS NULL OR decision IN ('approve','reject'))
);

-- 批量任务
CREATE TABLE cmd_batch (
    batch_id              UUID PRIMARY KEY,
    name                  VARCHAR(128) NOT NULL,
    action_id             VARCHAR(64) NOT NULL,
    target_scope          JSONB NOT NULL,          -- 原始范围表达式(展示)
    target_snapshot_sha256 CHAR(64),               -- 冻结快照摘要(审批绑定)
    target_count          INT,
    concurrency           INT NOT NULL DEFAULT 100,
    interval_sec          INT NOT NULL DEFAULT 60,
    fuse_fail_pct         SMALLINT NOT NULL DEFAULT 5,
    status                VARCHAR(16) NOT NULL DEFAULT 'DRAFT',
    success_count         INT DEFAULT 0,
    fail_count            INT DEFAULT 0,
    created_by            BIGINT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 批量目标快照(审批时冻结,执行不漂移)
CREATE TABLE cmd_batch_target (
    batch_id     UUID NOT NULL REFERENCES cmd_batch(batch_id),
    asset_id     VARCHAR(64) NOT NULL,
    cell_id      VARCHAR(32) NOT NULL,
    wave_no      INT NOT NULL,                      -- 波次
    target_order INT NOT NULL,                      -- 确定性排序: hash(batch_id+asset_id)
    state        VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    last_cmd_id  UUID,
    PRIMARY KEY(batch_id, asset_id)
);

-- 幂等键(管控台/API 防重复创建)
CREATE TABLE cmd_idempotency (
    idem_key    VARCHAR(128) PRIMARY KEY,           -- operator 提供的请求幂等键
    cmd_id      UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 动作目录
CREATE TABLE policy_action (
    action_id       VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    category        VARCHAR(32) NOT NULL,
    risk_level      VARCHAR(8) NOT NULL,
    mfa_level_req   SMALLINT NOT NULL DEFAULT 1,
    need_approval   BOOLEAN NOT NULL DEFAULT FALSE,
    param_schema    JSONB NOT NULL,
    default_timeout INT NOT NULL DEFAULT 60,
    max_timeout     INT NOT NULL DEFAULT 300,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    policy_version  VARCHAR(16) NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 制品 + 扫描记录
CREATE TABLE artifact (
    artifact_id  VARCHAR(64) PRIMARY KEY,
    name         VARCHAR(128) NOT NULL,
    version      VARCHAR(64) NOT NULL,
    kind         VARCHAR(16) NOT NULL,
    sha256       CHAR(64) NOT NULL,
    size_bytes   BIGINT NOT NULL,
    signature    TEXT NOT NULL,
    signing_key_id VARCHAR(64) NOT NULL,
    storage_path VARCHAR(256) NOT NULL,
    status       VARCHAR(16) NOT NULL DEFAULT 'active',
    uploaded_by  BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(name, version)
);
CREATE TABLE artifact_scan_result (
    scan_id     BIGSERIAL PRIMARY KEY,
    artifact_id VARCHAR(64) NOT NULL REFERENCES artifact(artifact_id),
    scanner     VARCHAR(64) NOT NULL,
    result      VARCHAR(16) NOT NULL,      -- pass/warn/fail
    report_ref  VARCHAR(256),
    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 升级批次(结构同 cmd_batch 思路,目标快照冻结,略)
CREATE TABLE upgrade_batch (
    batch_id      UUID PRIMARY KEY,
    artifact_id   VARCHAR(64) NOT NULL REFERENCES artifact(artifact_id),
    from_version  VARCHAR(32),
    to_version    VARCHAR(32) NOT NULL,
    target_snapshot_sha256 CHAR(64),
    target_count  INT,
    concurrency   INT NOT NULL DEFAULT 50,
    observe_sec   INT NOT NULL DEFAULT 300,
    fuse_fail_pct SMALLINT NOT NULL DEFAULT 5,
    status        VARCHAR(16) NOT NULL DEFAULT 'DRAFT',
    success_count INT DEFAULT 0,
    fail_count    INT DEFAULT 0,
    created_by    BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 5.3 资产域（库 `asset`，v0.2 修订）

```sql
-- 资产主表(状态四维拆分)
CREATE TABLE cmdb_asset (
    asset_id           VARCHAR(64) PRIMARY KEY,
    hostname           VARCHAR(128) NOT NULL,
    machine_id         VARCHAR(64),
    boot_id            VARCHAR(64),
    board_sn           VARCHAR(64),
    primary_mac        MACADDR,
    arch               VARCHAR(16) NOT NULL,
    os_distro          VARCHAR(32) NOT NULL,
    os_version         VARCHAR(64) NOT NULL,
    kernel_version     VARCHAR(64),
    cpu_model          VARCHAR(128),
    cpu_cores          SMALLINT,
    mem_total_mb       BIGINT,
    agent_version      VARCHAR(32),
    cell_id            VARCHAR(32) NOT NULL,
    idc                VARCHAR(32),
    cluster            VARCHAR(64),
    biz_system         VARCHAR(64),
    owner              VARCHAR(64),
    lifecycle_status   VARCHAR(16) NOT NULL DEFAULT 'provisioning', -- provisioning/active/maintenance/decommissioned
    connectivity_status VARCHAR(10) NOT NULL DEFAULT 'unknown',     -- online/offline/unknown
    health_status      VARCHAR(10) NOT NULL DEFAULT 'unknown',      -- healthy/degraded/unhealthy
    trust_status       VARCHAR(12) NOT NULL DEFAULT 'trusted',      -- trusted/quarantined/revoked
    registered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_asset_hostname ON cmdb_asset(hostname);
CREATE INDEX idx_asset_cell ON cmdb_asset(cell_id, connectivity_status);
CREATE INDEX idx_asset_biz  ON cmdb_asset(biz_system) WHERE biz_system IS NOT NULL;

-- 身份材料(风险评分材料,非绝对唯一; v0.2 重构)
CREATE TABLE asset_identity (
    id              BIGSERIAL PRIMARY KEY,
    asset_id        VARCHAR(64) NOT NULL REFERENCES cmdb_asset(asset_id),
    id_type         VARCHAR(16) NOT NULL,        -- machine_id/board_sn/mac/disk_sn
    id_value        VARCHAR(128) NOT NULL,
    normalized_value VARCHAR(128) NOT NULL,      -- 归一化(去OEM占位值等)
    confidence      SMALLINT NOT NULL DEFAULT 50, -- 0-100 可信度
    verified        BOOLEAN NOT NULL DEFAULT FALSE,
    binding_status  VARCHAR(12) NOT NULL DEFAULT 'active',  -- active/superseded/rejected
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 仅高可信且有效的材料建部分唯一约束
CREATE UNIQUE INDEX uq_identity_verified ON asset_identity(id_type, normalized_value)
    WHERE verified AND binding_status = 'active' AND confidence >= 80;
CREATE INDEX idx_identity_lookup ON asset_identity(id_type, normalized_value);

-- 注册幂等记录(v0.2 新增)
CREATE TABLE register_enrollment (
    enrollment_request_id UUID PRIMARY KEY,      -- agent 生成,重试不变
    bootstrap_batch_id    VARCHAR(32) NOT NULL,
    csr_pubkey_sha256     CHAR(64) NOT NULL,
    request_nonce         VARCHAR(64) NOT NULL,
    asset_id              VARCHAR(64),            -- 签发后回填
    cert_id               BIGINT,
    status                VARCHAR(16) NOT NULL DEFAULT 'processing', -- processing/completed/failed
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ
);

-- 身份冲突队列
CREATE TABLE asset_conflict (
    conflict_id    BIGSERIAL PRIMARY KEY,
    id_type        VARCHAR(16) NOT NULL,
    id_value       VARCHAR(128) NOT NULL,
    exist_asset_id VARCHAR(64) NOT NULL,
    new_material   JSONB NOT NULL,
    resolution     VARCHAR(16),
    resolved_by    BIGINT,
    resolved_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 引导凭据批次
CREATE TABLE register_bootstrap (
    batch_id    VARCHAR(32) PRIMARY KEY,
    token_hash  CHAR(64) NOT NULL,
    scope       JSONB NOT NULL,
    max_uses    INT NOT NULL,
    used_count  INT NOT NULL DEFAULT 0,
    expires_at  TIMESTAMPTZ NOT NULL,
    status      VARCHAR(16) NOT NULL DEFAULT 'active',
    created_by  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 设备证书(v0.2 生命周期补全)
CREATE TABLE agent_certificate (
    cert_id            BIGSERIAL PRIMARY KEY,
    asset_id           VARCHAR(64) NOT NULL REFERENCES cmdb_asset(asset_id),
    serial_no          VARCHAR(64) NOT NULL UNIQUE,
    fingerprint        CHAR(64) NOT NULL UNIQUE,
    issuer_id          VARCHAR(64) NOT NULL,
    key_id             VARCHAR(64),
    not_before         TIMESTAMPTZ NOT NULL,
    not_after          TIMESTAMPTZ NOT NULL,
    status             VARCHAR(16) NOT NULL DEFAULT 'active',
    revoked_at         TIMESTAMPTZ,
    revocation_reason  VARCHAR(64),
    replaced_by_cert_id BIGINT,
    last_seen_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_cert_asset ON agent_certificate(asset_id, status);

-- 证书吊销列表(Connector 同步缓存)
CREATE TABLE certificate_revocation (
    serial_no   VARCHAR(64) PRIMARY KEY,
    revoked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason      VARCHAR(64)
);

-- 资产上报索引(全文在对象存储; v0.2 修复分区表主键)
CREATE TABLE inventory_report (
    report_id      BIGSERIAL,
    asset_id       VARCHAR(64) NOT NULL,
    revision       BIGINT NOT NULL,           -- agent 侧递增
    report_type    VARCHAR(8) NOT NULL,       -- full/delta
    schema_version INT NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    payload_ref    VARCHAR(256) NOT NULL,     -- 对象存储 key
    payload_size   BIGINT NOT NULL,
    process_status VARCHAR(16) NOT NULL DEFAULT 'pending',  -- pending/processed/failed
    processed_at   TIMESTAMPTZ,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (received_at, report_id)      -- v0.2: 分区键必须在主键中
) PARTITION BY RANGE (received_at);
-- 运维要求: 当月分区 + 下月预建分区 + DEFAULT 分区 + 过期归档任务(自动化)
CREATE INDEX idx_inv_asset ON inventory_report(asset_id, received_at DESC);

-- 资产当前态: 网卡与 IP 规范化(v0.2 拆分)
CREATE TABLE asset_interface (
    asset_id    VARCHAR(64) NOT NULL REFERENCES cmdb_asset(asset_id),
    name        VARCHAR(32) NOT NULL,
    mac         MACADDR,
    speed_mbps  INT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, name)
);
CREATE TABLE asset_ip_address (
    asset_id    VARCHAR(64) NOT NULL,
    iface_name  VARCHAR(32) NOT NULL,
    family      SMALLINT NOT NULL,           -- 4/6
    ip          INET NOT NULL,
    prefix_len  SMALLINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, iface_name, family, ip)
);
CREATE INDEX idx_assetip_ip ON asset_ip_address(ip);   -- 按 IP 反查资产/冲突检测

-- 资产当前态: 挂载
CREATE TABLE asset_mount (
    asset_id    VARCHAR(64) NOT NULL REFERENCES cmdb_asset(asset_id),
    mount_point VARCHAR(256) NOT NULL,
    device      VARCHAR(128),
    fstype      VARCHAR(32),
    total_gb    NUMERIC(12,2),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, mount_point)
);

-- 资产当前态: 软件包(v0.2 主键含版本与架构)
CREATE TABLE asset_package (
    asset_id   VARCHAR(64) NOT NULL REFERENCES cmdb_asset(asset_id),
    pkg_name   VARCHAR(128) NOT NULL,
    pkg_version VARCHAR(128) NOT NULL,
    pkg_arch   VARCHAR(16) NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, pkg_name, pkg_version, pkg_arch)
);

-- 资产当前态: 进程清单【缓建/二期启用】
-- 决策: agent 先具备采集能力, 上送默认关闭(processes.upload_enabled=false)。
-- v0.2 修正: PID 会复用, 未来主键必须 = (asset_id, boot_id, pid, process_start_time)
-- CREATE TABLE asset_process_latest (
--     asset_id VARCHAR(64) NOT NULL,
--     boot_id  VARCHAR(64) NOT NULL,
--     pid      INT NOT NULL,
--     process_start_time TIMESTAMPTZ NOT NULL,
--     ppid INT, username VARCHAR(32), cmdline TEXT, listen_ports INT[],
--     snapshot_at TIMESTAMPTZ NOT NULL,
--     PRIMARY KEY(asset_id, boot_id, pid, process_start_time)
-- );
```

### 5.4 基础设施域（库 `infra`，v0.2 修订）

```sql
CREATE TABLE cell (
    cell_id         VARCHAR(32) PRIMARY KEY,
    idc             VARCHAR(32) NOT NULL,
    description     VARCHAR(256),
    telemetry_kafka VARCHAR(256) NOT NULL,
    critical_kafka  VARCHAR(256) NOT NULL,      -- v0.2: 两集群分离
    status          VARCHAR(16) NOT NULL DEFAULT 'active'
);

CREATE TABLE gateway_node (
    gateway_id     VARCHAR(64) PRIMARY KEY,     -- Connector 实例
    cell_id        VARCHAR(32) NOT NULL REFERENCES cell(cell_id),
    max_conns      INT NOT NULL DEFAULT 10000,
    version        VARCHAR(32),
    status         VARCHAR(16) NOT NULL DEFAULT 'active',  -- active/draining/down
    last_heartbeat TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 注: current_conns 移出关系库,走监控系统(v0.2: 避免秒级刷库)

-- agent 会话快照(实时态在 Redis,本表分钟级快照; v0.2 增加 epoch)
CREATE TABLE agent_session_snapshot (
    asset_id       VARCHAR(64) PRIMARY KEY,
    cell_id        VARCHAR(32),
    connector_id   VARCHAR(64),
    session_id     VARCHAR(64),
    session_epoch  BIGINT,
    connected_at   TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    boot_id        VARCHAR(64),
    agent_version  VARCHAR(32),
    cert_serial    VARCHAR(64),
    disconnect_reason VARCHAR(64),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Redis 实时结构(v0.2 CAS + fencing):
--   SESS:{asset_id} = {cell_id, connector_id, session_id, session_epoch,
--                      connected_at, last_heartbeat, boot_id, agent_version,
--                      protocol_version, cert_serial, source_ip, config_version}
--   TTL = 3 × heartbeat_interval
--   注册: Lua 脚本原子递增 epoch 并写入; 向旧 connector 发送 fence;
--   下推: 携带 expected_session_id/epoch, Connector 本地比对不符即 NACK;
--   重复连接: 默认新连接获新 epoch、旧连接断开, 同时产生高优先级安全事件
--             (同 asset 不同 IP/Cell/boot_id 同时在线 = 可疑);
--   心跳只刷 Redis TTL, 不写 Kafka/PostgreSQL; Kafka 只记上下线/版本/证书/Cell 变化事件。
```

### 5.5 配置下发（库 `infra`，v0.2 新增 desired/observed 模型）

```sql
CREATE TABLE agent_config_profile (
    profile_id   BIGSERIAL PRIMARY KEY,
    name         VARCHAR(64) NOT NULL,
    config_body  JSONB NOT NULL,           -- agent.yaml 同构子集
    version      INT NOT NULL,
    signature    TEXT NOT NULL,            -- 配置签名,agent 校验
    status       VARCHAR(16) NOT NULL DEFAULT 'active',
    created_by   BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(name, version)
);

CREATE TABLE agent_config_assignment (
    assignment_id BIGSERIAL PRIMARY KEY,
    profile_id    BIGINT NOT NULL REFERENCES agent_config_profile(profile_id),
    scope         JSONB NOT NULL,           -- {asset_ids:[]} 或 {cells:[], percent:N(确定性hash)}
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE agent_config_status (           -- observed 状态(agent 应用后回执)
    asset_id       VARCHAR(64) NOT NULL,
    profile_id     BIGINT NOT NULL,
    applied_version INT NOT NULL,
    apply_status   VARCHAR(16) NOT NULL,    -- applied/failed/rollback
    error          VARCHAR(256),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, profile_id)
);
```

### 5.6 审计域（库 `audit`，v0.2 分片链 + Merkle 锚定）

```sql
-- 仅追加(数据库权限强制 REVOKE UPDATE/DELETE)
CREATE TABLE audit_log (
    audit_id   BIGSERIAL,
    chain_id   VARCHAR(32) NOT NULL,        -- 分片: topic_partition 或 asset shard
    sequence   BIGINT NOT NULL,             -- 分片内序号
    source     VARCHAR(16) NOT NULL,
    event_type VARCHAR(48) NOT NULL,
    actor      VARCHAR(64),
    asset_id   VARCHAR(64),
    cmd_id     UUID,
    detail     JSONB NOT NULL,
    prev_hash  CHAR(64) NOT NULL,           -- 分片内链
    entry_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(created_at, audit_id),
    UNIQUE(chain_id, sequence)
) PARTITION BY RANGE (created_at);
CREATE INDEX idx_audit_asset ON audit_log(asset_id, created_at DESC);
CREATE INDEX idx_audit_cmd   ON audit_log(cmd_id) WHERE cmd_id IS NOT NULL;

-- Merkle 锚定(每分钟/批,Root 写外部 WORM/对象锁存储)
CREATE TABLE audit_anchor (
    anchor_id    BIGSERIAL PRIMARY KEY,
    chain_id     VARCHAR(32) NOT NULL,
    seq_from     BIGINT NOT NULL,
    seq_to       BIGINT NOT NULL,
    merkle_root  CHAR(64) NOT NULL,
    worm_ref     VARCHAR(256) NOT NULL,      -- 外部不可变存储位置
    anchor_sig   TEXT NOT NULL,              -- Audit Anchor Key 签名
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 5.7 Kafka 规划（v0.2 两集群物理隔离）

| 集群 | Topic | 内容 | 说明 |
|---|---|---|---|
| **Telemetry**（高吞吐，允许短暂积压） | `aiops.metrics.host.v1.<region>` | 指标批次 | 分区键 asset_id；分区数 **M0 按 32/64/128/256 四档压测定**，公式 `P≥峰值吞吐/单分区安全吞吐` 与 `P≥消费并发` 取大 |
| | `aiops.events.agent.v1.<region>` | 上下线/版本/证书/Cell 变化事件（**不含心跳**，v0.2） | 心跳只刷 Redis TTL |
| **Critical**（低吞吐高可靠，`acks=all`+幂等 producer+严格配额） | `aiops.command.result.v1.<region>` | 指令 ACK/小结果（大结果只传 ref） | |
| | `aiops.audit.agent.v1.<region>` | 审计事件 | |
| | `aiops.inventory.v1.<region>` | 资产报告（全文在对象存储时为索引消息） | |
| | `aiops.security.v1.<region>` | 注册/重复连接/验签失败等安全事件 | |

---

## 6. 关键接口协议（v0.2 P0 修正）

### 6.1 agent ↔ Connector gRPC（方向修正 + 流式 ACK）

```protobuf
service AgentGateway {
  // 控制流: agent 发 AgentControlFrame, 收 GatewayControlFrame (v0.1 方向写反,已修正)
  rpc Control(stream AgentControlFrame) returns (stream GatewayControlFrame);
  // 指标流: 双向流, 逐批/累计 ACK (v0.1 单次响应,agent 无法推进 WAL,已修正)
  rpc Metrics(stream MetricBatch)     returns (stream MetricAck);
  // 可靠流: 结果/审计/inventory, Kafka 持久化后才最终 ACK
  rpc Reports(stream ReliableReport)  returns (stream ReportAck);
}

service AgentBootstrap {   // 经 SNI=agent-bootstrap, server-auth TLS + bootstrap token
  rpc Register(RegisterRequest)             returns (RegisterResponse);
  rpc RotateCertificate(RotateCertRequest)  returns (RotateCertResponse);
}
```

**连接握手（Hello/Welcome，v0.2 新增）**：控制流建立后 agent 首帧必须是 `AgentHello{asset_id, boot_id, agent_version, protocol_min/max_version, supported_schema_versions, supported_compressions, supported_actions(动作目录版本), capabilities, current_config_version, last_report_ack_cursor}`；Connector 回 `GatewayWelcome{session_id, session_epoch, server_time, heartbeat_interval, offline_timeout, max_message_bytes, max_inflight_reports, target_config_version, reconnect_after}`。**未完成握手的连接不得发送指标与结果**；动作目录版本在握手时对齐（评审接口对齐项）。

**下推帧**：`GatewayControlFrame = CommandEnvelope | CancelCommand | ConfigPush(签名配置) | ReconnectHint{reconnect_after} | FenceNotice`；
**上推帧**：`AgentControlFrame = AgentHello | Heartbeat | CommandAck{accepted/rejected} | CommandEvent{running/...} | ConfigApplyAck | Event`。

### 6.2 Connector ↔ 中心（控制隧道，v0.2 新增）

```
Connector 主动出站 ──mTLS 双向流──▶ Dispatch Hub
  上行: SessionDelta(注册/注销/epoch), 心跳摘要, PushResult(ACK/NACK)
  下行: PushCommand{envelope, expected_session_id, expected_session_epoch}, Fence{session_id}
```

中心不向区域开放任何入站端口；中心只维护数十条 Connector 隧道；中心不可达时 Connector 维持 agent 连接与数据缓冲。

### 6.3 管控台 OpenAPI

沿用 v0.1 清单（登录/资产/指令/审批/批次/引导批次/Gateway/审计），补充：
- `POST /api/v1/commands` 请求携带客户端幂等键（落 `cmd_idempotency`）；
- `GET /api/v1/commands/{id}/events` 查生命周期事件流；
- `GET /api/v1/commands/{id}/result` 小结果内联、大结果返回带权限校验的预签名下载 URL（下载记审计）；
- 认证走自研 IAM（口令 + 会话 token；高危操作 Step-up TOTP，§4.4）；管控台后端统一 Go（v0.3 D6）。

---

## 7. 非功能设计

### 7.1 容量规划（v0.2 按 Cell N+1 修正）

v0.1 的"每 Cell ≥3 实例 + 全局 52 台"自相矛盾。修正公式：

```
connector_count_per_cell =
    ceil(peak_online_agents × headroom_factor(1.3) / tested_safe_capacity_per_connector)
    + failure_reserve(≥1)
```

- 例：3 万台 Cell、单实例实测安全容量 10,000 → `ceil(30000×1.3/10000)+1 = 5` 实例；若坚持每 Cell 3 实例，则 M0 必须证明单实例在 1 实例故障后能承载 15,000 连接（即安全容量 ≥20,000）；
- 全局总数只作预算参考，**某 Cell 的空闲容量救不了另一个 Cell**；
- NGINX FD 预算：L4 透传时每 agent 占 2 个 socket（agent↔NGINX、NGINX↔Connector），3 万 agent ≈ 6 万+ FD，`worker_rlimit_nofile`/`worker_connections` 按此配；
- 其他初值：指标入口 ≈40MB/s（每 10s×2KB×20 万）；遥测 Kafka 日逻辑写入 ≈3.5TB、RF=3 物理 ≈10.4TB（broker 磁盘/网卡/重建时间需配套测算，M0 出正式容量模型）；PostgreSQL 单实例足够（指令流水 + 资产），audit/audit_anchor 分区 + WORM 归档。

### 7.2 平台 SLO（v0.2 新增）

| 指标 | 目标 |
|---|---|
| 在线 agent 指令投递延迟 | p99 ≤ 2s |
| agent 离线识别 | ≤ 90–120s |
| Command 状态查询可用性 | ≥ 99.9% |
| 控制流可用性 | ≥ 99.95% |
| Metrics | 有界丢失（按配置明示，不承诺零丢失） |
| Result/Audit/Inventory | 平台确认后不得丢失 |
| 证书吊销传播 | ≤ 5 分钟 |
| 单 Cell 故障 | 不扩散其他 Cell |
| 批量熔断 | 达阈值停新投递，不影响已运行任务 |

### 7.3 安全要点

- 生产服务器网段只能出站到本 Cell VIP:443；Connector 内部端口仅 NGINX 可达；Kafka 仅 Connector/消费服务可达；管控台仅办公网；
- 六类密钥分离（§4.4），Command/Artifact/Audit 签名私钥在 KMS/HSM，Signer 独立服务；
- 重复连接（同 asset 不同 IP/Cell/boot_id）→ 默认新连接替换旧连接 + 高优先级安全事件（策略可配 replace_old/reject_new/quarantine_both）；
- 审计原文归 WORM；MFA 一次性绑定；批量目标快照冻结。

---

### 7.4 基础组件选型定案（v0.3 决策 D5/D6/D7）

| 组件 | 定案 | 说明 |
|---|---|---|
| 时序库 | **VictoriaMetrics**（D5） | 与总体方案数据底座章节对齐；Telemetry Ingestor 写入目标 |
| 平台语言栈 | **统一 Golang**（D6） | 管控台后端、IAM、Command、Signer、Register、CMDB、Connector、Ingestor 全部 Go；管控台前端页面由 Go 服务托管（模板或嵌入静态资源） |
| 对象存储 | **S3 兼容抽象 + 配置切换**（D7） | 开发期一套 MinIO 共用（制品/inventory 原文/大结果/审计归档分 bucket）；生产环境换新实例，仅改配置（endpoint/credentials/bucket 前缀），代码零改动；接口层限定 S3 兼容操作子集（Put/Get/Delete/Head + 预签名 URL） |
| 关系库 | PostgreSQL 16/17 | §5 全部表结构 |
| 缓存/会话 | Redis 7（哨兵或集群） | Session Directory 等 |

## 8. 与 agent-design v0.2 的接口对齐清单（v0.2 更新）

| 主题 | agent 侧 | 平台侧 | 状态 |
|---|---|---|---|
| 身份注册 | 首启注册、幂等重试 | §4.5 Bootstrap SNI + `register_enrollment` | ✅ 已对齐 |
| 上行通道 | mTLS gRPC 三流 | §6.1（方向与流式 ACK 已修正） | ✅ 已对齐 |
| 握手协商 | Hello/能力/动作目录版本 | §6.1 Hello/Welcome | ✅ 新增对齐 |
| 指令信封 | 验签、持久化防重放 | §4.3 Outbox + 投递时签短期信封 + attempt | ✅ 已对齐 |
| 会话语义 | 断线重连、ReconnectHint | session_epoch + fencing + 排空流程 | ✅ 新增对齐 |
| 资产上报 | InventoryReport，传输 ACK ≠ 业务 ACK | §3.4 ACK 边界 + §5.3 对象存储 | ✅ 已对齐 |
| 配置下发 | ConfigPush + 应用回执 | §5.5 desired/observed + 配置签名 | ✅ 新增对齐 |
| 制品/大结果 | 制品下载、结果上传 | §4.5 同 VIP 四 SNI 端点 | ✅ 补齐路径 |
| 进程上传 | `processes.upload_enabled=false` | 配置字段同名（§5.5 profile） | ✅ 命名统一 |
| HTTPS 长轮询兼容 | agent 备选模式 | 平台侧协议/容量设计 | ⏳ M0 后补 |

---

## 9. 开发顺序（v0.2 按评审 P0–P3 重排）

| 阶段 | 内容 | 说明 |
|---|---|---|
| **P0（冻结 proto/DDL 前）** | NGINX TLS 透传定型；三条 gRPC 修正；Hello/epoch/fencing；两类 ACK 边界；Outbox/Event/DispatchAttempt；投递时签信封；注册幂等；制品/结果网络路径；DDL 修复；**自研 PKI/Signer/KMS 就位（v0.3 D2）** | v0.2/v0.3 设计已定，本轮冻结 |
| P0-M0 | Connector 原型 + NGINX 接入 + 模拟器压测（§9.1 场景） | 对齐 agent M0 |
| P1（百台灰度前） | 证书轮换/吊销传播；RBAC+资源范围；MFA 一次性绑定；批量目标快照；配置 desired/observed；审计 WORM；关键 Topic 幂等 producer；协议版本协商 | 对齐 agent M2–M5 |
| P2（万台前） | Cell 两级 Session Router；Connector 控制隧道完善；Telemetry Ingestor 与基数治理；区域制品/结果缓存；Cell 配额熔断；多 Cell 演练；PG PITR/Kafka 恢复/密钥灾备 | |
| P3（20 万前） | 3 万连接单 Cell N+1 验证；5 万重连风暴；全链路容量模型；多版本兼容分批升级；DR 演练；CA/密钥批量轮换演练 | |

### 9.1 M0 必测场景（评审意见纳入）

单 Cell 3 万长连接；单 Connector 故障剩余实例全承载；3 万同时重连 + 5 万跨 Cell 重连；Redis 全不可用存量连接不受影响；Kafka 中断 30 分钟 + 恢复限速重放 + 中断期间 Connector 重启；Result/Audit/Inventory 零丢失；同一指令重复投递 100 次仅执行一次；执行成功但结果 ACK 丢失；执行后 agent 立即重启；新旧 session 同时在线（fencing）；优雅排空；证书吊销与轮换；RegisterResponse 丢失重试（幂等）；20 万批次目标快照；批量失败熔断；签名密钥轮换；agent N-2 版本协议兼容。

---

## 10. 开放问题（v0.3：已全部决策，见 §12 决策记录）

无待决项。proto 与 DDL 可冻结 v1.0，进入开发阶段。后续专题（不影响主干开发）：

1. **进程信息存储模型**（已确认缓建）：上送频率、保留周期、是否入图库——二期专题讨论；
2. **AD/LDAP 对接**：自研 IAM 预留适配接口，是否对接二期评估；
3. **申威平台**：存量确认后决定是否投入特殊构建链（agent 侧问题）。

---

## 11. 外部评审意见评估结论（v0.2 采纳记录）

对 `platform-architecture-chatgpt.md` 的逐条评估（10 项 P0 全部成立，P1/P2 建议全部采纳排期）：

| # | 评审意见 | 评估 | 处理 |
|---|---|---|---|
| 1 | NGINX stream 无法注入 gRPC 头，默认改 TLS 透传 | **成立（原方案不可实现）** | §3.1 修正，Connector 终止 mTLS |
| 2 | ControlStream 方向写反、MetricsStream 单次 ACK 不可用 | **成立（proto 硬错误）** | §6.1 重写三流 + 流式 ACK |
| 3 | Hello/Welcome 握手、协议与能力协商 | 成立 | §6.1 新增 |
| 4 | 事务 Outbox + cmd_event + dispatch_attempt | 成立 | §4.3/§5.2 新增三表 |
| 5 | 审批冻结命令、投递时签短期信封 | 成立（离线过期问题真实） | §4.3 修正 |
| 6 | 信封不入 JSONB（签名原文序列化不稳定） | 成立 | BYTEA 存规范化原文，§5.2 |
| 7 | session_epoch + Redis CAS fencing + 重复连接安全事件 | 成立 | §5.4/§7.3 |
| 8 | 关键消息 Kafka 持久化后才最终 ACK | 成立（本地落盘≠平台持久化） | §3.4 修正 |
| 9 | Connector 非无状态、排空需 ReconnectHint+期限 | 成立 | §3.2/§3.3 |
| 10 | Connector 主动出站连中心 Dispatch Hub | 成立（防火墙更简单） | §2.2/§6.2 |
| 11 | IAM 不全自研，OIDC + RBAC/资源范围 | 成立 | §4.4/§5.1 重构 |
| 12 | MFA 一次性绑定操作摘要 | 成立（防复用） | §4.4/§5.1 |
| 13 | Signer/KMS 提前到 P0，六类密钥分离 | 成立（私钥=20万台 RCE） | §4.4/§9 |
| 14 | PostgreSQL 13 EOL → 16/17 | 成立 | §5 基线修正 |
| 15 | inventory_report 分区表主键缺分区键 | **成立（DDL 直接失败）** | §5.3 修复 |
| 16 | inventory 全文不入库（720GB）→ 对象存储 | 成立 | §4.6/§5.3 |
| 17 | 资产状态四维拆分 | 成立 | §5.3 |
| 18 | asset_identity 不设绝对唯一，可信度模型 | 成立（证明材料可伪造） | §4.5/§5.3 |
| 19 | 注册幂等、证书生命周期字段 | 成立 | §4.5/§5.3 |
| 20 | NIC/IP 规范化、Package/进程主键修正 | 成立 | §5.3 |
| 21 | 审计全局链不可扩展 → 分片链 + Merkle 锚定 + WORM | 成立 | §4.7/§5.6 |
| 22 | 批量目标快照冻结 + 确定性灰度 | 成立（审批范围漂移是安全问题） | §4.3/§5.2 |
| 23 | 心跳不写 Kafka/PG | 成立（5.76 亿次/天无价值） | §5.4/§5.7 |
| 24 | Telemetry/Critical Kafka 物理隔离 | 成立 | §5.7 |
| 25 | 大结果不进 Kafka，走 Result Blob | 成立 | §4.5/§4.8 |
| 26 | 高变标签不入时序库 | 成立 | §4.6 |
| 27 | 容量按 Cell N+1 公式；NGINX FD 双倍 | 成立（v0.1 数字自相矛盾） | §7.1 修正 |
| 28 | 平台 SLO 与 M0 必测场景 | 成立 | §7.2/§9.1 |
| 29 | 配置 desired/observed + 配置签名 | 成立 | §5.5 新增 |
| 30 | OIDC/接口对齐余项 | 成立 | §8/§10 |

**保留意见**：① "Connector 主动连中心" 与 "中心 push 到 Connector" 两种模式 v0.2 取前者，若 M0 发现隧道维护复杂度过高可回退为中心主动连接（接口不变，仅连接方向变化）；② ~~Keycloak 信创兼容性~~ 已由 v0.3 决策 D3 消解（全自研 IAM）。

---

## 12. v0.3 决策记录（2026-07-19 讨论定案）

| # | 议题 | 决策 |
|---|---|---|
| D1 | CMDB 对接策略 | 部门无 CMDB → **新建轻量 CMDB**（§4.6/§5.3），预留企业级 CMDB 同步接口 |
| D2 | CA 与 KMS 选型 | 无现成设施 → **自研内置 PKI**（Go 自建 CA，§4.5）+ **自研轻量 KMS**（KEK 双人分段保管 + Ed25519 工作密钥加密存储，预留 PKCS#11/HSM 适配，§4.4）；P0 就位 |
| D3 | OIDC 选型 | 不引入 Keycloak → **IAM 全自研**（账号+口令+TOTP+会话+锁定 + RBAC/资源范围 + 审批，§4.4/§5.1）；AD/LDAP 预留 |
| D4 | 进程信息存储 | 维持缓建：agent 具备采集能力、上送默认关闭；存储模型二期专题 |
| D5 | 时序库 | **VictoriaMetrics**（与总体方案数据底座对齐，§7.4） |
| D6 | 管控台技术栈 | **统一 Golang**（§7.4） |
| D7 | 对象存储 | 开发期一套 MinIO 共用；生产换新实例；**S3 兼容抽象 + 配置切换**（§7.4） |

---

*本文档 v0.3 为架构基线。七项开放问题已全部决策，proto 与 DDL 冻结 v1.0，进入开发阶段：agent 侧先行（M1 骨架），平台侧 P0-M0 并行筹备。*
