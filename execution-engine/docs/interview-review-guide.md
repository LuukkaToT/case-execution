# 2-1 面试核心复习地图

这份材料不按目录平均复习，而是围绕最容易被面试官连续追问的五条主线：并发流水线、DB/MQ 一致性、RabbitMQ 可靠发布、幂等与状态竞争、取消与生命周期。

## 一、复习优先级

### P0：必须能脱离代码讲清楚

| 主线 | 必读文件 | 重点位置 |
| --- | --- | --- |
| 批量并发流水线 | `internal/usecase/dispatch_app_service.go` | `dispatchStream`、`dispatchOneBatch`、`completeBatchResult` |
| DB/MQ 一致性 | `internal/usecase/outbox_relay.go` | `DispatchOnce`、重试退避、成功/重试/DEAD 分区 |
| Outbox 多实例竞争 | `internal/infrastructure/persistence/mysql/outbox_repository.go` | `ClaimPending`、租约令牌、条件更新 |
| RabbitMQ 可靠发布 | `internal/infrastructure/mq/rabbitmq/executor_client.go` | `BatchRun`、`waitConfirms`、`reconnect`、`borrow/release` |
| 请求幂等与状态防回退 | `internal/infrastructure/persistence/mysql/execution_repository.go` | `Add`、`BatchAdd`、`Save`、`BatchUpdateStatus` |
| API 边界 | `api/proto/taskexecution/v1/task_execution.proto` | 三个 RPC、服务端流、request_id、下发状态定义 |

P0 文件不是要求逐行背诵，而是关闭编辑器后仍能画出数据流、说出失败窗口，并解释为什么这样设计。

### P1：面试官继续深挖时要能定位

| 主题 | 文件 | 要点 |
| --- | --- | --- |
| 游标扫描 | `internal/infrastructure/persistence/mysql/ut_case_repository.go` | `FindInBatches`、复合索引、有界内存 |
| 状态机 | `internal/domain/vo/execution_status.go` | INIT、WAIT、RUNNING、SUCCESS、FAILED 的合法迁移 |
| 执行记录聚合 | `internal/domain/entity/execution_record.go` | 状态转换和时间字段 |
| gRPC 适配 | `internal/infrastructure/grpcserver/handler.go` | 参数校验、错误码映射、单协程发送进度 |
| 服务生命周期 | `cmd/server/main.go` | 依赖装配、Relay 启动、退出顺序 |
| 健康检查 | `internal/infrastructure/grpcserver/server.go` | SERVING/NOT_SERVING、优雅停机超时 |
| 数据库结构 | `scripts/schema.sql` | 幂等唯一键、扫描索引、Outbox 索引 |

### P2：知道作用即可

- `internal/domain/repository`、`internal/domain/port`：依赖倒置和测试替身。
- `pkg/logger`：统一结构化日志。
- `cmd/envinit`：测试环境初始化，不是核心业务链路。
- `cmd/bench`：性能证据采集工具，不是调度算法本身。
- 普通值对象、简单构造器和配置映射不需要花大量时间背。

## 二、主线一：为什么拆 Go，以及批量流水线如何工作

### 必读代码

1. `internal/usecase/dispatch_app_service.go`
   - `dispatchStream`：生产者、worker、聚合器和全局批量信号量。
   - `dispatchOneBatch`：DB 事务、MQ 发布、状态回写三个阶段。
   - `completeBatchResult`：部分成功时保留已确认结果。
2. `internal/infrastructure/persistence/mysql/ut_case_repository.go`
   - `ScanByVersion`、`ScanByChannelVersion`、`scan`。
3. `internal/infrastructure/grpcserver/handler.go`
   - `progressForwarder`。

### 面试高频追问

#### 为什么不继续优化 Python

回答结构：

1. 原 Python Web 同时承担在线请求和长时间批量下发。
2. Pika `BlockingConnection` 需要互斥使用，消息发布容易串行化。
3. 全量加载用例造成峰值内存和长时间占用 Web worker。
4. 下发边界稳定，适合独立扩缩容；最终状态仍保留在 Python，降低迁移面。
5. 代价是增加 RPC、部署和一致性处理，所以只抽性能热点，不重写整个系统。

#### 为什么用生产者—worker—聚合器

- 生产者负责游标扫描和稳定分片编号。
- 固定 worker 数并行处理 DB/MQ，避免为每条用例创建 goroutine。
- 聚合器单协程调用 `stream.Send`，避免并发发送不安全和累计数竞争。
- 有界 channel 提供背压，RabbitMQ 或数据库变慢时，扫描不会无限占用内存。
- 全局 `batchSlots` 限制同时运行的批量 RPC，避免多个大任务叠加压垮中间件。

#### 内存为什么不会随 52,265 条线性增长

因为用例通过 `FindInBatches` 分片读取，内存中主要保留：

```text
生产队列容量 × batch_size
+ 正在被 worker 处理的分片
+ 当前分片的执行记录与 MQ 任务
```

而不是一次性加载全部用例。需要主动说明这是“有界内存”，不是“零内存开销”。

#### 为什么进度帧不保证 chunk_index 顺序

分片编号由生产者分配，但多个 worker 完成速度不同；聚合器按完成顺序发送。`chunk_index` 用于客户端关联，`total_dispatched` 由聚合器串行累计。

### 动手练习

1. 不看代码画出：`scan → batchCh → workers → progressCh → stream.Send`。
2. 把 `worker_count` 从 4 改成 1，预测吞吐、内存和 MQ channel 使用变化，再用 `cmd/bench` 验证。
3. 回答：如果 `progressCh` 不关闭，聚合器会发生什么；由谁负责关闭最安全。
4. 找出代码中三个响应 `ctx.Done()` 的位置，说明各自防止什么泄漏。

## 三、主线二：MySQL 与 RabbitMQ 双写一致性

### 必读代码

1. `internal/domain/entity/outbox_message.go`
2. `internal/usecase/dispatch_app_service.go` 中写 execution + outbox 的事务阶段。
3. `internal/usecase/outbox_relay.go` 的 `DispatchOnce`。
4. `internal/infrastructure/persistence/mysql/outbox_repository.go` 的 `ClaimPending`。
5. `internal/usecase/outbox_relay_test.go`。

### 必须会画的故障矩阵

| 故障窗口 | 没有 Outbox | 当前方案 |
| --- | --- | --- |
| DB 提交前崩溃 | DB、消息都没有 | 事务整体回滚 |
| DB 提交后、MQ 发布前崩溃 | 执行记录存在但任务永久丢失 | Relay 重发 PENDING |
| MQ 确认后、DB 回写前崩溃 | 无法判断消息是否到达 | Relay 可能重发，消费者按 execution_id 去重 |
| Relay 持有消息时崩溃 | 消息可能永久卡住 | 租约过期后其他实例接管 |

### 面试高频追问

#### 为什么不能用一个数据库事务包住 MQ 发布

MySQL 事务只能回滚数据库，无法回滚已经到达 RabbitMQ 的消息。把网络调用放在长事务内还会占用连接和锁。Outbox 把“业务记录”和“待发布事实”放进同一个数据库事务，再异步完成跨系统投递。

#### 这是 exactly-once 吗

不是，是 at-least-once。MQ 已确认但 PUBLISHED 尚未写回时崩溃，系统无法可靠区分“消息没发出”和“已发出但没记账”，只能重发。真正收敛点是执行机按 `execution_id` 去重。

#### 为什么需要租约令牌，只有 lease_until 不够吗

场景：A 领取消息后暂停，租约过期；B 重新领取并开始处理；A 恢复后迟到回写。如果只检查时间，A 可能覆盖 B 的结果。每次领取生成新的 `lease_token`，更新时同时匹配状态和令牌，A 的旧令牌影响零行，从而拒绝迟到写入。

#### 为什么使用 SKIP LOCKED

多个 Relay 实例同时领取任务时，`FOR UPDATE SKIP LOCKED` 让每个实例跳过已被其他事务锁定的行，减少等待并避免重复领取。领取和 PROCESSING 状态更新必须在同一个短事务内完成。

### 动手练习

```powershell
go test ./internal/usecase -run "TestOutboxRelay|TestDispatchAppService_执行记录与Outbox" -v

$env:TEST_MYSQL_DSN = 'root:123456@tcp(127.0.0.1:3307)/case_execution?parseTime=true&charset=utf8mb4&loc=Local'
go test -tags=integration ./internal/infrastructure/persistence/mysql -run "TestOutboxRepository" -count=1 -v
```

练习解释三个结果分区：PUBLISHED、PENDING 重试、达到次数上限后的 DEAD。

## 四、主线三：RabbitMQ 可靠发布与部分成功

### 必读代码

`internal/infrastructure/mq/rabbitmq/executor_client.go`：

- `Client`、`pooledChannel`：连接和 channel 池模型。
- `openPooledChannel`：Confirm、NotifyReturn、NotifyClose。
- `BatchRun`：序号映射和批量发布。
- `waitConfirms`：ack、nack、return、超时、连接关闭。
- `reconnect`、`borrow`、`release`：断线恢复和 channel 生命周期。

### 面试高频追问

#### Connection 和 Channel 为什么这样使用

AMQP Connection 可以由多个 goroutine 共享；Channel 不适合被多个 goroutine 并发发布。实现中每个批量 worker 独占一个 pooled channel，多个 channel 复用同一个 TCP Connection。

#### Publisher Confirm 为什么还需要 mandatory

Confirm ack 只表示交换机接收消息，不代表存在可消费路由。`mandatory=true` 时，无绑定路由会收到 `basic.return`。因此只有“未 return 且收到 ack”才算成功下发。

#### 批量中途连接断开怎么办

- 已确认 ack 且未 return 的 ID 保留在成功集合。
- 已 nack、return 的 ID 进入失败集合。
- 尚未收到确认的序号进入失败集合。
- channel 标记为 broken，不放回池。
- 上层状态回写保留部分成功，未决任务由 Outbox 负责恢复。

#### 为什么发布前记录 GetNextPublishSeqNo

Confirm 返回的是 delivery tag，需要在发布前建立 `sequence number → execution_id` 映射，才能把逐条确认还原成业务执行记录。

#### Confirm channel 缓冲区为什么至少覆盖单批在途消息

如果通知 channel 太小且业务尚未读取，AMQP 客户端内部投递确认可能阻塞。当前 `PublisherBuffer` 根据批大小设置，使单个 pooled channel 的在途确认有足够缓冲空间。

### 动手练习

```powershell
$env:TEST_AMQP_URL = 'amqp://practice:practice@127.0.0.1:5673/'
go test -tags=integration ./internal/infrastructure/mq/rabbitmq -run "TestClient_BatchRun" -count=1 -v
```

手动练习：

1. 有绑定队列时运行，确认全部成功。
2. 使用没有绑定的版本路由，确认进入 FailedIDs 而不是 SuccessIDs。
3. 解释为什么不能只看 `PublishWithContext` 返回 nil。

## 五、主线四：request_id 幂等与状态竞争

### 必读代码

1. `api/proto/taskexecution/v1/task_execution.proto`
2. `internal/usecase/dispatch_app_service.go` 的 `NormalizeRequestID` 和已有记录分流。
3. `internal/infrastructure/persistence/mysql/execution_repository.go`
4. `internal/infrastructure/persistence/mysql/po/execution_record_po.go`
5. `internal/domain/vo/execution_status.go`

### 面试高频追问

#### 幂等是怎么保证的

1. Python Web 为一次业务下发生成 `request_id`，网络重试复用。
2. 数据库唯一键 `(request_id, case_id)` 是最终并发防线。
3. 插入使用冲突忽略，再回读已经存在的记录。
4. 已有状态不是 INIT 时直接返回原结果，不重新发布 MQ。
5. MQ 消息使用 `execution_id` 作为 message_id，执行机继续做消费幂等。

#### 为什么不能只在 Redis 里 SETNX

Redis 锁过期、缓存丢失或 DB 提交失败会引入新的双写问题。数据库唯一键与执行记录在同一个事实源中，能处理真正的并发插入竞争。Redis 可以做前置削峰，但不能替代数据库约束。

#### 为什么状态更新带 WHERE status=INIT

执行机可能非常快，在下发引擎写 WAIT 前，已经通过 Python Web 把状态推进到 RUNNING/SUCCESS。如果直接 UPDATE，会把更后的状态回退为 WAIT。条件更新只允许 INIT 推进；未命中后检查记录是否存在，存在说明已被合法推进，不视为失败。

#### 同一 request_id 被错误用于不同版本怎么办

单条和已有 case 重叠时会检测版本冲突并返回参数错误。更根本的约束是调用方把 request_id 当作一次业务操作标识，不能跨业务复用。面试时要主动说明当前唯一键语义是“同一次请求中的同一 case 幂等”。

### 动手练习

```powershell
go test ./internal/usecase -run "RequestIdempotent|相同请求" -v

$env:TEST_MYSQL_DSN = 'root:123456@tcp(127.0.0.1:3307)/case_execution?parseTime=true&charset=utf8mb4&loc=Local'
go test -tags=integration ./internal/infrastructure/persistence/mysql -run "Idempotent|幂等|Regress" -count=1 -v
```

另外用 `cmd/bench` 连续两次传相同 request_id，对账 execution 和 outbox 数量不增加。

## 六、主线五：取消、补偿和优雅停机

### 必读代码

1. `internal/usecase/dispatch_app_service.go`
   - channel 发送和 worker 循环中的 `ctx.Done()`。
   - MQ 发布后的 `context.WithoutCancel + 5 秒超时`。
2. `cmd/server/main.go`
   - gRPC、Outbox、MQ、MySQL 的退出顺序。
3. `internal/infrastructure/grpcserver/server.go`
   - 健康检查与 `GracefulStop`。
4. `internal/infrastructure/mq/rabbitmq/executor_client.go`
   - `Close` 与重连竞争处理。

### 面试高频追问

#### 客户端断开后为什么还要继续写数据库

如果 MQ 已经确认，消息可能立即被执行机消费。此时继续使用已经取消的 RPC context，会导致执行记录停在 INIT。代码保留原请求 values，但去掉取消信号，并设置独立 5 秒超时完成补偿写回。

#### 为什么不能所有阶段都用 WithoutCancel

扫描和未发布任务应该随客户端取消，否则会浪费资源继续生成新任务。只有“不可撤销副作用已经发生”后的收尾阶段需要独立补偿上下文。

#### 为什么停机顺序是 gRPC → Relay → MQ → MySQL

- 先停止新 RPC，并等待正在执行的 handler。
- 再停止后台 Outbox，避免继续创建 MQ/DB 操作。
- 然后关闭 MQ。
- 最后关闭 MySQL，因为前面的收尾仍可能需要状态写回。

### 动手练习

```powershell
go test ./internal/usecase -run "ClientCancelAfterPublish|WriteBackError" -v
go test ./internal/infrastructure/grpcserver -run "HealthAndGracefulStop" -v
go test -race ./...
```

在隔离测试环境手动向运行中的服务发送 SIGTERM，观察健康状态、在途请求、Outbox 和最终退出日志。

## 七、最值得复习的测试文件

测试代码比普通实体代码更能展示边界意识，建议按以下顺序阅读：

1. `internal/usecase/dispatch_app_service_test.go`
   - 取消后补偿。
   - 部分发布成功。
   - 状态写回错误可见。
   - request_id 重试。
2. `internal/usecase/outbox_relay_test.go`
   - 发布成功、退避重试、达到上限进入 DEAD。
3. `internal/infrastructure/persistence/mysql/outbox_repository_integration_test.go`
   - 租约与旧令牌拒绝。
4. `internal/infrastructure/persistence/mysql/execution_repository_integration_test.go`
   - 幂等、状态防回退、缺失记录报错。
5. `internal/infrastructure/mq/rabbitmq/client_integration_test.go`
   - 真正确认发布、无路由失败、Close 幂等。

面试官问“你怎么证明”时，不要只回答“我测过”，要能说出对应的测试场景、注入的故障和断言。

## 八、三张白板图必须会画

### 1. 系统边界图

```text
Python Web → gRPC → Go 下发引擎 → MySQL / RabbitMQ → 执行机
     ↑                                               ↓
     └──────────── 最终执行状态回调 ────────────────┘
```

强调 Go 引擎只负责下发结果，不拥有最终执行状态机。

### 2. 单分片三阶段

```text
事务一：execution_record + outbox
                ↓
事务外：RabbitMQ publish + return/confirm
                ↓
事务二：INIT → WAIT/FAILED + outbox terminal state
```

### 3. 批量并发拓扑

```text
游标生产者 → 有界 batchCh → N 个 worker → progressCh → 单聚合器 → gRPC stream
```

## 九、90 分钟练习法

### 前 30 分钟：只看 P0 文件

- 顺着一个 case 从 gRPC 请求追到 MQ 消息和状态回写。
- 在纸上画三张图。
- 给每个跨系统边界标出可能失败的位置。

### 中间 30 分钟：关闭代码口述

每个问题控制在 2～3 分钟：

1. 为什么拆 Go？
2. 批量流水线如何控制内存和并发？
3. DB/MQ 双写怎么处理？
4. 为什么不是 exactly-once？
5. Confirm 和 mandatory 分别解决什么？
6. request_id、execution_id 各负责哪一层幂等？
7. 执行机快速回调时如何防止状态回退？
8. 客户端取消后为什么还要补偿写回？

### 最后 30 分钟：跑边界测试

```powershell
go test ./internal/usecase -v
go test -race ./...

$env:TEST_MYSQL_DSN = 'root:123456@tcp(127.0.0.1:3307)/case_execution?parseTime=true&charset=utf8mb4&loc=Local'
go test -tags=integration ./internal/infrastructure/persistence/mysql -count=1 -v

$env:TEST_AMQP_URL = 'amqp://practice:practice@127.0.0.1:5673/'
go test -tags=integration ./internal/infrastructure/mq/rabbitmq -count=1 -v
```

每跑完一个测试，强迫自己回答：测试制造了什么故障，如果没有这段实现会出现什么线上后果。

## 十、2-1 回答标准

### 只到 1-2 的讲法

“我用 Go、gRPC、RabbitMQ 重写了 Python 下发，用 goroutine 提高了性能。”

### 更符合 2-1 的讲法

“我先定位到全量加载和 Pika 连接锁导致的串行瓶颈，再把稳定的下发边界抽成独立引擎；使用有界流水线控制资源，通过 Outbox 和消费幂等收敛 DB/MQ 一致性，并针对无路由、部分确认、客户端取消、状态竞争和多实例 Relay 逐一设计了恢复语义和测试证据。方案明确提供 at-least-once，不宣称 exactly-once。”

2-1 的重点不是技术名词数量，而是能够从业务瓶颈推导设计、说明取舍、识别不可能保证的语义，并拿故障测试和真实数据证明结果。
