# 用例下发引擎

这是从 Python Web 服务中抽离出的高吞吐用例下发引擎。Python Web 仍然负责业务入口、权限与用例最终状态；本引擎通过 gRPC 接收下发请求，查询用例、创建执行记录，并将任务可靠地投递到 RabbitMQ。执行机消费任务后，仍回调 Python Web 更新 RUNNING、SUCCESS、FAILED 等执行状态。

```mermaid
flowchart LR
    A["Python Web<br/>业务入口与最终状态"] -->|"gRPC 下发请求"| B["Go execution-engine"]
    B -->|"游标扫描"| C[("MySQL<br/>ut_case")]
    B -->|"同事务写入"| D[("execution_record<br/>dispatch_outbox")]
    B -->|"确认发布"| E["RabbitMQ"]
    D -->|"失败恢复与重试"| F["Outbox Relay"]
    F --> E
    E --> G["执行机"]
    G -->|"回调执行状态"| A
```

## 为什么单独做成 Go 引擎

原 Python 链路同时承担 Web 请求、全量用例加载、执行记录创建和 RabbitMQ 发布。Pika 的 `BlockingConnection` 需要互斥访问，批量下发容易退化为串行网络往返；全量查询还会把一次任务的所有用例放进内存。该问题适合拆出一个边界清晰、可独立扩缩容的控制面引擎，而不是重写 Python Web 的业务能力。

引擎采用有界的生产者—工作协程—聚合器流水线：MySQL 游标分片负责生产，多个工作协程并行完成“批量落库—MQ 发布—状态回写”，单协程聚合进度并通过服务端流返回。内存上限主要由 `worker_count × batch_size` 决定，不随用例总量线性增长。

## 核心工程能力

- 三个 gRPC 接口：单用例、按版本全量下发、按渠道与版本下发；批量接口使用服务端流持续返回分片进度。
- `request_id` 幂等：Python Web 重试同一请求时复用请求标识，同一用例不会重复创建执行记录；数据库唯一键处理并发竞争。
- 事务 Outbox：执行记录与待投递消息在同一个 MySQL 事务中提交，进程崩溃后由 Relay 恢复投递。
- 至少一次投递：RabbitMQ 使用持久化消息、Publisher Confirm 和 `mandatory` 路由检查；消费者必须按 `execution_id` 去重。
- 部分成功语义：批量发布中断时保留已经确认的成功集合，仅把 nack、无路由、超时和未决消息归入失败集合。
- 多实例安全：Outbox 使用 `FOR UPDATE SKIP LOCKED`、租约和随机租约令牌，旧实例不能覆盖新实例的处理结果。
- 状态防回退：下发侧只允许 INIT 推进为 WAIT/FAILED；如果执行机已经回写更后面的状态，不会被迟到的下发回写覆盖。
- 有界并发与取消传播：限制单请求工作协程数和全局批量 RPC 数；客户端断开会取消扫描和发布，已完成 MQ 发布后的状态补偿使用独立短超时上下文。
- 标准健康检查与结构化日志：注册 gRPC Health 服务，优雅停机时先摘除健康状态，再按 gRPC、Outbox、RabbitMQ、MySQL 的顺序收尾。

## 一致性语义

MySQL 与 RabbitMQ 之间没有分布式事务。本项目选择事务 Outbox 和至少一次投递，而不是宣称无法成立的“绝对不重复”。

| 故障窗口 | 恢复行为 | 对外语义 |
| --- | --- | --- |
| 执行记录事务回滚 | Outbox 同时回滚，不会发布任务 | 不下发 |
| DB 提交后、首次发布前崩溃 | Relay 扫描 PENDING 并重新发布 | 最终可达 |
| MQ 已确认、状态回写前崩溃 | Relay 可能再次发布相同 `execution_id` | 至少一次，消费端去重 |
| 无队列绑定路由 | `mandatory` 返回 NO_ROUTE，记录为失败 | 不把“交换机接收”误判为可消费 |
| Relay 实例持有任务时崩溃 | 租约到期后由其他实例重新获取 | 自动恢复 |

更完整的设计取舍见 [架构说明](docs/architecture.md)。

## 目录

```text
api/proto/                       gRPC 契约
cmd/server/                      服务入口与生命周期
cmd/bench/                       可复现的下发压测客户端
internal/domain/                 领域实体、值对象、仓储与端口
internal/usecase/                下发流水线与 Outbox Relay
internal/infrastructure/         gRPC、MySQL、RabbitMQ 适配器
scripts/                         建表与测试数据脚本
configs/                         本地配置
```

根目录的 `before_reactoring` 保存抽离前的 Python 核心代码，用来说明性能瓶颈和演进过程，不参与 Go 服务构建。

## 本地运行

当前 `configs/config.yaml` 使用本机已有的 MySQL `127.0.0.1:3307` 和 RabbitMQ `127.0.0.1:5673`，不要为了运行示例修改这两个端口。确保 `case_execution` 数据库、`ut.exec` 交换机及对应版本路由已经准备好，然后执行：

```powershell
cd D:\Project\case-execution\execution-engine
go run ./cmd/server
```

Python Web 调用方应为每次业务操作生成 `request_id`；网络重试复用原值。若不传，引擎会生成随机值并在响应或进度帧中返回。

## 验证

```powershell
go test ./...
go test -race ./...
go vet ./...

$env:TEST_MYSQL_DSN = 'root:123456@tcp(127.0.0.1:3307)/case_execution?parseTime=true&charset=utf8mb4&loc=Local'
go test -tags=integration ./internal/infrastructure/persistence/mysql -count=1

$env:TEST_AMQP_URL = 'amqp://practice:practice@127.0.0.1:5673/'
go test -tags=integration ./internal/infrastructure/mq/rabbitmq -count=1
```

集成测试会验证数据库幂等、状态防回退、Outbox 租约隔离、Publisher Confirm 以及无路由消息失败语义。持续集成配置位于 `.github/workflows/ci.yml`。

## 压测

```powershell
go run ./cmd/bench -addr localhost:9090 -version 26B -user bench -request-id bench-20260720-01 -expect 52265 -json-out bench-20260720-01.json
```

必须先为测试版本绑定真实队列；由于启用了 `mandatory`，没有任何队列能接收的消息会被正确统计为失败。基础口径见 [压测说明](docs/benchmark.md)，公司测试环境初始化、交叉编译、指标采集、对账 SQL 和回退流程见 [初始化与性能基准攻略](scripts/benchmark/README.md)。

## 项目讲述

面试时应把“线上已经验证的抽离与性能优化”和“复盘后在本地完成的可靠性演进”分开说明。两部分都可以作为本人完成的工程工作，但不要把本地验证描述成已在线上运行。可直接参考 [面试讲述提纲](docs/interview.md)。
