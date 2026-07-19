# 公司测试环境初始化与性能基准攻略

## 一、这次到底测什么

平台只有约 200 名用户，不需要构造 200 个并发全量请求。这个引擎一次只接收一个 gRPC 请求，但会扇出数万条执行记录和 RabbitMQ 消息，真正的压力单位是：

- 单次下发的用例总数，例如固定的 52,265 条。
- 同时存在的全量任务数，通常只需要测 1；历史上可能重叠时再测 2。
- MySQL 批量写入、MQ Confirm、Outbox 和状态回写能否稳定完成。

所以报告应叫“批量下发性能基准与容量验证”，而不是“200 用户接口压测”。

明天建议按三个阶段执行：

1. 独立交换机上的全量下发基准，不让真实执行机消费。
2. 10～100 条安全用例的真实端到端验证。
3. 只有业务允许时，才做全量真实执行。

## 二、目录中的文件

| 文件 | 用途 |
| --- | --- |
| `config.example.yaml` | 测试环境配置模板 |
| `init-test-env.sh` | Linux 一键初始化入口 |
| `cmd/envinit` | 初始化程序源码；补齐数据库表、声明 MQ 资源 |
| `cmd/bench` | 下发基准客户端；输出终端结果与 JSON 报告 |

初始化工具只执行以下操作：

- DSN 指定的数据库不存在时创建数据库。
- 使用 GORM AutoMigrate 补齐 `ut_case`、`execution_record`、`dispatch_outbox` 表、字段和索引。
- 声明 durable direct exchange、durable queue，并按版本绑定路由键。
- 检查并打印队列已有消息数和消费者数。

它不会清表、删表、清空队列、生成用例或发送消息。重复运行是面向测试环境的增量补齐，不是重置环境。

## 三、最重要的安全规则

1. 不要在公司环境执行 `scripts/seed.sql`，里面包含 `TRUNCATE`。
2. 初始化脚本必须显式带 `-confirm-test-environment`，否则拒绝执行。
3. 默认只允许名字中包含 `bench`、`test`、`qa` 或 `staging` 的交换机。
4. 全量下发基准使用独立交换机，例如 `ut.exec.bench.20260720`。
5. 仅仅在生产交换机上多建一个队列不能隔离消息；真实执行队列也绑定相同版本时，真实机器仍会收到任务。
6. 生产账号密码写入 Git 仓库外的配置文件，并设置为仅运行用户可读。
7. 不直接覆盖旧二进制；保留旧版本、旧配置和 Python Web 的切回方式。

## 四、编译前检查

在目标执行机确认架构：

```bash
uname -m
```

- `x86_64` 对应 `GOARCH=amd64`。
- `aarch64` 对应 `GOARCH=arm64`。

在 Windows 开发机记录代码版本并完成验证：

```powershell
cd D:\Project\case-execution\execution-engine
git status --short
git rev-parse HEAD
go test ./...
go test -race ./...
go vet ./...
```

以 `x86_64` 为例交叉编译三个二进制：

```powershell
New-Item -ItemType Directory -Force dist
$env:CGO_ENABLED = "0"
$env:GOOS = "linux"
$env:GOARCH = "amd64"

go build -trimpath -ldflags="-s -w" -o dist/execution-engine ./cmd/server
go build -trimpath -ldflags="-s -w" -o dist/bench ./cmd/bench
go build -trimpath -ldflags="-s -w" -o dist/envinit ./cmd/envinit

Get-FileHash -Algorithm SHA256 dist/execution-engine
Get-FileHash -Algorithm SHA256 dist/bench
Get-FileHash -Algorithm SHA256 dist/envinit
go version -m dist/execution-engine

Remove-Item Env:CGO_ENABLED, Env:GOOS, Env:GOARCH
```

上传后在 Linux 再核对：

```bash
sha256sum execution-engine bench envinit
chmod 750 execution-engine bench envinit
file execution-engine bench envinit
```

最终报告保存 Git 提交号、SHA256、Go 版本、编译时间和目标机器架构。

## 五、准备配置文件

把模板复制到 Git 仓库外再填写：

```bash
sudo install -d -m 750 /etc/case-execution
sudo install -m 600 scripts/benchmark/config.example.yaml /etc/case-execution/bench.yaml
sudo vi /etc/case-execution/bench.yaml
```

第一阶段应满足：

- MySQL 指向测试库或独立压测库。
- RabbitMQ 地址可以是真实测试集群，但 exchange 必须是独立压测交换机。
- `dispatch.worker_count=4`、`rabbitmq.channel_pool=8` 先保持当前推荐值。
- `max_concurrent_batches=2` 只作为保护上限，不代表要主动跑两个并发任务。

## 六、一键初始化 MySQL 和 RabbitMQ

在 Linux 上执行：

```bash
CONFIG_PATH=/etc/case-execution/bench.yaml \
ROUTING_KEY=26B \
QUEUE_NAME=ut.exec.bench.26B \
ENVINIT_BIN=./envinit \
sh scripts/benchmark/init-test-env.sh
```

预期输出包括：

- 数据库名称以及本次是否新建。
- 三张表已就绪。
- exchange、queue、routing key。
- 队列当前消息数和消费者数。

重复执行时不会清空已有队列。如果提示队列声明参数不一致，工具会失败退出，不会删除并重建队列。

如果测试环境必须复用名为 `ut.exec` 的交换机，不能直接使用一键包装脚本。先确认这个交换机不连接真实消费者，再手工执行：

```bash
./envinit \
  -config /etc/case-execution/bench.yaml \
  -queue ut.exec.bench.26B \
  -routing-key 26B \
  -confirm-test-environment \
  -allow-shared-exchange
```

这属于高风险模式，优先改成独立交换机。

### 初始化后核对数据库

```sql
SHOW CREATE TABLE ut_case;
SHOW CREATE TABLE execution_record;
SHOW CREATE TABLE dispatch_outbox;

SELECT version, COUNT(*) AS case_count
FROM ut_case
GROUP BY version
ORDER BY case_count DESC;
```

初始化工具不生成用例。如果 `26B` 不是预期的 52,265 条，需要先通过公司允许的数据同步方式补齐用例，再开始测试。

对于正式生产库，不建议用该工具临时迁移大表；应由 DBA 审核 `scripts/migrations/20260719_reliable_dispatch.sql`，评估索引创建和锁表影响。AutoMigrate 只用于测试环境快速补齐。

## 七、启动引擎与冒烟

指定 Git 外部配置启动：

```bash
export EXEC_ENGINE_CONFIG=/etc/case-execution/bench.yaml
./execution-engine
```

建议交给 systemd 或公司的进程管理平台运行，并保存标准输出、退出码和 PID。启动后检查：

```bash
ss -lntp | grep 9090
```

如果安装了 gRPC 健康检查工具：

```bash
grpc_health_probe -addr 127.0.0.1:9090
```

先用一个只有 1～10 条用例的专用版本冒烟，确认：

- 客户端能收到进度流。
- MQ 队列收到对应消息。
- 没有 NO_ROUTE、nack 或 confirm timeout。
- 执行记录不长期停留在 INIT。
- Outbox 最终为 PUBLISHED，不出现 DEAD。

## 八、单次全量性能基准

固定同一份 52,265 条用例。先预热 1 次，再正式运行 3～5 次；没有必要跑 10 次，更不要制造 200 并发。

```bash
RUN_ID="ce-$(date +%Y%m%d-%H%M%S)-single-01"

./bench \
  -addr 127.0.0.1:9090 \
  -version 26B \
  -user benchmark \
  -request-id "$RUN_ID" \
  -expect 52265 \
  -timeout 10m \
  -json-out "$RUN_ID.json"
```

客户端会记录：

- 总耗时和首帧耗时。
- 计划、成功、失败、未决数量。
- 分片数和分片到达间隔 P50/P95。
- 平均下发吞吐。
- request_id、测试开始结束时间、错误分片和流式错误。

以下情况客户端以失败退出码结束：

- 计划数与 `-expect` 不一致。
- 发布失败或存在未决记录。
- 分片错误或流式 RPC 中断。

结果文件使用安全创建模式，已存在时不会覆盖。

每轮正式样本使用新的 `request_id`。同一个 `request_id` 再执行一次是在验证幂等，不是新的性能样本。

## 九、要不要测并发

200 名用户不等于 200 个批量任务并发。先从 Python Web 日志或历史任务记录确认：一分钟内最多同时发起过几个全量任务。

- 峰值只有 1：只做单任务基准。
- 偶尔有 2 个重叠：补做 2 并发容量验证。
- 没有证据支持 4 或 8：不继续提高。

两个任务同时运行时，分别使用不同 `request_id` 和 JSON 文件。重点观察总耗时是否成倍恶化、MySQL 是否连接耗尽、RabbitMQ 是否 blocked，以及内存是否持续上升。

`max_concurrent_batches` 是服务自我保护上限，不是压测目标。

## 十、生产环境必须验证的内容

### 1. 协议兼容

- Python gRPC 客户端能调用三个接口。
- 新增字段是向后兼容的；Python Web 应保存并在重试时复用 `request_id`。
- MQ 消息仍为 `execution_id`、`case_name`、`version` 三个字段。
- routing key 仍等于 version。

### 2. MQ 语义

- Publisher Confirm 正常，没有 nack 和超时。
- `mandatory` 能识别不存在的路由。
- 真实队列绑定正确，不会把“交换机接收”误当成“执行机能收到”。
- 执行机按 `execution_id` 去重，能够承受至少一次投递。

### 3. 状态竞争

- 执行机很快回调 RUNNING/SUCCESS 时，Go 引擎迟到的 WAIT 不会把状态回退。
- FAILED 记录有 `finish_at`。
- 客户端断开后，已经发布的消息仍能完成状态补偿。

### 4. Outbox

- 正常全量下发后 PENDING、PROCESSING、DEAD 都归零。
- 进程优雅重启后没有任务永久停在租约状态。
- 故障注入只在独立测试交换机和测试数据库进行，不重启生产 RabbitMQ/MySQL。

### 5. 小规模真实端到端

准备 10～100 条无副作用测试用例，切到真实执行队列后核对：

- MQ 收到数量。
- 执行机收到的唯一 execution_id 数。
- Python Web 回调成功率。
- RUNNING、SUCCESS、FAILED 状态及时间字段。
- Python Web 其他正常接口延迟没有明显上升。

## 十一、按 request_id 对账

把变量换成本轮真实标识，以下 SQL 都是只读查询：

```sql
SET @request_id = 'ce-20260720-100000-single-01';

SELECT COUNT(*) AS execution_count,
       COUNT(DISTINCT case_id) AS distinct_case_count
FROM execution_record
WHERE request_id = @request_id;

SELECT execution_status, COUNT(*) AS count
FROM execution_record
WHERE request_id = @request_id
GROUP BY execution_status
ORDER BY execution_status;

SELECT o.state, COUNT(*) AS count
FROM dispatch_outbox o
JOIN execution_record e ON e.execution_id = o.execution_id
WHERE e.request_id = @request_id
GROUP BY o.state
ORDER BY o.state;

SELECT COUNT(*) AS duplicate_case_groups
FROM (
    SELECT case_id
    FROM execution_record
    WHERE request_id = @request_id
    GROUP BY case_id
    HAVING COUNT(*) > 1
) duplicated;

SELECT COUNT(*) AS stale_init
FROM execution_record
WHERE request_id = @request_id
  AND execution_status = 'INIT'
  AND create_at < NOW(3) - INTERVAL 15 SECOND;

SELECT COUNT(*) AS stale_outbox
FROM dispatch_outbox o
JOIN execution_record e ON e.execution_id = o.execution_id
WHERE e.request_id = @request_id
  AND o.state IN ('PENDING', 'PROCESSING')
  AND o.create_at < NOW(3) - INTERVAL 15 SECOND;
```

计算记录创建到 Outbox 标记发布的消息级延迟：

```sql
WITH latency AS (
    SELECT TIMESTAMPDIFF(MICROSECOND, e.create_at, o.published_at) / 1000.0 AS dispatch_ms
    FROM execution_record e
    JOIN dispatch_outbox o ON o.execution_id = e.execution_id
    WHERE e.request_id = @request_id
      AND o.published_at IS NOT NULL
), ranked AS (
    SELECT dispatch_ms,
           CUME_DIST() OVER (ORDER BY dispatch_ms) AS percentile
    FROM latency
)
SELECT MIN(CASE WHEN percentile >= 0.50 THEN dispatch_ms END) AS p50_ms,
       MIN(CASE WHEN percentile >= 0.95 THEN dispatch_ms END) AS p95_ms,
       MAX(dispatch_ms) AS max_ms
FROM ranked;
```

端到端小流量测试可以把 `published_at` 换成 `execute_at` 或 `finish_at`，计算排队和最终完成延迟。

## 十二、运行期间采集的指标

### Go 执行机

- CPU 平均值和峰值。
- RSS 内存平均值、峰值，以及任务结束后是否回落。
- 磁盘 IO、网络发送速率、打开文件数。
- gRPC、MySQL、RabbitMQ 错误日志和进程重启次数。

如果机器安装了 sysstat：

```bash
pidstat -rud -p "$(pidof execution-engine)" 1
vmstat 1
```

保留完整测试窗口的日志，不只截取 `top` 的某一瞬间。

### MySQL

- 执行记录与 Outbox 写入数。
- CPU、连接数、慢查询、磁盘 IO 和锁等待。
- `ut_case` 游标扫描是否使用复合索引。
- 是否存在超过 15 秒的 INIT、PENDING、PROCESSING。

### RabbitMQ

- Publish、Confirm、Deliver/Ack 每秒速率。
- 专用队列的 Ready、Unacked、消费者数和累计接收数。
- Connections、Channels 数。
- unroutable、nack、connection blocked、memory alarm、disk alarm。

### Python Web 与执行机消费者

- 消息接收数、唯一 execution_id 数、重复消息数。
- 队列等待时间。
- 回调成功率和延迟。
- 最终状态分布。
- 正常 Web 接口延迟是否受影响。

## 十三、通过标准

单次全量下发至少满足：

- `planned = execution_record 数 = distinct case_id 数 = 52,265`。
- 客户端 failed=0、unresolved=0、没有 chunk error。
- Outbox 最终全部 PUBLISHED。
- 专用 MQ 队列接收的唯一 execution_id 数与计划数一致。
- 没有长期 INIT/PENDING/PROCESSING/DEAD。
- Go 进程不崩溃，内存不持续增长。
- MySQL 与 RabbitMQ 没有资源报警。

## 十四、立即停止条件

- 真实执行队列意外收到隔离测试消息。
- Python Web 正常业务明显变慢。
- RabbitMQ 出现内存、磁盘报警或 connection blocked。
- MySQL 出现持续锁等待、连接耗尽或慢查询暴涨。
- Outbox DEAD 持续增长。
- 失败、未决或重复执行数量不为零且原因不明。

## 十五、回退

1. 先停止 Python Web 向新 gRPC 引擎发送新请求，切回旧 Python 下发路径。
2. 给在途请求一个明确收尾时间，再向 Go 进程发送 SIGTERM。
3. 保存二进制版本、配置、日志、JSON、执行记录和 Outbox 数据。
4. 检查 PENDING/PROCESSING，决定由新版本恢复还是人工处理。
5. 恢复旧二进制和旧配置。
6. 数据库新增的可空字段和新表先保留，不在故障回退时做高风险删除 DDL。

## 十六、最终报告模板

| 项目 | 需要记录 |
| --- | --- |
| 代码 | Git 提交号、二进制 SHA256、Go 版本 |
| 环境 | CPU、内存、Linux、MySQL、RabbitMQ 版本和网络位置 |
| 数据 | 版本号、计划用例数、是否真实执行 |
| 参数 | batch、worker、channel pool、批量任务并发数 |
| 性能 | 3～5 次总耗时、中位数、P95、吞吐、首帧耗时 |
| 资源 | CPU/RSS 峰值、DB/MQ 关键水位 |
| 正确性 | execution、outbox、MQ、Python 回调四方对账 |
| 异常 | 分片错误、重复、失败、未决和恢复过程 |

如果旧 Python 链路还能在同一台机器、同一份用例、同一个测试交换机上运行，也各跑 3～5 次。只有环境和可靠性口径相同时，才能计算严格的耗时下降比例与吞吐提升倍数。
