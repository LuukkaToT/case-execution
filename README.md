# Case Execution

本仓库记录自动化测试平台“用例下发”能力从 Python Web 内部实现演进为独立 Go gRPC 引擎的过程。

- `before_reactoring/`：抽离前的 Python 业务服务与 RabbitMQ 发布代码，仅作为演进背景，不参与 Go 服务构建。
- `execution-engine/`：当前 Go 用例下发引擎，包含 gRPC、游标分片、并发流水线、事务 Outbox、幂等、RabbitMQ 发布确认、集成测试和压测工具。

项目定位、运行方式和可靠性语义见 [execution-engine/README.md](execution-engine/README.md)，公司测试环境初始化与性能基准步骤见 [execution-engine/scripts/benchmark/README.md](execution-engine/scripts/benchmark/README.md)，面试讲述建议见 [execution-engine/docs/interview.md](execution-engine/docs/interview.md)。
