// Package port 定义领域层依赖但不实现的被驱动端接口，由基础设施适配器实现。
package port

import (
	"context"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// UtCaseExecutorClient 抽象面向 MQ 的任务发布器。生产实现位于
// rabbitmq/executor_client.go，测试可注入 fake。
//
// 并发约束：
//   - 实现必须支持多个 goroutine 并发调用，因为下发 usecase 会启动 N 个 worker。
//   - 每个进行中的分片应独占一个 amqp.Channel，避免跨 goroutine 共享 channel
//     导致发布序号映射错乱。
type UtCaseExecutorClient interface {
	// Execute 发布单条任务，收到发布确认后返回 nil。
	Execute(ctx context.Context, record *entity.ExecutionRecord, caseName string) error

	// BatchRun 发布一批任务并等待逐条 publisher confirm。返回的 BatchResult
	// 按确认结果划分输入；只有批次级异常返回 error，单条 nack 通过 FailedIDs 表示。
	BatchRun(ctx context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error)

	// Close 释放连接和 channel 池资源。
	Close(ctx context.Context) error
}
