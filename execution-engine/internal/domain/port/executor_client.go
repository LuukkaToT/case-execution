// Package port declares driven-side interfaces the domain depends on but
// does not implement. Infrastructure packages plug adapters into these ports.
package port

import (
	"context"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// UtCaseExecutorClient abstracts the MQ-facing dispatcher. Production
// implementation is rabbitmq/executor_client.go; tests may inject a fake.
//
// Concurrency contract:
//   - Implementations MUST be safe to invoke from many goroutines
//     concurrently (the dispatch usecase fans-out N workers).
//   - Implementations should use one amqp.Channel per in-flight batch to
//     avoid the seqNo corruption that occurs when amqp.Channel is shared
//     across goroutines.
type UtCaseExecutorClient interface {
	// Execute publishes a single task. Returns nil on confirmed publish.
	Execute(ctx context.Context, record *entity.ExecutionRecord, caseName string) error

	// BatchRun publishes tasks and waits for per-task publisher confirms.
	// The returned BatchResult partitions the inputs by confirm outcome.
	// An error is returned only for whole-batch failures; per-task nacks
	// are reported via BatchResult.FailedIDs.
	BatchRun(ctx context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error)

	// Close releases connection and channel-pool resources.
	Close(ctx context.Context) error
}
