package repository

import (
	"context"
	"time"

	"execution-engine/internal/domain/entity"
)

// OutboxRepository 负责可靠投递消息的持久化和租约管理。Add、BatchAdd 与
// 终态更新均加入 ctx 携带的事务；ClaimPending 使用独立短事务，保证
// SELECT ... FOR UPDATE SKIP LOCKED 与 PROCESSING 状态迁移在多实例间原子执行。
type OutboxRepository interface {
	Add(ctx context.Context, message *entity.OutboxMessage) error
	BatchAdd(ctx context.Context, messages []*entity.OutboxMessage) error
	ClaimPending(ctx context.Context, limit int, now, leaseUntil time.Time) ([]*entity.OutboxMessage, error)
	MarkPublished(ctx context.Context, executionIDs []int64, leaseToken string, publishedAt time.Time) error
	MarkRetry(ctx context.Context, executionIDs []int64, leaseToken string, availableAt time.Time, lastError string) error
	MarkDead(ctx context.Context, executionIDs []int64, leaseToken string, lastError string) error
}
