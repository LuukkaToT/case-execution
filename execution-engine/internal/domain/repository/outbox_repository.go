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
	// Add 在调用方事务中幂等写入单条消息，并回填 Outbox 主键。
	Add(ctx context.Context, message *entity.OutboxMessage) error
	// BatchAdd 在调用方事务中批量幂等写入消息，并回填持久化信息。
	BatchAdd(ctx context.Context, messages []*entity.OutboxMessage) error
	// ClaimPending 用独立短事务领取到期消息，并写入 PROCESSING、租约和新令牌。
	ClaimPending(ctx context.Context, limit int, now, leaseUntil time.Time) ([]*entity.OutboxMessage, error)
	// MarkPublished 把当前租约持有者确认成功的消息更新为 PUBLISHED。
	MarkPublished(ctx context.Context, executionIDs []int64, leaseToken string, publishedAt time.Time) error
	// MarkRetry 把可重试失败恢复为 PENDING，并设置下一次可领取时间。
	MarkRetry(ctx context.Context, executionIDs []int64, leaseToken string, availableAt time.Time, lastError string) error
	// MarkDead 把达到重试上限或快速路径确定失败的消息更新为 DEAD。
	MarkDead(ctx context.Context, executionIDs []int64, leaseToken string, lastError string) error
}
