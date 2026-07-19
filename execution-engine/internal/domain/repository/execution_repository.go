// Package repository 定义领域层所需的持久化端口。
package repository

import (
	"context"

	"execution-engine/internal/domain/entity"
)

// ExecutionRepository 负责持久化 ExecutionRecord 聚合。
//
// 实现必须识别 ctx 中携带的事务句柄，详见 mysql.WithTx；未携带事务时，
// 仓储通过 gorm.WithContext 使用根连接池执行独立数据库操作。
type ExecutionRepository interface {
	// Add 插入单条记录，并原地回填 ExecutionID。
	Add(ctx context.Context, record *entity.ExecutionRecord) error

	// BatchAdd 高效批量插入记录，并原地回填各自的 ExecutionID。
	BatchAdd(ctx context.Context, records []*entity.ExecutionRecord) error

	// FindByID 在记录不存在时返回 nil, nil，与 Python 适配器返回 None 的语义一致。
	FindByID(ctx context.Context, executionID int64) (*entity.ExecutionRecord, error)

	// Save 持久化 status、execute_at、finish_at 等可变字段。
	Save(ctx context.Context, record *entity.ExecutionRecord) error

	// BatchUpdateStatus 按目标状态分组持久化下发状态和终态时间，限制 SQL 数量。
	BatchUpdateStatus(ctx context.Context, records []*entity.ExecutionRecord) error
}
