// Package repository declares persistence ports for the domain.
package repository

import (
	"context"

	"execution-engine/internal/domain/entity"
)

// ExecutionRepository persists ExecutionRecord aggregates.
//
// Transactions: implementations must respect a transactional *gorm.DB held
// in ctx (see infrastructure/persistence/mysql.WithTx). A method called with
// a non-transactional ctx opens its own short-lived transaction implicitly
// via gorm's WithContext.
type ExecutionRepository interface {
	// Add inserts a single record and back-fills ExecutionID in place.
	Add(ctx context.Context, record *entity.ExecutionRecord) error

	// BatchAdd inserts records in efficient batches and back-fills each
	// ExecutionID in place.
	BatchAdd(ctx context.Context, records []*entity.ExecutionRecord) error

	// FindByID returns nil, nil when the id is absent (not an error; this
	// matches the semantic of the Python adapter returning None).
	FindByID(ctx context.Context, executionID int64) (*entity.ExecutionRecord, error)

	// Save persists mutable fields (status, execute_at, finish_at).
	Save(ctx context.Context, record *entity.ExecutionRecord) error

	// BatchUpdateStatus updates status only, grouped by target status to
	// keep the SQL count bounded (two statuses -> two UPDATE statements).
	BatchUpdateStatus(ctx context.Context, records []*entity.ExecutionRecord) error
}
