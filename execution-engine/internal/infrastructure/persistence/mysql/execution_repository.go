package mysql

import (
	"context"
	"errors"
	"fmt"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/domain/vo"
	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"gorm.io/gorm"
)

// ExecutionRepository is the mysql-backed repository for ExecutionRecord.
type ExecutionRepository struct {
	root *gorm.DB
}

// NewExecutionRepository returns a repository bound to the root gorm handle.
func NewExecutionRepository(root *gorm.DB) repository.ExecutionRepository {
	return &ExecutionRepository{root: root}
}

// Add inserts one record and writes the DB-assigned id back into r.
func (r *ExecutionRepository) Add(ctx context.Context, record *entity.ExecutionRecord) error {
	db := FromCtx(ctx, r.root)
	row := po.FromExecutionRecord(record)
	if err := db.Create(row).Error; err != nil {
		return fmt.Errorf("insert execution_record: %w", err)
	}
	record.ExecutionID = row.ExecutionID
	record.CreateAt = row.CreateAt
	return nil
}

// BatchAdd inserts records in sub-batches of 200 (gorm default) to avoid
// hitting max_allowed_packet and to keep the per-statement latency bounded.
func (r *ExecutionRepository) BatchAdd(ctx context.Context, records []*entity.ExecutionRecord) error {
	if len(records) == 0 {
		return nil
	}
	db := FromCtx(ctx, r.root)
	rows := make([]*po.ExecutionRecord, 0, len(records))
	for _, rec := range records {
		rows = append(rows, po.FromExecutionRecord(rec))
	}
	if err := db.CreateInBatches(&rows, 200).Error; err != nil {
		return fmt.Errorf("batch insert execution_record: %w", err)
	}
	// Back-fill ids and timestamps into the domain aggregates; rows order
	// matches records order because CreateInBatches preserves it.
	for i, row := range rows {
		records[i].ExecutionID = row.ExecutionID
		records[i].CreateAt = row.CreateAt
	}
	return nil
}

// FindByID returns nil, nil when no row matches; the domain treats "absent"
// as distinct from "error" (see the Python adapter returning None).
func (r *ExecutionRepository) FindByID(ctx context.Context, executionID int64) (*entity.ExecutionRecord, error) {
	db := FromCtx(ctx, r.root)
	var row po.ExecutionRecord
	err := db.First(&row, executionID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select execution_record by id: %w", err)
	}
	return row.ToEntity(), nil
}

// Save updates the mutable fields. Only status, execute_at, finish_at ever
// change post-insert, so we write just those three columns to avoid stomping
// concurrent writes to e.g. create_by from the Python side.
func (r *ExecutionRepository) Save(ctx context.Context, record *entity.ExecutionRecord) error {
	db := FromCtx(ctx, r.root)
	res := db.Model(&po.ExecutionRecord{}).
		Where("execution_id = ?", record.ExecutionID).
		Updates(map[string]any{
			"execution_status": string(record.ExecutionStatus),
			"execute_at":       record.ExecuteAt,
			"finish_at":        record.FinishAt,
		})
	if res.Error != nil {
		return fmt.Errorf("update execution_record: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("update execution_record: no row matched id=%d", record.ExecutionID)
	}
	return nil
}

// BatchUpdateStatus groups records by target status and issues at most one
// UPDATE per distinct status. In practice the dispatch pipeline only ever
// writes WAIT or FAILED, so two statements cover the whole chunk.
func (r *ExecutionRepository) BatchUpdateStatus(ctx context.Context, records []*entity.ExecutionRecord) error {
	if len(records) == 0 {
		return nil
	}
	db := FromCtx(ctx, r.root)
	grouped := make(map[vo.ExecutionStatus][]int64, 2)
	for _, rec := range records {
		// StatusInit means DispatchBatch did not assign a terminal
		// status (execution_id was neither in success nor failed sets).
		// Skip these - the reconciler owns them.
		if rec.ExecutionStatus == vo.StatusInit {
			continue
		}
		grouped[rec.ExecutionStatus] = append(grouped[rec.ExecutionStatus], rec.ExecutionID)
	}
	for status, ids := range grouped {
		if err := db.Model(&po.ExecutionRecord{}).
			Where("execution_id IN ?", ids).
			Update("execution_status", string(status)).Error; err != nil {
			return fmt.Errorf("bulk update execution_status=%s: %w", status, err)
		}
	}
	return nil
}
