package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OutboxRepository struct {
	root *gorm.DB
}

func NewOutboxRepository(root *gorm.DB) repository.OutboxRepository {
	return &OutboxRepository{root: root}
}

func (r *OutboxRepository) Add(ctx context.Context, message *entity.OutboxMessage) error {
	db := FromCtx(ctx, r.root)
	row := po.FromOutboxMessage(message)
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error; err != nil {
		return fmt.Errorf("insert dispatch_outbox: %w", err)
	}
	if err := db.Where("execution_id = ?", message.ExecutionID).First(row).Error; err != nil {
		return fmt.Errorf("select idempotent dispatch_outbox: %w", err)
	}
	message.OutboxID = row.OutboxID
	message.CreatedAt = row.CreatedAt
	message.UpdatedAt = row.UpdatedAt
	return nil
}

func (r *OutboxRepository) BatchAdd(ctx context.Context, messages []*entity.OutboxMessage) error {
	if len(messages) == 0 {
		return nil
	}
	rows := make([]*po.OutboxMessage, 0, len(messages))
	for _, message := range messages {
		rows = append(rows, po.FromOutboxMessage(message))
	}
	db := FromCtx(ctx, r.root)
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(&rows, 200).Error; err != nil {
		return fmt.Errorf("batch insert dispatch_outbox: %w", err)
	}
	executionIDs := make([]int64, 0, len(messages))
	for _, message := range messages {
		executionIDs = append(executionIDs, message.ExecutionID)
	}
	var persisted []po.OutboxMessage
	if err := db.Where("execution_id IN ?", executionIDs).Find(&persisted).Error; err != nil {
		return fmt.Errorf("select idempotent dispatch_outbox batch: %w", err)
	}
	byExecutionID := make(map[int64]*po.OutboxMessage, len(persisted))
	for i := range persisted {
		byExecutionID[persisted[i].ExecutionID] = &persisted[i]
	}
	for _, message := range messages {
		row := byExecutionID[message.ExecutionID]
		if row == nil {
			return fmt.Errorf("select idempotent dispatch_outbox batch: execution_id=%d missing", message.ExecutionID)
		}
		message.OutboxID = row.OutboxID
		message.CreatedAt = row.CreatedAt
		message.UpdatedAt = row.UpdatedAt
	}
	return nil
}

func (r *OutboxRepository) ClaimPending(
	ctx context.Context,
	limit int,
	now time.Time,
	leaseUntil time.Time,
) ([]*entity.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	leaseToken, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	var claimed []*entity.OutboxMessage
	err = r.root.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []po.OutboxMessage
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("(state = ? AND available_at <= ?) OR (state = ? AND lease_until <= ?)",
				string(entity.OutboxPending), now, string(entity.OutboxProcessing), now).
			Order("outbox_id ASC").
			Limit(limit).
			Find(&rows).Error
		if err != nil {
			return fmt.Errorf("select claimable dispatch_outbox: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		ids := make([]int64, 0, len(rows))
		for i := range rows {
			ids = append(ids, rows[i].OutboxID)
		}
		if err := tx.Model(&po.OutboxMessage{}).
			Where("outbox_id IN ?", ids).
			Updates(map[string]any{
				"state":       string(entity.OutboxProcessing),
				"lease_until": leaseUntil,
				"lease_token": leaseToken,
				"attempts":    gorm.Expr("attempts + 1"),
			}).Error; err != nil {
			return fmt.Errorf("lease dispatch_outbox: %w", err)
		}
		claimed = make([]*entity.OutboxMessage, 0, len(rows))
		for i := range rows {
			rows[i].State = string(entity.OutboxProcessing)
			rows[i].Attempts++
			rows[i].LeaseUntil = &leaseUntil
			rows[i].LeaseToken = leaseToken
			claimed = append(claimed, rows[i].ToEntity())
		}
		return nil
	})
	return claimed, err
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, executionIDs []int64, leaseToken string, publishedAt time.Time) error {
	return r.updateByExecutionIDs(ctx, executionIDs, leaseToken, map[string]any{
		"state":        string(entity.OutboxPublished),
		"published_at": publishedAt,
		"lease_until":  nil,
		"lease_token":  "",
		"last_error":   "",
	})
}

func (r *OutboxRepository) MarkRetry(ctx context.Context, executionIDs []int64, leaseToken string, availableAt time.Time, lastError string) error {
	return r.updateByExecutionIDs(ctx, executionIDs, leaseToken, map[string]any{
		"state":        string(entity.OutboxPending),
		"available_at": availableAt,
		"lease_until":  nil,
		"lease_token":  "",
		"last_error":   truncateOutboxError(lastError),
	})
}

func (r *OutboxRepository) MarkDead(ctx context.Context, executionIDs []int64, leaseToken string, lastError string) error {
	return r.updateByExecutionIDs(ctx, executionIDs, leaseToken, map[string]any{
		"state":       string(entity.OutboxDead),
		"lease_until": nil,
		"lease_token": "",
		"last_error":  truncateOutboxError(lastError),
	})
}

func (r *OutboxRepository) updateByExecutionIDs(ctx context.Context, executionIDs []int64, leaseToken string, updates map[string]any) error {
	if len(executionIDs) == 0 {
		return nil
	}
	query := FromCtx(ctx, r.root).Model(&po.OutboxMessage{}).Where("execution_id IN ?", executionIDs)
	if leaseToken == "" {
		query = query.Where("state = ?", string(entity.OutboxPending))
	} else {
		query = query.Where("state = ? AND lease_token = ?", string(entity.OutboxProcessing), leaseToken)
	}
	result := query.Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update dispatch_outbox: %w", result.Error)
	}
	if result.RowsAffected != int64(len(uniqueInt64s(executionIDs))) {
		return fmt.Errorf("update dispatch_outbox: lease lost or state changed")
	}
	return nil
}

func newLeaseToken() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate outbox lease token: %w", err)
	}
	return hex.EncodeToString(random), nil
}

func uniqueInt64s(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	unique := make([]int64, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func truncateOutboxError(message string) string {
	const maxBytes = 1024
	if len(message) <= maxBytes {
		return message
	}
	return message[:maxBytes]
}
