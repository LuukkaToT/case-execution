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

// UtCaseRepository reads the ut_case catalogue. Writes happen upstream and
// are not modelled here.
type UtCaseRepository struct {
	root *gorm.DB
}

// NewUtCaseRepository constructs a repository bound to root.
func NewUtCaseRepository(root *gorm.DB) repository.UtCaseRepository {
	return &UtCaseRepository{root: root}
}

// FindByID returns nil, nil when the id is absent.
func (r *UtCaseRepository) FindByID(ctx context.Context, caseID int64) (*entity.UtCase, error) {
	db := FromCtx(ctx, r.root)
	var row po.UtCase
	err := db.First(&row, caseID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select ut_case by id: %w", err)
	}
	return row.ToEntity(), nil
}

// CountByVersion returns the row count; used by the batch RPC to populate
// total_planned in the progress stream.
func (r *UtCaseRepository) CountByVersion(ctx context.Context, v vo.Version) (int64, error) {
	db := FromCtx(ctx, r.root)
	var n int64
	if err := db.Model(&po.UtCase{}).Where("version = ?", string(v)).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count ut_case by version: %w", err)
	}
	return n, nil
}

// CountByChannelVersion is the (channel, version) variant.
func (r *UtCaseRepository) CountByChannelVersion(ctx context.Context, ch vo.Channel, v vo.Version) (int64, error) {
	db := FromCtx(ctx, r.root)
	var n int64
	if err := db.Model(&po.UtCase{}).
		Where("channel = ? AND version = ?", string(ch), string(v)).
		Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count ut_case by channel+version: %w", err)
	}
	return n, nil
}

// ScanByVersion walks the catalogue via gorm.FindInBatches, which internally
// paginates by primary key range - no full load into memory and no server-side
// cursor to babysit.
func (r *UtCaseRepository) ScanByVersion(
	ctx context.Context, v vo.Version, size int,
	fn repository.UtCaseScanFn,
) error {
	return r.scan(ctx, size, fn, func(db *gorm.DB) *gorm.DB {
		return db.Where("version = ?", string(v))
	})
}

// ScanByChannelVersion is the (channel, version) variant.
func (r *UtCaseRepository) ScanByChannelVersion(
	ctx context.Context, ch vo.Channel, v vo.Version, size int,
	fn repository.UtCaseScanFn,
) error {
	return r.scan(ctx, size, fn, func(db *gorm.DB) *gorm.DB {
		return db.Where("channel = ? AND version = ?", string(ch), string(v))
	})
}

// scan is the shared streaming body. Aborting mid-scan is best-effort:
// FindInBatches will invoke the callback at most once more after ctx cancels
// because the outer gorm cannot observe ctx between the SQL fetch and the
// Go callback. The callback itself checks ctx and returns fast, so this
// extra invocation is harmless.
func (r *UtCaseRepository) scan(
	ctx context.Context, size int, fn repository.UtCaseScanFn,
	where func(*gorm.DB) *gorm.DB,
) error {
	if size <= 0 {
		size = 1000
	}
	db := FromCtx(ctx, r.root)
	var rows []po.UtCase
	tx := where(db).FindInBatches(&rows, size, func(tx *gorm.DB, _ int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := make([]*entity.UtCase, len(rows))
		for i := range rows {
			batch[i] = rows[i].ToEntity()
		}
		return fn(batch)
	})
	if tx.Error != nil {
		return fmt.Errorf("scan ut_case: %w", tx.Error)
	}
	return nil
}
