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

// UtCaseRepository 读取 ut_case 用例目录，写入由上游负责，本引擎不建模。
type UtCaseRepository struct {
	root *gorm.DB
}

// NewUtCaseRepository 创建绑定根数据库句柄的仓储。
func NewUtCaseRepository(root *gorm.DB) repository.UtCaseRepository {
	return &UtCaseRepository{root: root}
}

// FindByID 在记录不存在时返回 nil, nil。
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

// CountByVersion 返回指定版本的记录数，用于填充批量 RPC 进度中的 total_planned。
func (r *UtCaseRepository) CountByVersion(ctx context.Context, v vo.Version) (int64, error) {
	db := FromCtx(ctx, r.root)
	var n int64
	if err := db.Model(&po.UtCase{}).Where("version = ?", string(v)).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count ut_case by version: %w", err)
	}
	return n, nil
}

// CountByChannelVersion 返回指定渠道和版本的记录数。
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

// ScanByVersion 使用 gorm.FindInBatches 按主键范围分页遍历目录，无需一次性
// 加载全部数据，也不需要维护服务端游标。
func (r *UtCaseRepository) ScanByVersion(
	ctx context.Context, v vo.Version, size int,
	fn repository.UtCaseScanFn,
) error {
	return r.scan(ctx, size, fn, func(db *gorm.DB) *gorm.DB {
		return db.Where("version = ?", string(v))
	})
}

// ScanByChannelVersion 是按渠道和版本过滤的分页扫描。
func (r *UtCaseRepository) ScanByChannelVersion(
	ctx context.Context, ch vo.Channel, v vo.Version, size int,
	fn repository.UtCaseScanFn,
) error {
	return r.scan(ctx, size, fn, func(db *gorm.DB) *gorm.DB {
		return db.Where("channel = ? AND version = ?", string(ch), string(v))
	})
}

// scan 是共用的流式扫描实现。中途取消为尽力而为：ctx 取消后，FindInBatches
// 最多可能额外调用一次回调，因为 GORM 在 SQL 返回和 Go 回调之间无法立即
// 感知取消；回调会自行检查 ctx 并快速返回，因此不会继续处理数据。
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

// 明天搞明白这里！！！ 是怎么做到一批一批扫描，一批一批调用函数发的？
