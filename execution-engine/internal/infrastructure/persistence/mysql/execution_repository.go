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
	"gorm.io/gorm/clause"
)

// ExecutionRepository 是 ExecutionRecord 的 MySQL 仓储实现。
type ExecutionRepository struct {
	root *gorm.DB
}

// NewExecutionRepository 创建绑定根 GORM 句柄的仓储。
func NewExecutionRepository(root *gorm.DB) repository.ExecutionRepository {
	return &ExecutionRepository{root: root}
}

// Add 插入单条记录，并将数据库分配的主键回填到 r。
func (r *ExecutionRepository) Add(ctx context.Context, record *entity.ExecutionRecord) error {
	db := FromCtx(ctx, r.root)
	row := po.FromExecutionRecord(record)
	query := db
	if record.RequestID != "" {
		query = query.Clauses(clause.OnConflict{DoNothing: true})
	}
	if err := query.Create(row).Error; err != nil {
		return fmt.Errorf("insert execution_record: %w", err)
	}
	if record.RequestID != "" {
		if err := db.Where("request_id = ? AND case_id = ?", record.RequestID, record.CaseID).
			First(row).Error; err != nil {
			return fmt.Errorf("select idempotent execution_record: %w", err)
		}
	}
	copyExecutionRecord(record, row.ToEntity())
	return nil
}

// BatchAdd 每 200 条执行一次子批量插入，避免超过 max_allowed_packet，并限制
// 单条 SQL 的延迟。
func (r *ExecutionRepository) BatchAdd(ctx context.Context, records []*entity.ExecutionRecord) error {
	if len(records) == 0 {
		return nil
	}
	db := FromCtx(ctx, r.root)
	rows := make([]*po.ExecutionRecord, 0, len(records))
	for _, rec := range records {
		rows = append(rows, po.FromExecutionRecord(rec))
	}
	requestID := records[0].RequestID
	for _, record := range records {
		if record.RequestID != requestID {
			return fmt.Errorf("batch insert execution_record: mixed request_id values")
		}
	}
	query := db
	if requestID != "" {
		query = query.Clauses(clause.OnConflict{DoNothing: true})
	}
	if err := query.CreateInBatches(&rows, 200).Error; err != nil {
		return fmt.Errorf("batch insert execution_record: %w", err)
	}
	if requestID == "" {
		for i, row := range rows {
			copyExecutionRecord(records[i], row.ToEntity())
		}
		return nil
	}

	caseIDs := make([]int64, 0, len(records))
	for _, record := range records {
		caseIDs = append(caseIDs, record.CaseID)
	}
	var persisted []po.ExecutionRecord
	if err := db.Where("request_id = ? AND case_id IN ?", requestID, caseIDs).Find(&persisted).Error; err != nil {
		return fmt.Errorf("select idempotent execution_record batch: %w", err)
	}
	byCaseID := make(map[int64]*po.ExecutionRecord, len(persisted))
	for i := range persisted {
		byCaseID[persisted[i].CaseID] = &persisted[i]
	}
	for _, record := range records {
		row := byCaseID[record.CaseID]
		if row == nil {
			return fmt.Errorf("select idempotent execution_record batch: case_id=%d missing", record.CaseID)
		}
		copyExecutionRecord(record, row.ToEntity())
	}
	return nil
}

func copyExecutionRecord(dst, src *entity.ExecutionRecord) {
	*dst = *src
}

// FindByID 在无匹配记录时返回 nil, nil，领域层将“不存在”和“查询错误”区分处理，
// 与 Python 适配器返回 None 的语义一致。
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

// Save 只更新 status、execute_at、finish_at 三个可变字段，避免覆盖 Python Web
// 对其他字段的并发写入。
func (r *ExecutionRepository) Save(ctx context.Context, record *entity.ExecutionRecord) error {
	db := FromCtx(ctx, r.root)
	res := db.Model(&po.ExecutionRecord{}).
		Where("execution_id = ? AND execution_status = ?", record.ExecutionID, string(vo.StatusWait)).
		Updates(map[string]any{
			"execution_status": string(record.ExecutionStatus),
			"execute_at":       record.ExecuteAt,
			"finish_at":        record.FinishAt,
		})
	if res.Error != nil {
		return fmt.Errorf("update execution_record: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// 下发回写 DISPATCH_FAILED 之前，执行机可能已把 WAIT 推进到 RUNNING/SUCCESS。
		// 此时视为成功，不回退状态，也不返回误导性的未找到错误。
		var existing po.ExecutionRecord
		err := db.Select("execution_id").First(&existing, record.ExecutionID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("update execution_record: no row matched id=%d", record.ExecutionID)
		}
		if err != nil {
			return fmt.Errorf("verify execution_record after conditional update: %w", err)
		}
	}
	return nil
}

// BatchUpdateStatus 按目标状态分组，每种状态最多执行一条 UPDATE。下发流水线
// 实际只把确定失败的记录写成 DISPATCH_FAILED，成功记录保持创建时的 WAIT。更新条件
// 限定为 WAIT，防止快速执行机回调后的 RUNNING/SUCCESS 被回退；DISPATCH_FAILED 同时
// 持久化 finish_at。
func (r *ExecutionRepository) BatchUpdateStatus(ctx context.Context, records []*entity.ExecutionRecord) error {
	if len(records) == 0 {
		return nil
	}
	db := FromCtx(ctx, r.root)
	grouped := make(map[vo.ExecutionStatus][]int64, 1)
	for _, rec := range records {
		// WAIT 表示无需改写执行记录；投递进度由 Outbox 维护，Relay 负责补偿。
		if rec.ExecutionStatus == vo.StatusWait {
			continue
		}
		grouped[rec.ExecutionStatus] = append(grouped[rec.ExecutionStatus], rec.ExecutionID)
	}
	for status, ids := range grouped {
		updates := map[string]any{"execution_status": string(status)}
		if status.IsTerminal() {
			updates["finish_at"] = recordFinishTime(records, ids)
		}
		result := db.Model(&po.ExecutionRecord{}).
			Where("execution_id IN ? AND execution_status = ?", ids, string(vo.StatusWait)).
			Updates(updates)
		if result.Error != nil {
			return fmt.Errorf("bulk update execution_status=%s: %w", status, result.Error)
		}
		uniqueIDs := uniqueInt64s(ids)
		if result.RowsAffected < int64(len(uniqueIDs)) {
			// 未命中可能是执行机已经推进了状态，也可能是记录被意外删除；只容忍前一种情况。
			var existing int64
			if err := db.Model(&po.ExecutionRecord{}).Where("execution_id IN ?", uniqueIDs).Count(&existing).Error; err != nil {
				return fmt.Errorf("verify execution_record batch after conditional update: %w", err)
			}
			if existing != int64(len(uniqueIDs)) {
				return fmt.Errorf("bulk update execution_status=%s: one or more rows are missing", status)
			}
		}
	}
	return nil
}

func recordFinishTime(records []*entity.ExecutionRecord, ids []int64) any {
	wanted := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	for _, record := range records {
		if _, ok := wanted[record.ExecutionID]; ok && record.FinishAt != nil {
			return *record.FinishAt
		}
	}
	return nil
}
