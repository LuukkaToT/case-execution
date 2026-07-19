package entity

import (
	"time"

	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/vo"
)

// ExecutionRecord 是一次单用例下发尝试的聚合根。下发引擎负责 INIT 到
// WAIT/FAILED 的迁移，随后由执行机回调 Python Web 完成 WAIT 到
// RUNNING/SUCCESS/FAILED 的迁移。
type ExecutionRecord struct {
	ExecutionID     int64
	RequestID       string
	CaseID          int64
	Version         vo.Version
	CreateBy        string
	ExecutionStatus vo.ExecutionStatus
	CreateAt        time.Time
	ExecuteAt       *time.Time
	FinishAt        *time.Time
}

// NewExecutionRecord 是与 Python ExecutionRecord.create 对应的领域工厂。
// 新记录从 INIT 开始，数据库主键在仓储插入后回填。
func NewExecutionRecord(caseID int64, v vo.Version, user string) *ExecutionRecord {
	return NewExecutionRecordForRequest("", caseID, v, user)
}

func NewExecutionRecordForRequest(requestID string, caseID int64, v vo.Version, user string) *ExecutionRecord {
	return &ExecutionRecord{
		RequestID:       requestID,
		CaseID:          caseID,
		Version:         v,
		CreateBy:        user,
		ExecutionStatus: vo.StatusInit,
		CreateAt:        time.Now(),
	}
}

// MarkAsWait 按状态机约束迁移到 WAIT。
func (r *ExecutionRecord) MarkAsWait() error {
	return r.transit(vo.StatusWait)
}

// MarkAsFailed 按状态机约束迁移到 FAILED。
func (r *ExecutionRecord) MarkAsFailed() error {
	return r.transit(vo.StatusFailed)
}

// MarkAsRunning 按状态机约束迁移到 RUNNING。该方法用于保持模型完整，
// 下发引擎本身不会调用。
func (r *ExecutionRecord) MarkAsRunning() error {
	now := time.Now()
	if err := r.transit(vo.StatusRunning); err != nil {
		return err
	}
	r.ExecuteAt = &now
	return nil
}

func (r *ExecutionRecord) transit(next vo.ExecutionStatus) error {
	if !r.ExecutionStatus.CanTransitTo(next) {
		return errs.NewIllegalStatusTransition(r.ExecutionID, string(r.ExecutionStatus), string(next))
	}
	r.ExecutionStatus = next
	if next.IsTerminal() {
		now := time.Now()
		r.FinishAt = &now
	}
	return nil
}
