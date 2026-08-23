package entity

import (
	"time"

	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/vo"
)

// ExecutionRecord 是一次单用例下发尝试的聚合根。新记录直接以 WAIT 落库，
// 表示等待执行机处理；消息投递进度由 Outbox 维护。下发确定失败时迁移到
// DISPATCH_FAILED。业务执行结果 RUNNING/SUCCESS/FAILED 由执行机回调
// Python Web 写入。
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
// 新记录从 WAIT 开始，数据库主键在仓储插入后回填。
func NewExecutionRecord(caseID int64, v vo.Version, user string) *ExecutionRecord {
	return NewExecutionRecordForRequest("", caseID, v, user)
}

func NewExecutionRecordForRequest(requestID string, caseID int64, v vo.Version, user string) *ExecutionRecord {
	return &ExecutionRecord{
		RequestID:       requestID,
		CaseID:          caseID,
		Version:         v,
		CreateBy:        user,
		ExecutionStatus: vo.StatusWait,
		CreateAt:        time.Now(),
	}
}

// MarkAsDispatchFailed 按下发失败迁移到 DISPATCH_FAILED。仅下发引擎调用。
func (r *ExecutionRecord) MarkAsDispatchFailed() error {
	return r.transit(vo.StatusDispatchFailed)
}

// MarkAsFailed 按状态机约束迁移到业务失败 FAILED。该方法用于保持模型完整，
// 下发引擎本身不会调用。
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
