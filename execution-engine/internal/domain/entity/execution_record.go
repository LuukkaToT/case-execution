package entity

import (
	"time"

	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/vo"
)

// ExecutionRecord is the aggregate root representing one dispatched attempt
// to execute a single test case. Its lifecycle is owned by the dispatch
// engine (INIT -> WAIT | FAILED) and later transitioned by worker callbacks
// on the Python side (WAIT -> RUNNING/SUCCESS/FAILED).
type ExecutionRecord struct {
	ExecutionID     int64
	CaseID          int64
	Version         vo.Version
	CreateBy        string
	ExecutionStatus vo.ExecutionStatus
	CreateAt        time.Time
	ExecuteAt       *time.Time
	FinishAt        *time.Time
}

// NewExecutionRecord is the domain factory matching the Python
// ExecutionRecord.create classmethod. Records start in StatusInit; DB id is
// zero until the repository assigns it on insert.
func NewExecutionRecord(caseID int64, v vo.Version, user string) *ExecutionRecord {
	return &ExecutionRecord{
		CaseID:          caseID,
		Version:         v,
		CreateBy:        user,
		ExecutionStatus: vo.StatusInit,
		CreateAt:        time.Now(),
	}
}

// MarkAsWait transits to StatusWait, enforcing the state machine.
func (r *ExecutionRecord) MarkAsWait() error {
	return r.transit(vo.StatusWait)
}

// MarkAsFailed transits to StatusFailed, enforcing the state machine.
func (r *ExecutionRecord) MarkAsFailed() error {
	return r.transit(vo.StatusFailed)
}

// MarkAsRunning transits to StatusRunning, enforcing the state machine.
// Exposed for completeness; the dispatch engine itself does not call this.
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
