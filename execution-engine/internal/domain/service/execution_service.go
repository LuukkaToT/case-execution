// Package service hosts pure-domain services: IO-free behaviour that spans
// more than one aggregate instance or coordinates several entities.
package service

import (
	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
	"execution-engine/pkg/logger"

	"go.uber.org/zap"
)

// ExecutionService owns domain-level orchestration rules (state transitions
// over a batch), matching the Python ExecutionService. It does not perform
// IO; repositories and MQ clients are injected at the usecase layer.
type ExecutionService struct{}

// New returns a zero-value service; kept as a constructor for DI symmetry.
func New() *ExecutionService { return &ExecutionService{} }

// CreateExecutionRecord builds a fresh aggregate in StatusInit.
func (s *ExecutionService) CreateExecutionRecord(caseID int64, v vo.Version, user string) *entity.ExecutionRecord {
	return entity.NewExecutionRecord(caseID, v, user)
}

// CreateBatchRecords materialises one ExecutionRecord per case and collects
// the case_id -> case_name mapping needed later by BatchAssemble. Returns
// (records, nameMap) matching the Python tuple return.
func (s *ExecutionService) CreateBatchRecords(cases []*entity.UtCase, v vo.Version, user string) ([]*entity.ExecutionRecord, map[int64]string) {
	records := make([]*entity.ExecutionRecord, 0, len(cases))
	nameMap := make(map[int64]string, len(cases))
	for _, c := range cases {
		records = append(records, entity.NewExecutionRecord(c.CaseID, v, user))
		nameMap[c.CaseID] = c.CaseName
	}
	return records, nameMap
}

// DispatchBatch mutates each record's status from StatusInit according to
// the BatchResult emitted by the MQ client. Records whose id appears in
// neither partition are left untouched (mirrors the Python "else: pass"),
// so the caller still has a chance to reclaim them via a separate path.
//
// Illegal transitions are swallowed with a log line rather than bubbled up:
// by construction records reach this method in StatusInit, so illegal
// transitions indicate a caller bug worth logging but not worth aborting
// the whole batch for.
func (s *ExecutionService) DispatchBatch(records []*entity.ExecutionRecord, result vo.BatchResult) {
	success := make(map[int64]struct{}, len(result.SuccessIDs))
	failed := make(map[int64]struct{}, len(result.FailedIDs))
	for _, id := range result.SuccessIDs {
		success[id] = struct{}{}
	}
	for _, id := range result.FailedIDs {
		failed[id] = struct{}{}
	}
	for _, r := range records {
		var transitErr error
		if _, ok := failed[r.ExecutionID]; ok {
			transitErr = r.MarkAsFailed()
		} else if _, ok := success[r.ExecutionID]; ok {
			transitErr = r.MarkAsWait()
		}
		if transitErr != nil {
			logger.L().Warn("illegal status transition during dispatch",
				zap.Int64("execution_id", r.ExecutionID),
				zap.Error(transitErr))
		}
	}
}
