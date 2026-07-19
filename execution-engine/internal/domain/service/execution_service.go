// Package service 包含纯领域服务，用于协调多个聚合或实体，不执行 IO。
package service

import (
	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
	"execution-engine/pkg/logger"

	"go.uber.org/zap"
)

// ExecutionService 负责批量状态迁移等领域编排规则，与原 Python
// ExecutionService 保持一致。仓储和 MQ 客户端由 usecase 层注入。
type ExecutionService struct{}

// New 返回零值领域服务，保留构造器以统一依赖注入形式。
func New() *ExecutionService { return &ExecutionService{} }

// CreateExecutionRecord 创建一条 INIT 状态的新聚合。
func (s *ExecutionService) CreateExecutionRecord(caseID int64, v vo.Version, user string) *entity.ExecutionRecord {
	return entity.NewExecutionRecord(caseID, v, user)
}

func (s *ExecutionService) CreateExecutionRecordForRequest(requestID string, caseID int64, v vo.Version, user string) *entity.ExecutionRecord {
	return entity.NewExecutionRecordForRequest(requestID, caseID, v, user)
}

// CreateBatchRecords 为每个用例创建一条 ExecutionRecord，并收集后续
// BatchAssemble 所需的 case_id 到 case_name 映射。返回形式与 Python 元组一致。
func (s *ExecutionService) CreateBatchRecords(cases []*entity.UtCase, v vo.Version, user string) ([]*entity.ExecutionRecord, map[int64]string) {
	return s.CreateBatchRecordsForRequest("", cases, v, user)
}

func (s *ExecutionService) CreateBatchRecordsForRequest(requestID string, cases []*entity.UtCase, v vo.Version, user string) ([]*entity.ExecutionRecord, map[int64]string) {
	records := make([]*entity.ExecutionRecord, 0, len(cases))
	nameMap := make(map[int64]string, len(cases))
	for _, c := range cases {
		records = append(records, entity.NewExecutionRecordForRequest(requestID, c.CaseID, v, user))
		nameMap[c.CaseID] = c.CaseName
	}
	return records, nameMap
}

// DispatchBatch 根据 MQ 客户端返回的 BatchResult，把记录从 INIT 迁移到
// 对应状态。两个结果分区都不存在的记录保持不变，留给独立修复流程处理。
//
// 非法迁移只记录日志而不向上返回：按正常流程记录进入本方法时应为 INIT，
// 非法迁移意味着调用方缺陷，不应因此中断整个分片。
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
