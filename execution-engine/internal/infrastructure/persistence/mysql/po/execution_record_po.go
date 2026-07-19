// Package po 包含 GORM 持久化对象和映射函数。PO 与领域实体分离，使数据库
// schema 可以独立演进，避免 GORM tag 和数据库命名泄漏到领域层。
package po

import (
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// ExecutionRecord 是 execution_record 表对应的 GORM PO。
type ExecutionRecord struct {
	ExecutionID     int64      `gorm:"column:execution_id;primaryKey;autoIncrement"`
	RequestID       *string    `gorm:"column:request_id;size:64;uniqueIndex:uk_execution_request_case,priority:1"`
	CaseID          int64      `gorm:"column:case_id;index;uniqueIndex:uk_execution_request_case,priority:2"`
	Version         string     `gorm:"column:version;size:64;index"`
	CreateBy        string     `gorm:"column:create_by;size:64"`
	ExecutionStatus string     `gorm:"column:execution_status;size:16;index"`
	CreateAt        time.Time  `gorm:"column:create_at;autoCreateTime"`
	ExecuteAt       *time.Time `gorm:"column:execute_at"`
	FinishAt        *time.Time `gorm:"column:finish_at"`
}

// TableName 指定物理表名。
func (ExecutionRecord) TableName() string { return "execution_record" }

// ToEntity 将 PO 转换为领域聚合。
func (p *ExecutionRecord) ToEntity() *entity.ExecutionRecord {
	return &entity.ExecutionRecord{
		ExecutionID:     p.ExecutionID,
		RequestID:       stringValue(p.RequestID),
		CaseID:          p.CaseID,
		Version:         vo.Version(p.Version),
		CreateBy:        p.CreateBy,
		ExecutionStatus: vo.ExecutionStatus(p.ExecutionStatus),
		CreateAt:        p.CreateAt,
		ExecuteAt:       p.ExecuteAt,
		FinishAt:        p.FinishAt,
	}
}

// FromExecutionRecord 将领域聚合映射为 PO。
func FromExecutionRecord(r *entity.ExecutionRecord) *ExecutionRecord {
	return &ExecutionRecord{
		ExecutionID:     r.ExecutionID,
		RequestID:       optionalString(r.RequestID),
		CaseID:          r.CaseID,
		Version:         string(r.Version),
		CreateBy:        r.CreateBy,
		ExecutionStatus: string(r.ExecutionStatus),
		CreateAt:        r.CreateAt,
		ExecuteAt:       r.ExecuteAt,
		FinishAt:        r.FinishAt,
	}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
