// Package po holds gorm persistent objects and mapping helpers.
// PO structs are intentionally kept separate from domain entities so the
// persistent schema can evolve without leaking tags and DB naming into the
// domain package.
package po

import (
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// ExecutionRecord is the gorm PO for the execution_record table.
type ExecutionRecord struct {
	ExecutionID     int64      `gorm:"column:execution_id;primaryKey;autoIncrement"`
	CaseID          int64      `gorm:"column:case_id;index"`
	Version         string     `gorm:"column:version;size:64;index"`
	CreateBy        string     `gorm:"column:create_by;size:64"`
	ExecutionStatus string     `gorm:"column:execution_status;size:16;index"`
	CreateAt        time.Time  `gorm:"column:create_at;autoCreateTime"`
	ExecuteAt       *time.Time `gorm:"column:execute_at"`
	FinishAt        *time.Time `gorm:"column:finish_at"`
}

// TableName pins the physical table.
func (ExecutionRecord) TableName() string { return "execution_record" }

// ToEntity converts a PO back into the domain aggregate.
func (p *ExecutionRecord) ToEntity() *entity.ExecutionRecord {
	return &entity.ExecutionRecord{
		ExecutionID:     p.ExecutionID,
		CaseID:          p.CaseID,
		Version:         vo.Version(p.Version),
		CreateBy:        p.CreateBy,
		ExecutionStatus: vo.ExecutionStatus(p.ExecutionStatus),
		CreateAt:        p.CreateAt,
		ExecuteAt:       p.ExecuteAt,
		FinishAt:        p.FinishAt,
	}
}

// FromExecutionRecord maps an aggregate into a PO.
func FromExecutionRecord(r *entity.ExecutionRecord) *ExecutionRecord {
	return &ExecutionRecord{
		ExecutionID:     r.ExecutionID,
		CaseID:          r.CaseID,
		Version:         string(r.Version),
		CreateBy:        r.CreateBy,
		ExecutionStatus: string(r.ExecutionStatus),
		CreateAt:        r.CreateAt,
		ExecuteAt:       r.ExecuteAt,
		FinishAt:        r.FinishAt,
	}
}
