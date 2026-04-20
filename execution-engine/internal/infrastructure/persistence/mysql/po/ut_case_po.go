package po

import (
	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// UtCase is the gorm PO for the ut_case table (the catalogue of test cases).
type UtCase struct {
	CaseID   int64  `gorm:"column:case_id;primaryKey"`
	CaseName string `gorm:"column:case_name;size:255"`
	Version  string `gorm:"column:version;size:64;index"`
	Channel  string `gorm:"column:channel;size:64;index"`
}

// TableName pins the physical table.
func (UtCase) TableName() string { return "ut_case" }

// ToEntity lifts the PO into the domain entity.
func (p *UtCase) ToEntity() *entity.UtCase {
	return &entity.UtCase{
		CaseID:   p.CaseID,
		CaseName: p.CaseName,
		Version:  vo.Version(p.Version),
		Channel:  vo.Channel(p.Channel),
	}
}
