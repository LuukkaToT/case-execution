package po

import (
	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// UtCase 是用例目录表 ut_case 对应的 GORM PO。
type UtCase struct {
	CaseID   int64  `gorm:"column:case_id;primaryKey;index:idx_version_caseid,priority:2;index:idx_channel_version_caseid,priority:3"`
	CaseName string `gorm:"column:case_name;size:255"`
	Version  string `gorm:"column:version;size:64;index;index:idx_version_caseid,priority:1;index:idx_channel_version_caseid,priority:2"`
	Channel  string `gorm:"column:channel;size:64;index;index:idx_channel_version_caseid,priority:1"`
}

// TableName 指定物理表名。
func (UtCase) TableName() string { return "ut_case" }

// ToEntity 将 PO 转换为领域实体。
func (p *UtCase) ToEntity() *entity.UtCase {
	return &entity.UtCase{
		CaseID:   p.CaseID,
		CaseName: p.CaseName,
		Version:  vo.Version(p.Version),
		Channel:  vo.Channel(p.Channel),
	}
}
