package po

import (
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

type OutboxMessage struct {
	OutboxID    int64      `gorm:"column:outbox_id;primaryKey;autoIncrement;index:idx_dispatch_outbox_claim,priority:3;index:idx_dispatch_outbox_reclaim,priority:3"`
	ExecutionID int64      `gorm:"column:execution_id;not null;uniqueIndex:uk_dispatch_outbox_execution"`
	CaseID      int64      `gorm:"column:case_id;not null"`
	CaseName    string     `gorm:"column:case_name;size:255;not null"`
	Version     string     `gorm:"column:version;size:64;not null"`
	State       string     `gorm:"column:state;size:16;not null;index:idx_dispatch_outbox_claim,priority:1;index:idx_dispatch_outbox_reclaim,priority:1"`
	Attempts    int        `gorm:"column:attempts;not null;default:0"`
	AvailableAt time.Time  `gorm:"column:available_at;not null;index:idx_dispatch_outbox_claim,priority:2"`
	LeaseUntil  *time.Time `gorm:"column:lease_until;index:idx_dispatch_outbox_reclaim,priority:2"`
	LeaseToken  string     `gorm:"column:lease_token;size:64;not null;default:''"`
	LastError   string     `gorm:"column:last_error;size:1024;not null;default:''"`
	CreatedAt   time.Time  `gorm:"column:create_at;autoCreateTime"`
	UpdatedAt   time.Time  `gorm:"column:update_at;autoUpdateTime"`
	PublishedAt *time.Time `gorm:"column:published_at"`
}

func (OutboxMessage) TableName() string { return "dispatch_outbox" }

func FromOutboxMessage(message *entity.OutboxMessage) *OutboxMessage {
	return &OutboxMessage{
		OutboxID:    message.OutboxID,
		ExecutionID: message.ExecutionID,
		CaseID:      message.CaseID,
		CaseName:    message.CaseName,
		Version:     string(message.Version),
		State:       string(message.State),
		Attempts:    message.Attempts,
		AvailableAt: message.AvailableAt,
		LeaseUntil:  message.LeaseUntil,
		LeaseToken:  message.LeaseToken,
		LastError:   message.LastError,
		CreatedAt:   message.CreatedAt,
		UpdatedAt:   message.UpdatedAt,
		PublishedAt: message.PublishedAt,
	}
}

func (message *OutboxMessage) ToEntity() *entity.OutboxMessage {
	return &entity.OutboxMessage{
		OutboxID:    message.OutboxID,
		ExecutionID: message.ExecutionID,
		CaseID:      message.CaseID,
		CaseName:    message.CaseName,
		Version:     vo.Version(message.Version),
		State:       entity.OutboxState(message.State),
		Attempts:    message.Attempts,
		AvailableAt: message.AvailableAt,
		LeaseUntil:  message.LeaseUntil,
		LeaseToken:  message.LeaseToken,
		LastError:   message.LastError,
		CreatedAt:   message.CreatedAt,
		UpdatedAt:   message.UpdatedAt,
		PublishedAt: message.PublishedAt,
	}
}
