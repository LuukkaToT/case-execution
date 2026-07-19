package po

import (
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// OutboxMessage 是 dispatch_outbox 表对应的 GORM PO。
// 表中同时保存发布载荷快照和 Relay 租约元数据，避免恢复投递时依赖用例表当前值。
type OutboxMessage struct {
	OutboxID    int64      `gorm:"column:outbox_id;primaryKey;autoIncrement;index:idx_dispatch_outbox_claim,priority:3;index:idx_dispatch_outbox_reclaim,priority:3;comment:Outbox 自增主键"`
	ExecutionID int64      `gorm:"column:execution_id;not null;uniqueIndex:uk_dispatch_outbox_execution;comment:执行记录编号与消息幂等键"`
	CaseID      int64      `gorm:"column:case_id;not null;comment:用例编号快照"`
	CaseName    string     `gorm:"column:case_name;size:255;not null;comment:用例名称快照"`
	Version     string     `gorm:"column:version;size:64;not null;comment:版本快照与 RabbitMQ 路由键"`
	State       string     `gorm:"column:state;size:16;not null;index:idx_dispatch_outbox_claim,priority:1;index:idx_dispatch_outbox_reclaim,priority:1;comment:投递状态 PENDING PROCESSING PUBLISHED DEAD"`
	Attempts    int        `gorm:"column:attempts;not null;default:0;comment:成功领取租约的累计次数"`
	AvailableAt time.Time  `gorm:"column:available_at;not null;index:idx_dispatch_outbox_claim,priority:2;comment:下一次允许领取时间"`
	LeaseUntil  *time.Time `gorm:"column:lease_until;index:idx_dispatch_outbox_reclaim,priority:2;comment:当前处理租约到期时间"`
	LeaseToken  string     `gorm:"column:lease_token;size:64;not null;default:'';comment:本次领取令牌 防止旧实例迟到写回"`
	LastError   string     `gorm:"column:last_error;size:1024;not null;default:'';comment:最近一次发布失败摘要"`
	CreatedAt   time.Time  `gorm:"column:create_at;autoCreateTime;comment:消息创建时间"`
	UpdatedAt   time.Time  `gorm:"column:update_at;autoUpdateTime;comment:消息最后更新时间"`
	PublishedAt *time.Time `gorm:"column:published_at;comment:发布确认写回时间"`
}

// TableName 指定可靠投递消息表名。
func (OutboxMessage) TableName() string { return "dispatch_outbox" }

// FromOutboxMessage 把领域消息映射为持久化对象。
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

// ToEntity 把持久化对象还原为领域消息。
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
