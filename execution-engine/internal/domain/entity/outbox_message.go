package entity

import (
	"time"

	"execution-engine/internal/domain/vo"
)

// OutboxState 表示一条执行消息的持久化投递状态。
type OutboxState string

const (
	OutboxPending    OutboxState = "PENDING"
	OutboxProcessing OutboxState = "PROCESSING"
	OutboxPublished  OutboxState = "PUBLISHED"
	OutboxDead       OutboxState = "DEAD"
)

// OutboxMessage 与 ExecutionRecord 在同一个 MySQL 事务中写入，用于消除
// DB 成功、MQ 发布前进程崩溃造成的消息丢失窗口。投递语义为 at-least-once：
// 收到发布确认后若数据库回写失败，relay 可能再次发送相同 execution_id，
// 因此消费者必须按 execution_id 去重。
type OutboxMessage struct {
	OutboxID    int64
	ExecutionID int64
	CaseID      int64
	CaseName    string
	Version     vo.Version
	State       OutboxState
	Attempts    int
	AvailableAt time.Time
	LeaseUntil  *time.Time
	LeaseToken  string
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	PublishedAt *time.Time
}

func NewOutboxMessage(record *ExecutionRecord, caseName string, availableAt time.Time) *OutboxMessage {
	return &OutboxMessage{
		ExecutionID: record.ExecutionID,
		CaseID:      record.CaseID,
		CaseName:    caseName,
		Version:     record.Version,
		State:       OutboxPending,
		AvailableAt: availableAt,
	}
}

func (m *OutboxMessage) Task() ExecutionTask {
	return ExecutionTask{
		ExecutionID: m.ExecutionID,
		CaseID:      m.CaseID,
		CaseName:    m.CaseName,
		Version:     m.Version,
	}
}
