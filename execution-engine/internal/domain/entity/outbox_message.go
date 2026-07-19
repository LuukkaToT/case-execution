package entity

import (
	"time"

	"execution-engine/internal/domain/vo"
)

// OutboxState 表示一条执行消息的持久化投递状态。
type OutboxState string

const (
	OutboxPending    OutboxState = "PENDING"    // 等待快速路径或 Relay 投递。
	OutboxProcessing OutboxState = "PROCESSING" // 已由某个 Relay 实例持有租约。
	OutboxPublished  OutboxState = "PUBLISHED"  // 已收到 RabbitMQ 发布确认。
	OutboxDead       OutboxState = "DEAD"       // 达到重试上限或确定性发布失败。
)

// OutboxMessage 与 ExecutionRecord 在同一个 MySQL 事务中写入，用于消除
// DB 成功、MQ 发布前进程崩溃造成的消息丢失窗口。投递语义为 at-least-once：
// 收到发布确认后若数据库回写失败，relay 可能再次发送相同 execution_id，
// 因此消费者必须按 execution_id 去重。
type OutboxMessage struct {
	OutboxID    int64       // Outbox 表自增主键，只用于持久化扫描排序。
	ExecutionID int64       // 执行记录编号，同时作为消息幂等键和 AMQP message_id。
	CaseID      int64       // 用例编号；与名称、版本共同组成发布载荷快照。
	CaseName    string      // 创建 Outbox 时保存的用例名称，Relay 无需回查用例表。
	Version     vo.Version  // 用例版本，同时作为 RabbitMQ routing key。
	State       OutboxState // 当前可靠投递状态。
	Attempts    int         // 领取次数；每次成功获得租约时递增，而非发布后才递增。
	AvailableAt time.Time   // 下一次允许领取的时间，用于快速路径保护和失败退避。
	LeaseUntil  *time.Time  // PROCESSING 租约到期时间；实例崩溃后其他实例可重新领取。
	LeaseToken  string      // 每次领取生成的新令牌，拒绝旧持有者迟到回写。
	LastError   string      // 最近一次发布失败摘要，限制在 1024 字节以内。
	CreatedAt   time.Time   // 消息创建时间。
	UpdatedAt   time.Time   // 状态、租约或错误信息最后更新时间。
	PublishedAt *time.Time  // 收到发布确认并写回数据库的时间。
}

// NewOutboxMessage 根据执行记录创建待投递消息，并保存发布所需的载荷快照。
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

// Task 把持久化消息还原成执行器端口使用的下发任务。
func (m *OutboxMessage) Task() ExecutionTask {
	return ExecutionTask{
		ExecutionID: m.ExecutionID,
		CaseID:      m.CaseID,
		CaseName:    m.CaseName,
		Version:     m.Version,
	}
}
