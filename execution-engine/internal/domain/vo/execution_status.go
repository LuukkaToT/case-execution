package vo

// ExecutionStatus 对应 execution_record 表中的持久化状态字段。
//
// 引擎在下发后只写 WAIT（MQ 发布成功，等待执行机）或 FAILED（下发失败）。
// RUNNING、SUCCESS 等后续状态由执行机回调 Python Web 更新；这里仍完整建模，
// 以便状态机拒绝非法迁移。
type ExecutionStatus string

const (
	StatusInit    ExecutionStatus = "INIT"
	StatusWait    ExecutionStatus = "WAIT"
	StatusRunning ExecutionStatus = "RUNNING"
	StatusSuccess ExecutionStatus = "SUCCESS"
	StatusFailed  ExecutionStatus = "FAILED"
)

// IsTerminal 判断当前状态是否为不可继续迁移的终态。
func (s ExecutionStatus) IsTerminal() bool {
	return s == StatusSuccess || s == StatusFailed
}

// CanTransitTo 判断 s 到 next 是否为合法状态机边。
//
// 合法迁移：
//
//	INIT    -> WAIT | FAILED
//	WAIT    -> RUNNING | FAILED | SUCCESS
//	RUNNING -> SUCCESS | FAILED
//	SUCCESS | FAILED 为终态
func (s ExecutionStatus) CanTransitTo(next ExecutionStatus) bool {
	switch s {
	case StatusInit:
		return next == StatusWait || next == StatusFailed
	case StatusWait:
		return next == StatusRunning || next == StatusFailed || next == StatusSuccess
	case StatusRunning:
		return next == StatusSuccess || next == StatusFailed
	default:
		return false
	}
}
