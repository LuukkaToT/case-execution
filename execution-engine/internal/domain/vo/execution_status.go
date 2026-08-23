package vo

// ExecutionStatus 对应 execution_record 表中的持久化状态字段。
//
// 下发引擎只写 WAIT（已受理，等待执行机）和 DISPATCH_FAILED（消息确定未能投递）。
// 投递过程由 dispatch_outbox 维护。RUNNING、SUCCESS、FAILED 表示业务执行结果，
// 由执行机回调 Python Web 更新；这里仍完整建模，以便状态机拒绝非法迁移。
type ExecutionStatus string

const (
	StatusWait           ExecutionStatus = "WAIT"
	StatusRunning        ExecutionStatus = "RUNNING"
	StatusSuccess        ExecutionStatus = "SUCCESS"
	StatusFailed         ExecutionStatus = "FAILED"
	StatusDispatchFailed ExecutionStatus = "DISPATCH_FAILED"
)

// IsTerminal 判断当前状态是否为不可继续迁移的终态。
func (s ExecutionStatus) IsTerminal() bool {
	return s == StatusSuccess || s == StatusFailed || s == StatusDispatchFailed
}

// CanTransitTo 判断 s 到 next 是否为合法状态机边。
//
// 合法迁移：
//
//	WAIT            -> RUNNING | SUCCESS | FAILED | DISPATCH_FAILED
//	RUNNING         -> SUCCESS | FAILED
//	SUCCESS | FAILED | DISPATCH_FAILED 为终态
func (s ExecutionStatus) CanTransitTo(next ExecutionStatus) bool {
	switch s {
	case StatusWait:
		return next == StatusRunning || next == StatusFailed || next == StatusSuccess || next == StatusDispatchFailed
	case StatusRunning:
		return next == StatusSuccess || next == StatusFailed
	default:
		return false
	}
}
