package vo

// ExecutionStatus models the persistent status column on execution_record.
//
// The engine itself only writes two terminal statuses after dispatch:
// StatusWait (MQ publish succeeded, waiting for worker) and StatusFailed
// (dispatch failed). Non-terminal statuses (INIT, RUNNING) and success are
// owned by Python Web via worker callbacks; we still model them here so the
// state-machine rejects illegal transitions from aged records.
type ExecutionStatus string

const (
	StatusInit    ExecutionStatus = "INIT"
	StatusWait    ExecutionStatus = "WAIT"
	StatusRunning ExecutionStatus = "RUNNING"
	StatusSuccess ExecutionStatus = "SUCCESS"
	StatusFailed  ExecutionStatus = "FAILED"
)

// IsTerminal reports whether the status cannot legally transit further.
func (s ExecutionStatus) IsTerminal() bool {
	return s == StatusSuccess || s == StatusFailed
}

// CanTransitTo returns true iff s -> next is a legal state-machine edge.
//
// Legal edges:
//
//	INIT    -> WAIT | FAILED
//	WAIT    -> RUNNING | FAILED | SUCCESS
//	RUNNING -> SUCCESS | FAILED
//	SUCCESS | FAILED are terminal
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
