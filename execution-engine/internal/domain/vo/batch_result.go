package vo

// BatchResult is the outcome of dispatching a batch of tasks to the MQ.
//
// SuccessIDs and FailedIDs partition the execution_ids of the input batch
// (modulo in-flight tasks whose confirm timed out, which are reported as
// Failed). ErrorMessage is set only for whole-batch failures (connection
// error, timeout); per-task nack errors are represented by membership in
// FailedIDs without populating ErrorMessage.
type BatchResult struct {
	SuccessIDs   []int64
	FailedIDs    []int64
	ErrorMessage string
}
