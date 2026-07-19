package vo

// BatchResult 表示一批任务发送到 MQ 后的下发结果。
//
// SuccessIDs 和 FailedIDs 对输入 execution_id 进行分区，确认超时的任务归入
// FailedIDs。ErrorMessage 保存连接异常、超时等批次错误；单条 nack 主要通过
// FailedIDs 表达。
type BatchResult struct {
	SuccessIDs   []int64
	FailedIDs    []int64
	ErrorMessage string
}
