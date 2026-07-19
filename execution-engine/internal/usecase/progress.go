// Package usecase 包含应用服务，负责在 gRPC handler、领域层和基础设施层之间编排流程。
package usecase

// Progress 是单个分片下发结果的分层无关表示。gRPC handler 会把它转换为
// BatchDispatchProgress，使 usecase 不依赖 protobuf 并可独立单元测试。
type Progress struct {
	ChunkIndex      int32
	ChunkSize       int32
	SuccessIDs      []int64
	FailedIDs       []int64
	TotalDispatched int32
	TotalPlanned    int32
	ChunkError      string
}

// ProgressFn 在每个分片完成后调用一次。返回非 nil error（例如客户端断开
// 导致 gRPC stream.Send 失败）时，取消剩余流水线任务。
type ProgressFn func(Progress) error
