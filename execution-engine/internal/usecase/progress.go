// Package usecase contains application services: the orchestration layer
// that bridges grpc handlers to domain + infrastructure.
package usecase

// Progress is the layer-neutral projection of one chunk's dispatch outcome.
// The grpc handler converts this to BatchDispatchProgress protobuf; the
// usecase stays free of protobuf imports so it is unit-testable in isolation.
type Progress struct {
	ChunkIndex      int32
	ChunkSize       int32
	SuccessIDs      []int64
	FailedIDs       []int64
	TotalDispatched int32
	TotalPlanned    int32
	ChunkError      string
}

// ProgressFn is invoked by the usecase once per completed chunk. Return a
// non-nil error (e.g. grpc stream.Send failed because the client disconnected)
// to cancel the remaining pipeline work.
type ProgressFn func(Progress) error
