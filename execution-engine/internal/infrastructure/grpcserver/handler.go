// Package grpcserver 是驱动端适配器，负责把 gRPC 请求转换为 usecase 调用，
// 并通过服务端流把 usecase.Progress 返回给 Python Web。
package grpcserver

import (
	"context"
	"errors"

	pb "execution-engine/api/gen/taskexecution/v1"
	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/vo"
	"execution-engine/internal/usecase"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler 通过委托 usecase 实现 TaskExecutionServiceServer。这里只负责请求
// 响应转换和领域错误到 gRPC 状态码的映射，业务规则位于更内层。
type Handler struct {
	pb.UnimplementedTaskExecutionServiceServer
	app *usecase.DispatchAppService
	log *zap.Logger
}

// NewHandler 创建并装配 handler。
func NewHandler(app *usecase.DispatchAppService, log *zap.Logger) *Handler {
	if log == nil {
		log = zap.NewNop()
	}
	return &Handler{app: app, log: log}
}

// Register 将 handler 注册到 grpc.Server。
func (h *Handler) Register(s *grpc.Server) {
	pb.RegisterTaskExecutionServiceServer(s, h)
}

// ExecuteCase 是单用例下发的一元 RPC。
func (h *Handler) ExecuteCase(ctx context.Context, req *pb.ExecuteCaseRequest) (*pb.ExecuteCaseResponse, error) {
	if req == nil || req.CaseId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "case_id is required")
	}
	requestID, err := usecase.NormalizeRequestID(req.RequestId)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	execID, stat, err := h.app.ExecuteCaseWithRequest(ctx, requestID, req.CaseId, vo.NewVersion(req.Version), req.User)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return &pb.ExecuteCaseResponse{
		ExecutionId: execID,
		Status:      toPbDispatchStatus(stat),
		RequestId:   requestID,
	}, nil
}

// ExecuteAllCases 是按版本全量下发的服务端流式 RPC。
func (h *Handler) ExecuteAllCases(
	req *pb.ExecuteAllCasesRequest,
	stream grpc.ServerStreamingServer[pb.BatchDispatchProgress],
) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	requestID, err := usecase.NormalizeRequestID(req.RequestId)
	if err != nil {
		return toGRPCErr(err)
	}
	err = h.app.ExecuteAllCasesWithRequest(
		stream.Context(),
		requestID,
		vo.NewVersion(req.Version),
		req.User,
		progressForwarder(stream, requestID),
	)
	return toGRPCErr(err)
}

// ExecuteChannelCases 是按渠道和版本下发的服务端流式 RPC。
func (h *Handler) ExecuteChannelCases(
	req *pb.ExecuteChannelCasesRequest,
	stream grpc.ServerStreamingServer[pb.BatchDispatchProgress],
) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	requestID, err := usecase.NormalizeRequestID(req.RequestId)
	if err != nil {
		return toGRPCErr(err)
	}
	err = h.app.ExecuteChannelCasesWithRequest(
		stream.Context(),
		requestID,
		vo.NewChannel(req.Channel),
		vo.NewVersion(req.Version),
		req.User,
		progressForwarder(stream, requestID),
	)
	return toGRPCErr(err)
}

// progressForwarder 把 usecase.Progress 转换为 protobuf 进度帧。只有
// dispatch_app_service.go 中的聚合 goroutine 会调用该回调，因此无需为本身
// 不支持并发调用的 stream.Send 额外加锁。
func progressForwarder(stream grpc.ServerStreamingServer[pb.BatchDispatchProgress], requestID string) usecase.ProgressFn {
	return func(p usecase.Progress) error {
		return stream.Send(&pb.BatchDispatchProgress{
			ChunkIndex:          p.ChunkIndex,
			ChunkSize:           p.ChunkSize,
			SuccessExecutionIds: p.SuccessIDs,
			FailedExecutionIds:  p.FailedIDs,
			TotalDispatched:     p.TotalDispatched,
			TotalPlanned:        p.TotalPlanned,
			ChunkError:          p.ChunkError,
			RequestId:           requestID,
		})
	}
}

func toPbDispatchStatus(s vo.ExecutionStatus) pb.DispatchStatus {
	switch s {
	case vo.StatusWait:
		return pb.DispatchStatus_DISPATCH_STATUS_WAIT
	case vo.StatusDispatchFailed:
		return pb.DispatchStatus_DISPATCH_STATUS_FAILED
	default:
		return pb.DispatchStatus_DISPATCH_STATUS_UNSPECIFIED
	}
}

// toGRPCErr 将领域错误映射为 gRPC 状态码，其他错误统一映射为 Internal。
// 调用方是可信的 Python Web，因此保留错误消息用于诊断。
func toGRPCErr(err error) error {
	if err == nil {
		return nil
	}
	// 将 ctx 错误重新映射为 Canceled/DeadlineExceeded，使调用方能够区分主动
	// 取消和引擎内部故障。
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, errs.ErrCaseNotExist):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, errs.ErrExecutionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, errs.ErrIllegalStatusTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, errs.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
