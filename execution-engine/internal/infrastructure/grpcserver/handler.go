// Package grpcserver is the driving adapter: it converts gRPC wire
// requests into usecase calls and pipes usecase.Progress frames back to the
// client over the server-side stream.
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

// Handler implements pb.TaskExecutionServiceServer by delegating to the
// usecase. It stays intentionally thin: request/response translation plus
// domain-error-to-grpc-status mapping. All business rules live deeper.
type Handler struct {
	pb.UnimplementedTaskExecutionServiceServer
	app *usecase.DispatchAppService
	log *zap.Logger
}

// NewHandler wires the handler.
func NewHandler(app *usecase.DispatchAppService, log *zap.Logger) *Handler {
	if log == nil {
		log = zap.NewNop()
	}
	return &Handler{app: app, log: log}
}

// Register attaches the handler to a grpc.Server.
func (h *Handler) Register(s *grpc.Server) {
	pb.RegisterTaskExecutionServiceServer(s, h)
}

// ExecuteCase is the unary single-task RPC.
func (h *Handler) ExecuteCase(ctx context.Context, req *pb.ExecuteCaseRequest) (*pb.ExecuteCaseResponse, error) {
	if req == nil || req.CaseId == 0 {
		return nil, status.Error(codes.InvalidArgument, "case_id is required")
	}
	execID, stat, err := h.app.ExecuteCase(ctx, req.CaseId, vo.NewVersion(req.Version), req.User)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return &pb.ExecuteCaseResponse{
		ExecutionId: execID,
		Status:      toPbDispatchStatus(stat),
	}, nil
}

// ExecuteAllCases is the server-streaming full-version RPC.
func (h *Handler) ExecuteAllCases(
	req *pb.ExecuteAllCasesRequest,
	stream grpc.ServerStreamingServer[pb.BatchDispatchProgress],
) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	err := h.app.ExecuteAllCases(
		stream.Context(),
		vo.NewVersion(req.Version),
		req.User,
		progressForwarder(stream),
	)
	return toGRPCErr(err)
}

// ExecuteChannelCases is the server-streaming (channel, version) RPC.
func (h *Handler) ExecuteChannelCases(
	req *pb.ExecuteChannelCasesRequest,
	stream grpc.ServerStreamingServer[pb.BatchDispatchProgress],
) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	err := h.app.ExecuteChannelCases(
		stream.Context(),
		vo.NewChannel(req.Channel),
		vo.NewVersion(req.Version),
		req.User,
		progressForwarder(stream),
	)
	return toGRPCErr(err)
}

// progressForwarder returns a ProgressFn that marshals usecase.Progress into
// the protobuf frame. Because the aggregator in dispatch_app_service.go is
// the only goroutine invoking this callback, we do not need extra locking
// around stream.Send (which is itself not safe for concurrent use).
func progressForwarder(stream grpc.ServerStreamingServer[pb.BatchDispatchProgress]) usecase.ProgressFn {
	return func(p usecase.Progress) error {
		return stream.Send(&pb.BatchDispatchProgress{
			ChunkIndex:          p.ChunkIndex,
			ChunkSize:           p.ChunkSize,
			SuccessExecutionIds: p.SuccessIDs,
			FailedExecutionIds:  p.FailedIDs,
			TotalDispatched:     p.TotalDispatched,
			TotalPlanned:        p.TotalPlanned,
			ChunkError:          p.ChunkError,
		})
	}
}

func toPbDispatchStatus(s vo.ExecutionStatus) pb.DispatchStatus {
	switch s {
	case vo.StatusWait:
		return pb.DispatchStatus_DISPATCH_STATUS_WAIT
	case vo.StatusFailed:
		return pb.DispatchStatus_DISPATCH_STATUS_FAILED
	default:
		return pb.DispatchStatus_DISPATCH_STATUS_UNSPECIFIED
	}
}

// toGRPCErr maps domain errors onto grpc status codes. Anything else
// surfaces as Internal: we deliberately do not leak infrastructure errors
// verbatim to the caller, but we do propagate the message for diagnostics
// (the caller runs in a trusted Python web tier).
func toGRPCErr(err error) error {
	if err == nil {
		return nil
	}
	// Re-wrap ctx errors to Canceled/DeadlineExceeded so clients can tell
	// apart "user cancelled" from genuine engine failures.
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
