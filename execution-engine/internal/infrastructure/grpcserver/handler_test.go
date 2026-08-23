package grpcserver

import (
	"context"
	"errors"
	"testing"

	pb "execution-engine/api/gen/taskexecution/v1"
	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/domain/vo"
	"execution-engine/internal/usecase"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// toGRPCErr：纯函数表驱动测试
// ---------------------------------------------------------------------------

func TestToGRPCErr(t *testing.T) {
	cases := []struct {
		name     string
		in       error
		wantCode codes.Code
		wantNil  bool
	}{
		{"nil", nil, 0, true},
		{"Canceled", context.Canceled, codes.Canceled, false},
		{"DeadlineExceeded", context.DeadlineExceeded, codes.DeadlineExceeded, false},
		{"CaseNotExist", errs.NewCaseNotExist(1), codes.NotFound, false},
		{"ExecutionNotFound", errs.NewExecutionNotFound(1), codes.NotFound, false},
		{"IllegalStatusTransition", errs.NewIllegalStatusTransition(1, "WAIT", "RUNNING"), codes.FailedPrecondition, false},
		{"InvalidArgument", errs.NewInvalidArgument("bad"), codes.InvalidArgument, false},
		{"Unknown", errors.New("some internal error"), codes.Internal, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toGRPCErr(c.in)
			if c.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			st, ok := status.FromError(got)
			require.True(t, ok)
			assert.Equal(t, c.wantCode, st.Code())
		})
	}
}

// ---------------------------------------------------------------------------
// toPbDispatchStatus：纯函数表驱动测试
// ---------------------------------------------------------------------------

func TestToPbDispatchStatus(t *testing.T) {
	cases := []struct {
		in   vo.ExecutionStatus
		want pb.DispatchStatus
	}{
		{vo.StatusWait, pb.DispatchStatus_DISPATCH_STATUS_WAIT},
		{vo.StatusDispatchFailed, pb.DispatchStatus_DISPATCH_STATUS_FAILED},
		{vo.StatusFailed, pb.DispatchStatus_DISPATCH_STATUS_UNSPECIFIED},
		{vo.StatusRunning, pb.DispatchStatus_DISPATCH_STATUS_UNSPECIFIED},
		{vo.StatusSuccess, pb.DispatchStatus_DISPATCH_STATUS_UNSPECIFIED},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, toPbDispatchStatus(c.in), "status=%s", c.in)
	}
}

// ---------------------------------------------------------------------------
// Handler.ExecuteCase：请求校验
// ---------------------------------------------------------------------------

func TestHandlerExecuteCase_NilRequest(t *testing.T) {
	h := NewHandler(newTestApp(), nil)
	_, err := h.ExecuteCase(context.Background(), nil)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestHandlerExecuteCase_ZeroCaseID(t *testing.T) {
	h := NewHandler(newTestApp(), nil)
	_, err := h.ExecuteCase(context.Background(), &pb.ExecuteCaseRequest{CaseId: 0, Version: "v1"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestHandlerExecuteCase_ValidRequest(t *testing.T) {
	h := NewHandler(newTestApp(), nil)
	resp, err := h.ExecuteCase(context.Background(), &pb.ExecuteCaseRequest{
		CaseId:  1,
		Version: "v1",
		User:    "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Greater(t, resp.ExecutionId, int64(0))
	assert.Equal(t, pb.DispatchStatus_DISPATCH_STATUS_WAIT, resp.Status)
	assert.NotEmpty(t, resp.RequestId)
}

// ---------------------------------------------------------------------------
// 使用最小测试替身装配真实 DispatchAppService，供 handler 测试使用
// ---------------------------------------------------------------------------

type handlerFakeTxRunner struct{}

func (f *handlerFakeTxRunner) Do(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type handlerFakeCaseRepo struct{}

func (f *handlerFakeCaseRepo) FindByID(_ context.Context, id int64) (*entity.UtCase, error) {
	if id == 1 {
		return &entity.UtCase{CaseID: 1, CaseName: "TestCase"}, nil
	}
	return nil, nil
}

func (f *handlerFakeCaseRepo) CountByVersion(_ context.Context, _ vo.Version) (int64, error) {
	return 1, nil
}

func (f *handlerFakeCaseRepo) CountByChannelVersion(_ context.Context, _ vo.Channel, _ vo.Version) (int64, error) {
	return 1, nil
}

func (f *handlerFakeCaseRepo) ScanByVersion(_ context.Context, _ vo.Version, _ int, fn repository.UtCaseScanFn) error {
	return fn([]*entity.UtCase{{CaseID: 1, CaseName: "TestCase"}})
}

func (f *handlerFakeCaseRepo) ScanByChannelVersion(_ context.Context, _ vo.Channel, _ vo.Version, _ int, fn repository.UtCaseScanFn) error {
	return fn([]*entity.UtCase{{CaseID: 1, CaseName: "TestCase"}})
}

type handlerFakeExecRepo struct {
	nextID  int64
	records map[int64]*entity.ExecutionRecord
}

func newHandlerFakeExecRepo() *handlerFakeExecRepo {
	return &handlerFakeExecRepo{nextID: 1, records: make(map[int64]*entity.ExecutionRecord)}
}

func (f *handlerFakeExecRepo) Add(_ context.Context, r *entity.ExecutionRecord) error {
	r.ExecutionID = f.nextID
	f.nextID++
	cp := *r
	f.records[r.ExecutionID] = &cp
	return nil
}

func (f *handlerFakeExecRepo) BatchAdd(_ context.Context, records []*entity.ExecutionRecord) error {
	for _, r := range records {
		if err := f.Add(context.Background(), r); err != nil {
			return err
		}
	}
	return nil
}

func (f *handlerFakeExecRepo) FindByID(_ context.Context, id int64) (*entity.ExecutionRecord, error) {
	r, ok := f.records[id]
	if !ok {
		return nil, nil
	}
	cp := *r
	return &cp, nil
}

func (f *handlerFakeExecRepo) Save(_ context.Context, r *entity.ExecutionRecord) error {
	if _, ok := f.records[r.ExecutionID]; !ok {
		return errors.New("record not found")
	}
	cp := *r
	f.records[r.ExecutionID] = &cp
	return nil
}

func (f *handlerFakeExecRepo) BatchUpdateStatus(_ context.Context, records []*entity.ExecutionRecord) error {
	for _, r := range records {
		if stored, ok := f.records[r.ExecutionID]; ok {
			stored.ExecutionStatus = r.ExecutionStatus
		}
	}
	return nil
}

type handlerFakeExecutor struct{}

func (f *handlerFakeExecutor) Execute(_ context.Context, _ *entity.ExecutionRecord, _ string) error {
	return nil
}

func (f *handlerFakeExecutor) BatchRun(_ context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error) {
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.ExecutionID)
	}
	return vo.BatchResult{SuccessIDs: ids}, nil
}

func (f *handlerFakeExecutor) Close(_ context.Context) error { return nil }

var _ port.UtCaseExecutorClient = (*handlerFakeExecutor)(nil)

func newTestApp() *usecase.DispatchAppService {
	return usecase.NewDispatchAppService(
		usecase.Config{BatchSize: 10, WorkerCount: 2},
		&handlerFakeTxRunner{},
		&handlerFakeCaseRepo{},
		newHandlerFakeExecRepo(),
		&handlerFakeExecutor{},
		nil,
	)
}
