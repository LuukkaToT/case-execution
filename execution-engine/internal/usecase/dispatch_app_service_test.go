package usecase

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/domain/vo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeTxRunner executes fn directly with no real DB transaction.
type fakeTxRunner struct{}

func (f *fakeTxRunner) Do(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// fakeUtCaseRepo holds a fixed set of UtCases for FindByID, Count and Scan.
type fakeUtCaseRepo struct {
	cases  map[int64]*entity.UtCase
	scanFn func(fn repository.UtCaseScanFn) error
}

func (f *fakeUtCaseRepo) FindByID(_ context.Context, id int64) (*entity.UtCase, error) {
	c, ok := f.cases[id]
	if !ok {
		return nil, nil
	}
	return c, nil
}

func (f *fakeUtCaseRepo) CountByVersion(_ context.Context, _ vo.Version) (int64, error) {
	return int64(len(f.cases)), nil
}

func (f *fakeUtCaseRepo) CountByChannelVersion(_ context.Context, _ vo.Channel, _ vo.Version) (int64, error) {
	return int64(len(f.cases)), nil
}

func (f *fakeUtCaseRepo) ScanByVersion(_ context.Context, _ vo.Version, _ int, fn repository.UtCaseScanFn) error {
	if f.scanFn != nil {
		return f.scanFn(fn)
	}
	batch := make([]*entity.UtCase, 0, len(f.cases))
	for _, c := range f.cases {
		batch = append(batch, c)
	}
	if len(batch) > 0 {
		return fn(batch)
	}
	return nil
}

func (f *fakeUtCaseRepo) ScanByChannelVersion(_ context.Context, _ vo.Channel, _ vo.Version, _ int, fn repository.UtCaseScanFn) error {
	return f.ScanByVersion(context.Background(), "", 0, fn)
}

// fakeExecRepo stores records in memory and assigns auto-increment IDs.
type fakeExecRepo struct {
	nextID  int64
	records map[int64]*entity.ExecutionRecord
}

func newFakeExecRepo() *fakeExecRepo {
	return &fakeExecRepo{nextID: 1, records: make(map[int64]*entity.ExecutionRecord)}
}

func (f *fakeExecRepo) Add(_ context.Context, r *entity.ExecutionRecord) error {
	r.ExecutionID = f.nextID
	f.nextID++
	cp := *r
	f.records[r.ExecutionID] = &cp
	return nil
}

func (f *fakeExecRepo) BatchAdd(_ context.Context, records []*entity.ExecutionRecord) error {
	for _, r := range records {
		if err := f.Add(context.Background(), r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeExecRepo) FindByID(_ context.Context, id int64) (*entity.ExecutionRecord, error) {
	r, ok := f.records[id]
	if !ok {
		return nil, nil
	}
	cp := *r
	return &cp, nil
}

func (f *fakeExecRepo) Save(_ context.Context, r *entity.ExecutionRecord) error {
	if _, ok := f.records[r.ExecutionID]; !ok {
		return errors.New("record not found")
	}
	cp := *r
	f.records[r.ExecutionID] = &cp
	return nil
}

func (f *fakeExecRepo) BatchUpdateStatus(_ context.Context, records []*entity.ExecutionRecord) error {
	for _, r := range records {
		if stored, ok := f.records[r.ExecutionID]; ok {
			stored.ExecutionStatus = r.ExecutionStatus
		}
	}
	return nil
}

// fakeExecutor controls whether MQ publish succeeds or fails.
type fakeExecutor struct {
	executeErr  error
	batchRunErr error
}

func (f *fakeExecutor) Execute(_ context.Context, _ *entity.ExecutionRecord, _ string) error {
	return f.executeErr
}

func (f *fakeExecutor) BatchRun(_ context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error) {
	if f.batchRunErr != nil {
		return vo.BatchResult{}, f.batchRunErr
	}
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.ExecutionID)
	}
	return vo.BatchResult{SuccessIDs: ids}, nil
}

func (f *fakeExecutor) Close(_ context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newService(caseRepo repository.UtCaseRepository, execRepo repository.ExecutionRepository, executor port.UtCaseExecutorClient) *DispatchAppService {
	return NewDispatchAppService(
		Config{BatchSize: 10, WorkerCount: 2},
		&fakeTxRunner{},
		caseRepo,
		execRepo,
		executor,
		nil,
	)
}

// ---------------------------------------------------------------------------
// Config.withDefaults
// ---------------------------------------------------------------------------

func TestConfigWithDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	assert.Equal(t, 1000, c.BatchSize)
	assert.Equal(t, 4, c.WorkerCount)
}

func TestConfigWithDefaults_NonZeroUnchanged(t *testing.T) {
	c := Config{BatchSize: 50, WorkerCount: 8}.withDefaults()
	assert.Equal(t, 50, c.BatchSize)
	assert.Equal(t, 8, c.WorkerCount)
}

// ---------------------------------------------------------------------------
// ExecuteCase
// ---------------------------------------------------------------------------

func TestExecuteCase_EmptyVersion(t *testing.T) {
	svc := newService(&fakeUtCaseRepo{}, newFakeExecRepo(), &fakeExecutor{})
	_, _, err := svc.ExecuteCase(context.Background(), 1, vo.Version(""), "u")
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrInvalidArgument))
}

func TestExecuteCase_CaseNotFound(t *testing.T) {
	svc := newService(&fakeUtCaseRepo{cases: map[int64]*entity.UtCase{}}, newFakeExecRepo(), &fakeExecutor{})
	_, _, err := svc.ExecuteCase(context.Background(), 99, vo.Version("v1"), "u")
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrCaseNotExist))
}

func TestExecuteCase_MQSuccess(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "Test1"},
	}}
	execRepo := newFakeExecRepo()
	svc := newService(caseRepo, execRepo, &fakeExecutor{})

	id, status, err := svc.ExecuteCase(context.Background(), 1, vo.Version("v1"), "u")
	require.NoError(t, err)
	assert.Greater(t, id, int64(0))
	assert.Equal(t, vo.StatusWait, status)

	stored, _ := execRepo.FindByID(context.Background(), id)
	require.NotNil(t, stored)
	assert.Equal(t, vo.StatusWait, stored.ExecutionStatus)
}

func TestExecuteCase_MQFailure(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "Test1"},
	}}
	execRepo := newFakeExecRepo()
	executor := &fakeExecutor{executeErr: errors.New("broker down")}
	svc := newService(caseRepo, execRepo, executor)

	id, status, err := svc.ExecuteCase(context.Background(), 1, vo.Version("v1"), "u")
	require.NoError(t, err)
	assert.Greater(t, id, int64(0))
	assert.Equal(t, vo.StatusFailed, status)

	stored, _ := execRepo.FindByID(context.Background(), id)
	require.NotNil(t, stored)
	assert.Equal(t, vo.StatusFailed, stored.ExecutionStatus)
}

// ---------------------------------------------------------------------------
// ExecuteAllCases
// ---------------------------------------------------------------------------

func TestExecuteAllCases_EmptyVersion(t *testing.T) {
	svc := newService(&fakeUtCaseRepo{}, newFakeExecRepo(), &fakeExecutor{})
	err := svc.ExecuteAllCases(context.Background(), vo.Version(""), "u", func(Progress) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrInvalidArgument))
}

func TestExecuteAllCases_ZeroCount(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{}}
	svc := newService(caseRepo, newFakeExecRepo(), &fakeExecutor{})
	var called int
	err := svc.ExecuteAllCases(context.Background(), vo.Version("v1"), "u", func(Progress) error {
		called++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, called)
}

func TestExecuteAllCases_SingleBatchSuccess(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "C1"},
		2: {CaseID: 2, CaseName: "C2"},
	}}
	execRepo := newFakeExecRepo()
	svc := newService(caseRepo, execRepo, &fakeExecutor{})

	var progresses []Progress
	err := svc.ExecuteAllCases(context.Background(), vo.Version("v1"), "u", func(p Progress) error {
		progresses = append(progresses, p)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, progresses)

	last := progresses[len(progresses)-1]
	assert.Equal(t, int32(2), last.TotalPlanned)
	assert.Equal(t, int32(2), last.TotalDispatched)
	assert.Empty(t, last.FailedIDs)
}

// ---------------------------------------------------------------------------
// ExecuteChannelCases
// ---------------------------------------------------------------------------

func TestExecuteChannelCases_EmptyChannel(t *testing.T) {
	svc := newService(&fakeUtCaseRepo{}, newFakeExecRepo(), &fakeExecutor{})
	err := svc.ExecuteChannelCases(context.Background(), vo.Channel(""), vo.Version("v1"), "u", func(Progress) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrInvalidArgument))
}

func TestExecuteChannelCases_EmptyVersion(t *testing.T) {
	svc := newService(&fakeUtCaseRepo{}, newFakeExecRepo(), &fakeExecutor{})
	err := svc.ExecuteChannelCases(context.Background(), vo.Channel("ios"), vo.Version(""), "u", func(Progress) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrInvalidArgument))
}

// ---------------------------------------------------------------------------
// dispatchStream: progress callback error cancels pipeline
// ---------------------------------------------------------------------------

func TestExecuteAllCases_ProgressCallbackError(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "C1"},
	}}
	svc := newService(caseRepo, newFakeExecRepo(), &fakeExecutor{})
	cbErr := errors.New("stream closed")
	err := svc.ExecuteAllCases(context.Background(), vo.Version("v1"), "u", func(Progress) error {
		return cbErr
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "progress callback")
}

// ---------------------------------------------------------------------------
// extractExecutionIDs helper
// ---------------------------------------------------------------------------

func TestExtractExecutionIDs(t *testing.T) {
	records := []*entity.ExecutionRecord{
		{ExecutionID: 10},
		{ExecutionID: 20},
	}
	ids := extractExecutionIDs(records)
	assert.Equal(t, []int64{10, 20}, ids)
}

// ---------------------------------------------------------------------------
// Concurrency: multiple workers emit progress for all chunks
// ---------------------------------------------------------------------------

func TestExecuteAllCases_MultipleChunks(t *testing.T) {
	t.Parallel()

	cases := make(map[int64]*entity.UtCase, 5)
	for i := int64(1); i <= 5; i++ {
		cases[i] = &entity.UtCase{CaseID: i, CaseName: "C"}
	}
	caseRepo := &fakeUtCaseRepo{
		cases: cases,
		scanFn: func(fn repository.UtCaseScanFn) error {
			// Deliver cases in two separate chunks to exercise multi-batch path.
			batch1 := []*entity.UtCase{cases[1], cases[2], cases[3]}
			if err := fn(batch1); err != nil {
				return err
			}
			batch2 := []*entity.UtCase{cases[4], cases[5]}
			return fn(batch2)
		},
	}

	svc := NewDispatchAppService(
		Config{BatchSize: 3, WorkerCount: 2},
		&fakeTxRunner{},
		caseRepo,
		newFakeExecRepo(),
		&fakeExecutor{},
		nil,
	)

	var totalDispatched int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := svc.ExecuteAllCases(ctx, vo.Version("v1"), "u", func(p Progress) error {
		atomic.StoreInt32(&totalDispatched, p.TotalDispatched)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(5), atomic.LoadInt32(&totalDispatched))
}
