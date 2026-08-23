package usecase

import (
	"context"
	"errors"
	"sync"
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
// 测试替身
// ---------------------------------------------------------------------------

// fakeTxRunner 直接执行 fn，不开启真实数据库事务。
type fakeTxRunner struct{}

func (f *fakeTxRunner) Do(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// fakeUtCaseRepo 保存固定用例集合，用于 FindByID、Count 和 Scan 测试。
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

// fakeExecRepo 在内存中保存执行记录并分配自增 ID。
type fakeExecRepo struct {
	mu             sync.Mutex
	nextID         int64
	records        map[int64]*entity.ExecutionRecord
	batchUpdateErr error
}

func newFakeExecRepo() *fakeExecRepo {
	return &fakeExecRepo{nextID: 1, records: make(map[int64]*entity.ExecutionRecord)}
}

func (f *fakeExecRepo) Add(ctx context.Context, r *entity.ExecutionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(r)
}

func (f *fakeExecRepo) add(r *entity.ExecutionRecord) error {
	if r.RequestID != "" {
		for _, existing := range f.records {
			if existing.RequestID == r.RequestID && existing.CaseID == r.CaseID {
				*r = *existing
				return nil
			}
		}
	}
	r.ExecutionID = f.nextID
	f.nextID++
	cp := *r
	f.records[r.ExecutionID] = &cp
	return nil
}

func (f *fakeExecRepo) BatchAdd(ctx context.Context, records []*entity.ExecutionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range records {
		if err := f.add(r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeExecRepo) FindByID(ctx context.Context, id int64) (*entity.ExecutionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.records[id]
	if !ok {
		return nil, nil
	}
	cp := *r
	return &cp, nil
}

func (f *fakeExecRepo) Save(ctx context.Context, r *entity.ExecutionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[r.ExecutionID]; !ok {
		return errors.New("record not found")
	}
	cp := *r
	f.records[r.ExecutionID] = &cp
	return nil
}

func (f *fakeExecRepo) BatchUpdateStatus(ctx context.Context, records []*entity.ExecutionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.batchUpdateErr != nil {
		return f.batchUpdateErr
	}
	for _, r := range records {
		if stored, ok := f.records[r.ExecutionID]; ok {
			stored.ExecutionStatus = r.ExecutionStatus
			stored.FinishAt = r.FinishAt
		}
	}
	return nil
}

// fakeExecutor 控制 MQ 发布成功、失败或部分成功。
type fakeExecutor struct {
	executeErr     error
	executeFn      func(context.Context) error
	batchRunErr    error
	batchRunResult *vo.BatchResult
	batchRunFn     func(context.Context, []entity.ExecutionTask) (vo.BatchResult, error)
	executeCalls   int32
	batchRunCalls  int32
}

func (f *fakeExecutor) Execute(ctx context.Context, _ *entity.ExecutionRecord, _ string) error {
	atomic.AddInt32(&f.executeCalls, 1)
	if f.executeFn != nil {
		return f.executeFn(ctx)
	}
	return f.executeErr
}

func (f *fakeExecutor) BatchRun(ctx context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error) {
	atomic.AddInt32(&f.batchRunCalls, 1)
	if f.batchRunFn != nil {
		return f.batchRunFn(ctx, tasks)
	}
	if f.batchRunResult != nil {
		return *f.batchRunResult, f.batchRunErr
	}
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
// 测试辅助函数
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
// Config.withDefaults 默认值
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
// 单用例下发
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
	assert.Equal(t, vo.StatusDispatchFailed, status)

	stored, _ := execRepo.FindByID(context.Background(), id)
	require.NotNil(t, stored)
	assert.Equal(t, vo.StatusDispatchFailed, stored.ExecutionStatus)
}

func TestExecuteCase_ClientCancelAfterPublishStillWritesStatus(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "Test1"},
	}}
	execRepo := newFakeExecRepo()
	ctx, cancel := context.WithCancel(context.Background())
	executor := &fakeExecutor{executeFn: func(context.Context) error {
		cancel()
		return nil
	}}
	svc := newService(caseRepo, execRepo, executor)

	id, status, err := svc.ExecuteCase(ctx, 1, vo.Version("v1"), "u")
	require.NoError(t, err)
	assert.Equal(t, vo.StatusWait, status)
	stored, findErr := execRepo.FindByID(context.Background(), id)
	require.NoError(t, findErr)
	assert.Equal(t, vo.StatusWait, stored.ExecutionStatus)
}

func TestExecuteCaseWithRequest_重试复用原记录且不重复发布(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "Test1"},
	}}
	execRepo := newFakeExecRepo()
	executor := &fakeExecutor{}
	svc := newService(caseRepo, execRepo, executor).WithOutbox(newFakeOutboxRepo())

	firstID, firstStatus, err := svc.ExecuteCaseWithRequest(context.Background(), "req-single-1", 1, vo.Version("v1"), "u")
	require.NoError(t, err)
	secondID, secondStatus, err := svc.ExecuteCaseWithRequest(context.Background(), "req-single-1", 1, vo.Version("v1"), "u")
	require.NoError(t, err)

	assert.Equal(t, firstID, secondID)
	assert.Equal(t, vo.StatusWait, firstStatus)
	assert.Equal(t, vo.StatusWait, secondStatus)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executor.executeCalls))
}

// ---------------------------------------------------------------------------
// 全版本批量下发
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

func TestExecuteAllCases_PartialPublishErrorPreservesConfirmedIDs(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "C1"},
		2: {CaseID: 2, CaseName: "C2"},
	}}
	execRepo := newFakeExecRepo()
	executor := &fakeExecutor{batchRunFn: func(_ context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error) {
		return vo.BatchResult{SuccessIDs: []int64{tasks[0].ExecutionID}}, errors.New("connection closed mid-batch")
	}}
	svc := newService(caseRepo, execRepo, executor)

	var progress Progress
	err := svc.ExecuteAllCases(context.Background(), vo.Version("v1"), "u", func(p Progress) error {
		progress = p
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, progress.SuccessIDs, 1)
	assert.Len(t, progress.FailedIDs, 1)
	assert.NotEqual(t, progress.SuccessIDs[0], progress.FailedIDs[0])
	assert.Contains(t, progress.ChunkError, "connection closed")

	succeeded, findErr := execRepo.FindByID(context.Background(), progress.SuccessIDs[0])
	require.NoError(t, findErr)
	failed, findErr := execRepo.FindByID(context.Background(), progress.FailedIDs[0])
	require.NoError(t, findErr)
	assert.Equal(t, vo.StatusWait, succeeded.ExecutionStatus)
	assert.Equal(t, vo.StatusDispatchFailed, failed.ExecutionStatus)
}

func TestExecuteAllCases_WriteBackErrorIsVisibleInProgress(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "C1"},
	}}
	execRepo := newFakeExecRepo()
	execRepo.batchUpdateErr = errors.New("mysql unavailable")
	svc := newService(caseRepo, execRepo, &fakeExecutor{batchRunErr: errors.New("broker down")})

	var progress Progress
	err := svc.ExecuteAllCases(context.Background(), vo.Version("v1"), "u", func(p Progress) error {
		progress = p
		return nil
	})
	require.NoError(t, err)
	assert.Contains(t, progress.ChunkError, "status write-back failed")
}

func TestExecuteAllCasesWithRequest_重试不重复批量发布(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "C1"},
		2: {CaseID: 2, CaseName: "C2"},
	}}
	execRepo := newFakeExecRepo()
	executor := &fakeExecutor{}
	svc := newService(caseRepo, execRepo, executor).WithOutbox(newFakeOutboxRepo())

	for i := 0; i < 2; i++ {
		err := svc.ExecuteAllCasesWithRequest(context.Background(), "req-batch-1", vo.Version("v1"), "u", func(Progress) error {
			return nil
		})
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&executor.batchRunCalls))
}

// ---------------------------------------------------------------------------
// 按渠道和版本批量下发
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
// dispatchStream：进度回调失败时取消整条流水线
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
// extractExecutionIDs 辅助函数
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
// 并发场景：多个 worker 为全部分片返回进度
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
			// 分两批返回用例，覆盖多分片并行路径。
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
