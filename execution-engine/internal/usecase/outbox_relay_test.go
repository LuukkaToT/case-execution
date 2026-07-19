package usecase

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOutboxRepo 用内存状态验证 relay 的租约和状态迁移，不依赖 MySQL。
type fakeOutboxRepo struct {
	mu       sync.Mutex
	claimed  []*entity.OutboxMessage
	messages map[int64]*entity.OutboxMessage
}

func newFakeOutboxRepo(messages ...*entity.OutboxMessage) *fakeOutboxRepo {
	repo := &fakeOutboxRepo{messages: make(map[int64]*entity.OutboxMessage)}
	for _, message := range messages {
		copyMessage := *message
		repo.messages[message.ExecutionID] = &copyMessage
		repo.claimed = append(repo.claimed, &copyMessage)
	}
	return repo
}

func (f *fakeOutboxRepo) Add(_ context.Context, message *entity.OutboxMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copyMessage := *message
	f.messages[message.ExecutionID] = &copyMessage
	return nil
}

func (f *fakeOutboxRepo) BatchAdd(ctx context.Context, messages []*entity.OutboxMessage) error {
	for _, message := range messages {
		if err := f.Add(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeOutboxRepo) ClaimPending(_ context.Context, _ int, _, _ time.Time) ([]*entity.OutboxMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	claimed := append([]*entity.OutboxMessage(nil), f.claimed...)
	f.claimed = nil
	return claimed, nil
}

func (f *fakeOutboxRepo) MarkPublished(_ context.Context, ids []int64, _ string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if message := f.messages[id]; message != nil {
			message.State = entity.OutboxPublished
			message.PublishedAt = &at
			message.LastError = ""
		}
	}
	return nil
}

func (f *fakeOutboxRepo) MarkRetry(_ context.Context, ids []int64, _ string, availableAt time.Time, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if message := f.messages[id]; message != nil {
			message.State = entity.OutboxPending
			message.AvailableAt = availableAt
			message.LastError = lastError
		}
	}
	return nil
}

func (f *fakeOutboxRepo) MarkDead(_ context.Context, ids []int64, _ string, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if message := f.messages[id]; message != nil {
			message.State = entity.OutboxDead
			message.LastError = lastError
		}
	}
	return nil
}

func seedFakeExecution(repo *fakeExecRepo, id int64) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	repo.records[id] = &entity.ExecutionRecord{ExecutionID: id, ExecutionStatus: "INIT"}
}

func relayMessage(id int64, attempts int) *entity.OutboxMessage {
	return &entity.OutboxMessage{
		ExecutionID: id,
		CaseID:      id + 1000,
		CaseName:    "case",
		Version:     "v1",
		State:       entity.OutboxProcessing,
		Attempts:    attempts,
		LeaseToken:  "test-lease-token",
	}
}

func TestOutboxRelay_确认成功后更新记录和Outbox(t *testing.T) {
	message := relayMessage(101, 1)
	outbox := newFakeOutboxRepo(message)
	execRepo := newFakeExecRepo()
	seedFakeExecution(execRepo, message.ExecutionID)
	relay := NewOutboxRelay(OutboxRelayConfig{}, &fakeTxRunner{}, outbox, execRepo, &fakeExecutor{}, nil)

	require.NoError(t, relay.DispatchOnce(context.Background()))
	stored, err := execRepo.FindByID(context.Background(), message.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "WAIT", string(stored.ExecutionStatus))
	assert.Equal(t, entity.OutboxPublished, outbox.messages[message.ExecutionID].State)
}

func TestOutboxRelay_未达到上限时退避重试(t *testing.T) {
	message := relayMessage(102, 2)
	outbox := newFakeOutboxRepo(message)
	execRepo := newFakeExecRepo()
	seedFakeExecution(execRepo, message.ExecutionID)
	executor := &fakeExecutor{batchRunResult: batchFailure(message.ExecutionID)}
	relay := NewOutboxRelay(OutboxRelayConfig{MaxAttempts: 3}, &fakeTxRunner{}, outbox, execRepo, executor, nil)
	fixedNow := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return fixedNow }

	require.NoError(t, relay.DispatchOnce(context.Background()))
	stored, err := execRepo.FindByID(context.Background(), message.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "INIT", string(stored.ExecutionStatus))
	assert.Equal(t, entity.OutboxPending, outbox.messages[message.ExecutionID].State)
	assert.True(t, outbox.messages[message.ExecutionID].AvailableAt.After(fixedNow))
}

func TestOutboxRelay_达到上限后进入死信状态(t *testing.T) {
	message := relayMessage(103, 3)
	outbox := newFakeOutboxRepo(message)
	execRepo := newFakeExecRepo()
	seedFakeExecution(execRepo, message.ExecutionID)
	executor := &fakeExecutor{batchRunResult: batchFailure(message.ExecutionID)}
	relay := NewOutboxRelay(OutboxRelayConfig{MaxAttempts: 3}, &fakeTxRunner{}, outbox, execRepo, executor, nil)

	require.NoError(t, relay.DispatchOnce(context.Background()))
	stored, err := execRepo.FindByID(context.Background(), message.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", string(stored.ExecutionStatus))
	assert.NotNil(t, stored.FinishAt)
	assert.Equal(t, entity.OutboxDead, outbox.messages[message.ExecutionID].State)
}

func TestDispatchAppService_执行记录与Outbox同步完成(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "case"},
	}}
	execRepo := newFakeExecRepo()
	outbox := newFakeOutboxRepo()
	svc := newService(caseRepo, execRepo, &fakeExecutor{}).WithOutbox(outbox)

	executionID, status, err := svc.ExecuteCaseWithRequest(context.Background(), "req-outbox-success", 1, "v1", "user")
	require.NoError(t, err)
	assert.Equal(t, vo.StatusWait, status)
	require.NotNil(t, outbox.messages[executionID])
	assert.Equal(t, entity.OutboxPublished, outbox.messages[executionID].State)
}

func TestDispatchAppService_发布失败时Outbox进入死信(t *testing.T) {
	caseRepo := &fakeUtCaseRepo{cases: map[int64]*entity.UtCase{
		1: {CaseID: 1, CaseName: "case"},
	}}
	execRepo := newFakeExecRepo()
	outbox := newFakeOutboxRepo()
	svc := newService(caseRepo, execRepo, &fakeExecutor{executeErr: errors.New("broker unavailable")}).WithOutbox(outbox)

	executionID, status, err := svc.ExecuteCaseWithRequest(context.Background(), "req-outbox-failed", 1, "v1", "user")
	require.NoError(t, err)
	assert.Equal(t, vo.StatusFailed, status)
	require.NotNil(t, outbox.messages[executionID])
	assert.Equal(t, entity.OutboxDead, outbox.messages[executionID].State)
}

func batchFailure(id int64) *vo.BatchResult {
	return &vo.BatchResult{FailedIDs: []int64{id}, ErrorMessage: "broker unavailable"}
}
