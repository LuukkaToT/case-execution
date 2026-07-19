//go:build integration

package mysql

import (
	"context"
	"testing"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxRepository_租约重试与发布闭环(t *testing.T) {
	ctx := context.Background()
	repo := NewOutboxRepository(testDB)
	executionID := time.Now().UnixNano()
	message := &entity.OutboxMessage{
		ExecutionID: executionID,
		CaseID:      executionID + 1,
		CaseName:    "integration-case",
		Version:     "integration-v1",
		State:       entity.OutboxPending,
		AvailableAt: time.Now().Add(-time.Second),
	}
	t.Cleanup(func() {
		_ = testDB.Where("execution_id = ?", executionID).Delete(&po.OutboxMessage{}).Error
	})

	require.NoError(t, repo.Add(ctx, message))
	assert.NotZero(t, message.OutboxID)

	now := time.Now()
	claimed, err := repo.ClaimPending(ctx, 10, now, now.Add(30*time.Second))
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, executionID, claimed[0].ExecutionID)
	assert.Equal(t, entity.OutboxProcessing, claimed[0].State)
	assert.Equal(t, 1, claimed[0].Attempts)
	firstLeaseToken := claimed[0].LeaseToken
	assert.NotEmpty(t, firstLeaseToken)

	retryAt := now.Add(time.Minute)
	require.NoError(t, repo.MarkRetry(ctx, []int64{executionID}, claimed[0].LeaseToken, retryAt, "broker unavailable"))
	claimed, err = repo.ClaimPending(ctx, 10, now, now.Add(30*time.Second))
	require.NoError(t, err)
	assert.Empty(t, claimed, "退避窗口内不应再次获取消息")

	claimed, err = repo.ClaimPending(ctx, 10, retryAt.Add(time.Millisecond), retryAt.Add(30*time.Second))
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, 2, claimed[0].Attempts)
	assert.NotEqual(t, firstLeaseToken, claimed[0].LeaseToken)

	publishedAt := retryAt.Add(2 * time.Second)
	require.Error(t, repo.MarkPublished(ctx, []int64{executionID}, firstLeaseToken, publishedAt), "过期租约不得覆盖新持有者")
	require.NoError(t, repo.MarkPublished(ctx, []int64{executionID}, claimed[0].LeaseToken, publishedAt))
	var row po.OutboxMessage
	require.NoError(t, testDB.Where("execution_id = ?", executionID).First(&row).Error)
	assert.Equal(t, string(entity.OutboxPublished), row.State)
	assert.NotNil(t, row.PublishedAt)
	assert.Empty(t, row.LastError)
}
