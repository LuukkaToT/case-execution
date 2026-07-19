//go:build integration

package mysql

import (
	"context"
	"errors"
	"testing"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newExecRecord(caseID int64, version, user string) *entity.ExecutionRecord {
	return entity.NewExecutionRecord(caseID, vo.Version(version), user)
}

// ---------------------------------------------------------------------------
// 单条新增
// ---------------------------------------------------------------------------

func TestExecutionRepository_Add(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	r := newExecRecord(100, "v1.0", "tester")
	require.NoError(t, repo.Add(ctx, r))
	assert.Greater(t, r.ExecutionID, int64(0))
}

func TestExecutionRepository_Add_相同请求幂等(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	first := entity.NewExecutionRecordForRequest("it-single-idempotent", 201, vo.Version("v1"), "alice")
	second := entity.NewExecutionRecordForRequest("it-single-idempotent", 201, vo.Version("v1"), "alice")
	require.NoError(t, repo.Add(ctx, first))
	require.NoError(t, repo.Add(ctx, second))
	assert.Equal(t, first.ExecutionID, second.ExecutionID)

	var count int64
	require.NoError(t, FromCtx(ctx, testDB).Model(&po.ExecutionRecord{}).
		Where("request_id = ? AND case_id = ?", "it-single-idempotent", 201).
		Count(&count).Error)
	assert.Equal(t, int64(1), count)
}

// ---------------------------------------------------------------------------
// 批量新增
// ---------------------------------------------------------------------------

func TestExecutionRepository_BatchAdd(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	records := []*entity.ExecutionRecord{
		newExecRecord(101, "v1.0", "tester"),
		newExecRecord(102, "v1.0", "tester"),
		newExecRecord(103, "v1.0", "tester"),
	}
	require.NoError(t, repo.BatchAdd(ctx, records))

	ids := make(map[int64]struct{})
	for _, r := range records {
		assert.Greater(t, r.ExecutionID, int64(0))
		ids[r.ExecutionID] = struct{}{}
	}
	assert.Len(t, ids, 3, "all ExecutionIDs should be unique")
}

func TestExecutionRepository_BatchAdd_相同请求幂等(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	first := []*entity.ExecutionRecord{
		entity.NewExecutionRecordForRequest("it-batch-idempotent", 211, vo.Version("v1"), "alice"),
		entity.NewExecutionRecordForRequest("it-batch-idempotent", 212, vo.Version("v1"), "alice"),
	}
	second := []*entity.ExecutionRecord{
		entity.NewExecutionRecordForRequest("it-batch-idempotent", 211, vo.Version("v1"), "alice"),
		entity.NewExecutionRecordForRequest("it-batch-idempotent", 212, vo.Version("v1"), "alice"),
	}
	require.NoError(t, repo.BatchAdd(ctx, first))
	require.NoError(t, repo.BatchAdd(ctx, second))
	assert.Equal(t, first[0].ExecutionID, second[0].ExecutionID)
	assert.Equal(t, first[1].ExecutionID, second[1].ExecutionID)
}

func TestExecutionRepository_BatchAdd_Empty(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)
	require.NoError(t, repo.BatchAdd(ctx, nil))
}

// ---------------------------------------------------------------------------
// 按编号查询
// ---------------------------------------------------------------------------

func TestExecutionRepository_FindByID_Found(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	r := newExecRecord(200, "v2.0", "alice")
	require.NoError(t, repo.Add(ctx, r))

	got, err := repo.FindByID(ctx, r.ExecutionID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, r.ExecutionID, got.ExecutionID)
	assert.Equal(t, int64(200), got.CaseID)
	assert.Equal(t, vo.Version("v2.0"), got.Version)
	assert.Equal(t, "alice", got.CreateBy)
	assert.Equal(t, vo.StatusInit, got.ExecutionStatus)
}

func TestExecutionRepository_FindByID_NotFound(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	got, err := repo.FindByID(ctx, 999999999)
	require.NoError(t, err)
	assert.Nil(t, got)
}

// ---------------------------------------------------------------------------
// 保存状态
// ---------------------------------------------------------------------------

func TestExecutionRepository_Save(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	r := newExecRecord(300, "v3.0", "bob")
	require.NoError(t, repo.Add(ctx, r))

	require.NoError(t, r.MarkAsWait())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.FindByID(ctx, r.ExecutionID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, vo.StatusWait, got.ExecutionStatus)
}

func TestExecutionRepository_Save_NoMatchingRow(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	r := &entity.ExecutionRecord{ExecutionID: 999999998, ExecutionStatus: vo.StatusWait}
	err := repo.Save(ctx, r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no row matched")
}

func TestExecutionRepository_Save_DoesNotRegressAdvancedState(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	r := newExecRecord(301, "v3.0", "bob")
	require.NoError(t, repo.Add(ctx, r))
	require.NoError(t, FromCtx(ctx, testDB).Model(&po.ExecutionRecord{}).
		Where("execution_id = ?", r.ExecutionID).
		Update("execution_status", string(vo.StatusSuccess)).Error)

	require.NoError(t, r.MarkAsWait())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.FindByID(ctx, r.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, vo.StatusSuccess, got.ExecutionStatus)
}

// ---------------------------------------------------------------------------
// 批量更新状态
// ---------------------------------------------------------------------------

func TestExecutionRepository_BatchUpdateStatus(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	records := []*entity.ExecutionRecord{
		newExecRecord(401, "v4", "tester"),
		newExecRecord(402, "v4", "tester"),
		newExecRecord(403, "v4", "tester"),
	}
	require.NoError(t, repo.BatchAdd(ctx, records))

	require.NoError(t, records[0].MarkAsWait())
	require.NoError(t, records[1].MarkAsFailed())
	// records[2] 保持 INIT 状态，更新时应跳过。

	require.NoError(t, repo.BatchUpdateStatus(ctx, records))

	r0, _ := repo.FindByID(ctx, records[0].ExecutionID)
	r1, _ := repo.FindByID(ctx, records[1].ExecutionID)
	r2, _ := repo.FindByID(ctx, records[2].ExecutionID)
	assert.Equal(t, vo.StatusWait, r0.ExecutionStatus)
	assert.Equal(t, vo.StatusFailed, r1.ExecutionStatus)
	assert.NotNil(t, r1.FinishAt)
	assert.Equal(t, vo.StatusInit, r2.ExecutionStatus, "StatusInit records must not be updated")
}

func TestExecutionRepository_BatchUpdateStatus_记录缺失时返回错误(t *testing.T) {
	ctx := withTx(t)
	repo := NewExecutionRepository(testDB)

	record := &entity.ExecutionRecord{
		ExecutionID:     999999997,
		ExecutionStatus: vo.StatusWait,
	}
	err := repo.BatchUpdateStatus(ctx, []*entity.ExecutionRecord{record})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rows are missing")
}

// ---------------------------------------------------------------------------
// 事务执行器
// ---------------------------------------------------------------------------

func TestTxRunner_Commit(t *testing.T) {
	runner := NewTxRunner(testDB)
	repo := NewExecutionRepository(testDB)
	var insertedID int64

	err := runner.Do(context.Background(), func(ctx context.Context) error {
		r := newExecRecord(500, "v5", "txcommit")
		if err := repo.Add(ctx, r); err != nil {
			return err
		}
		insertedID = r.ExecutionID
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, insertedID, int64(0))

	// 测试完成后清理已提交的数据行。
	t.Cleanup(func() { testDB.Delete(&po.ExecutionRecord{}, insertedID) })

	// 提交后，事务外必须能够查到该记录。
	got, err := repo.FindByID(context.Background(), insertedID)
	require.NoError(t, err)
	assert.NotNil(t, got)
}

func TestTxRunner_Rollback(t *testing.T) {
	runner := NewTxRunner(testDB)
	repo := NewExecutionRepository(testDB)

	forcedErr := errors.New("force rollback")
	var tentativeID int64

	err := runner.Do(context.Background(), func(ctx context.Context) error {
		r := newExecRecord(501, "v5", "txrollback")
		if err := repo.Add(ctx, r); err != nil {
			return err
		}
		tentativeID = r.ExecutionID
		return forcedErr
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, forcedErr))

	// 事务已回滚，因此事务外不能查到该记录。
	if tentativeID > 0 {
		got, err := repo.FindByID(context.Background(), tentativeID)
		require.NoError(t, err)
		assert.Nil(t, got)
	}
}
