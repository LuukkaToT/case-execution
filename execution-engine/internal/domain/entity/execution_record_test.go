package entity

import (
	"errors"
	"testing"

	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/vo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExecutionRecord(t *testing.T) {
	r := NewExecutionRecord(10, vo.Version("v1.0"), "alice")
	assert.Equal(t, int64(10), r.CaseID)
	assert.Equal(t, vo.Version("v1.0"), r.Version)
	assert.Equal(t, "alice", r.CreateBy)
	assert.Equal(t, vo.StatusInit, r.ExecutionStatus)
	assert.Zero(t, r.ExecutionID)
	assert.Nil(t, r.ExecuteAt)
	assert.Nil(t, r.FinishAt)
}

func TestMarkAsWait(t *testing.T) {
	r := NewExecutionRecord(1, vo.Version("v1"), "u")
	require.NoError(t, r.MarkAsWait())
	assert.Equal(t, vo.StatusWait, r.ExecutionStatus)
	assert.Nil(t, r.FinishAt)
}

func TestMarkAsFailed(t *testing.T) {
	r := NewExecutionRecord(1, vo.Version("v1"), "u")
	require.NoError(t, r.MarkAsFailed())
	assert.Equal(t, vo.StatusFailed, r.ExecutionStatus)
	assert.NotNil(t, r.FinishAt)
}

func TestMarkAsRunning(t *testing.T) {
	r := NewExecutionRecord(1, vo.Version("v1"), "u")
	require.NoError(t, r.MarkAsWait())
	require.NoError(t, r.MarkAsRunning())
	assert.Equal(t, vo.StatusRunning, r.ExecutionStatus)
	assert.NotNil(t, r.ExecuteAt)
}

func TestMarkAsWait_IllegalTransition(t *testing.T) {
	r := NewExecutionRecord(1, vo.Version("v1"), "u")
	require.NoError(t, r.MarkAsWait())
	// WAIT 不能再次迁移到 WAIT。
	err := r.MarkAsWait()
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrIllegalStatusTransition))
}

func TestMarkAsFailed_FromTerminal(t *testing.T) {
	r := NewExecutionRecord(1, vo.Version("v1"), "u")
	require.NoError(t, r.MarkAsFailed())
	// FAILED 为终态，不能再次迁移到 FAILED。
	err := r.MarkAsFailed()
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrIllegalStatusTransition))
}

func TestMarkAsRunning_IllegalFromInit(t *testing.T) {
	r := NewExecutionRecord(1, vo.Version("v1"), "u")
	// INIT 不能跳过 WAIT 直接迁移到 RUNNING。
	err := r.MarkAsRunning()
	require.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrIllegalStatusTransition))
}
