package service

import (
	"testing"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateExecutionRecord(t *testing.T) {
	svc := New()
	r := svc.CreateExecutionRecord(5, vo.Version("v2.0"), "bob")
	assert.Equal(t, int64(5), r.CaseID)
	assert.Equal(t, vo.Version("v2.0"), r.Version)
	assert.Equal(t, "bob", r.CreateBy)
	assert.Equal(t, vo.StatusWait, r.ExecutionStatus)
}

func TestCreateBatchRecords(t *testing.T) {
	svc := New()
	cases := []*entity.UtCase{
		{CaseID: 1, CaseName: "Alpha", Version: "v1"},
		{CaseID: 2, CaseName: "Beta", Version: "v1"},
	}
	records, nameMap := svc.CreateBatchRecords(cases, vo.Version("v1"), "alice")
	require.Len(t, records, 2)
	assert.Len(t, nameMap, 2)
	assert.Equal(t, "Alpha", nameMap[1])
	assert.Equal(t, "Beta", nameMap[2])
	for _, r := range records {
		assert.Equal(t, vo.StatusWait, r.ExecutionStatus)
	}
}

func TestDispatchBatch_AllSuccess(t *testing.T) {
	svc := New()
	records := makeRecords(3)
	result := vo.BatchResult{SuccessIDs: []int64{1, 2, 3}}
	svc.DispatchBatch(records, result)
	for _, r := range records {
		assert.Equal(t, vo.StatusWait, r.ExecutionStatus)
	}
}

func TestDispatchBatch_AllFailed(t *testing.T) {
	svc := New()
	records := makeRecords(3)
	result := vo.BatchResult{FailedIDs: []int64{1, 2, 3}}
	svc.DispatchBatch(records, result)
	for _, r := range records {
		assert.Equal(t, vo.StatusDispatchFailed, r.ExecutionStatus)
	}
}

func TestDispatchBatch_Mixed(t *testing.T) {
	svc := New()
	records := makeRecords(3)
	result := vo.BatchResult{
		SuccessIDs: []int64{1, 3},
		FailedIDs:  []int64{2},
	}
	svc.DispatchBatch(records, result)
	assert.Equal(t, vo.StatusWait, records[0].ExecutionStatus)
	assert.Equal(t, vo.StatusDispatchFailed, records[1].ExecutionStatus)
	assert.Equal(t, vo.StatusWait, records[2].ExecutionStatus)
}

func TestDispatchBatch_UnknownIDUnchanged(t *testing.T) {
	svc := New()
	records := makeRecords(2)
	// 两条记录的 ID 都不在结果分区中。
	result := vo.BatchResult{SuccessIDs: []int64{99}, FailedIDs: []int64{100}}
	svc.DispatchBatch(records, result)
	for _, r := range records {
		assert.Equal(t, vo.StatusWait, r.ExecutionStatus)
	}
}

// makeRecords 创建 n 条 ExecutionRecord，ExecutionID 从 1 开始递增。
func makeRecords(n int) []*entity.ExecutionRecord {
	records := make([]*entity.ExecutionRecord, n)
	for i := 0; i < n; i++ {
		r := entity.NewExecutionRecord(int64(i+1), vo.Version("v1"), "tester")
		r.ExecutionID = int64(i + 1)
		records[i] = r
	}
	return records
}
