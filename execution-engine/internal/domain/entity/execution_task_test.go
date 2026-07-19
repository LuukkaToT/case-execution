package entity

import (
	"testing"

	"execution-engine/internal/domain/vo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRecord(executionID, caseID int64, v vo.Version) *ExecutionRecord {
	r := NewExecutionRecord(caseID, v, "u")
	r.ExecutionID = executionID
	return r
}

func TestBatchAssemble_Normal(t *testing.T) {
	records := []*ExecutionRecord{
		makeRecord(1, 10, "v1.0"),
		makeRecord(2, 20, "v1.0"),
	}
	nameMap := map[int64]string{10: "CaseA", 20: "CaseB"}

	tasks := BatchAssemble(records, nameMap)
	require.Len(t, tasks, 2)
	assert.Equal(t, int64(1), tasks[0].ExecutionID)
	assert.Equal(t, int64(10), tasks[0].CaseID)
	assert.Equal(t, "CaseA", tasks[0].CaseName)
	assert.Equal(t, int64(2), tasks[1].ExecutionID)
	assert.Equal(t, "CaseB", tasks[1].CaseName)
}

func TestBatchAssemble_SkipMissingName(t *testing.T) {
	records := []*ExecutionRecord{
		makeRecord(1, 10, "v1.0"),
		makeRecord(2, 20, "v1.0"), // caseID 20 missing from nameMap
	}
	nameMap := map[int64]string{10: "CaseA"}

	tasks := BatchAssemble(records, nameMap)
	require.Len(t, tasks, 1)
	assert.Equal(t, int64(1), tasks[0].ExecutionID)
}

func TestBatchAssemble_Empty(t *testing.T) {
	tasks := BatchAssemble(nil, map[int64]string{})
	assert.Empty(t, tasks)
}
