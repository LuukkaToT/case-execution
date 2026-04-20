package entity

import "execution-engine/internal/domain/vo"

// ExecutionTask is the MQ payload model (value object flavoured). It exists
// separately from ExecutionRecord so the messaging format stays decoupled
// from the persistence aggregate.
type ExecutionTask struct {
	ExecutionID int64
	CaseID      int64
	CaseName    string
	Version     vo.Version
}

// BatchAssemble zips records with their case-name map to produce MQ-bound
// tasks, 1:1 port of the Python ExecutionTask.batch_assemble. Records whose
// case_id is missing from nameMap are skipped rather than producing a task
// with an empty case_name, which would confuse the worker.
func BatchAssemble(records []*ExecutionRecord, nameMap map[int64]string) []ExecutionTask {
	tasks := make([]ExecutionTask, 0, len(records))
	for _, r := range records {
		name, ok := nameMap[r.CaseID]
		if !ok {
			continue
		}
		tasks = append(tasks, ExecutionTask{
			ExecutionID: r.ExecutionID,
			CaseID:      r.CaseID,
			CaseName:    name,
			Version:     r.Version,
		})
	}
	return tasks
}
