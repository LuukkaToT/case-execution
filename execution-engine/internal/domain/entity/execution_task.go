package entity

import "execution-engine/internal/domain/vo"

// ExecutionTask 是发送到 MQ 的任务模型。它与 ExecutionRecord 分离，避免
// 消息协议和持久化聚合相互耦合。
type ExecutionTask struct {
	ExecutionID int64
	CaseID      int64
	CaseName    string
	Version     vo.Version
}

// BatchAssemble 根据执行记录和 case_id 到 case_name 的映射组装 MQ 任务，
// 与 Python ExecutionTask.batch_assemble 语义一致。缺少名称映射的记录会被
// 跳过，避免执行机收到 case_name 为空的无效任务。
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
