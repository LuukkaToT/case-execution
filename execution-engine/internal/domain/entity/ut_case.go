// Package entity 包含聚合根及其他可变领域对象。
package entity

import "execution-engine/internal/domain/vo"

// UtCase 是从主用例目录读取的用例定义。这里只建模下发所需字段，完整的
// 用例领域语义仍由 Python Web 维护。
type UtCase struct {
	CaseID   int64
	CaseName string
	Version  vo.Version
	Channel  vo.Channel
}
