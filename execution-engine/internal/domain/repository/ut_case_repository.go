package repository

import (
	"context"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// UtCaseScanFn 在流式扫描每生成一个分片时调用一次；返回非 nil error 可提前
// 终止扫描，扫描方法会继续向上传递该错误。
type UtCaseScanFn func(batch []*entity.UtCase) error

// UtCaseRepository 是用例目录的读模型。引擎只读取，写入仍由上游 Python Web
// 或用例目录服务负责。
//
// 接口刻意不提供一次性返回 []*UtCase 的 FindByVersion；下发流水线通过
// ScanByVersion/ScanByChannelVersion 分片消费，使内存不随目录规模线性增长。
type UtCaseRepository interface {
	FindByID(ctx context.Context, caseID int64) (*entity.UtCase, error)

	CountByVersion(ctx context.Context, v vo.Version) (int64, error)
	CountByChannelVersion(ctx context.Context, ch vo.Channel, v vo.Version) (int64, error)

	// ScanByVersion 按 size 分片遍历目录，并对每个分片调用 fn；ctx 取消或 fn
	// 返回非 nil error 时必须尽快终止。
	ScanByVersion(ctx context.Context, v vo.Version, size int, fn UtCaseScanFn) error

	// ScanByChannelVersion 是按渠道和版本过滤的分片扫描实现。
	ScanByChannelVersion(ctx context.Context, ch vo.Channel, v vo.Version, size int, fn UtCaseScanFn) error
}
