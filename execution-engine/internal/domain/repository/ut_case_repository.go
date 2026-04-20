package repository

import (
	"context"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
)

// UtCaseScanFn is invoked once per chunk produced by a streaming scan. Return
// a non-nil error to abort the scan early (the scan method propagates it).
type UtCaseScanFn func(batch []*entity.UtCase) error

// UtCaseRepository is the read model over the test-case catalog. The engine
// only reads; writes happen upstream (Python / catalogue service).
//
// Note the deliberate absence of a bulk FindByVersion returning []*UtCase -
// the dispatch pipeline consumes cases via ScanByVersion/ScanByChannelVersion
// so that memory stays bounded regardless of catalogue size.
type UtCaseRepository interface {
	FindByID(ctx context.Context, caseID int64) (*entity.UtCase, error)

	CountByVersion(ctx context.Context, v vo.Version) (int64, error)
	CountByChannelVersion(ctx context.Context, ch vo.Channel, v vo.Version) (int64, error)

	// ScanByVersion walks the catalogue in chunks of `size`, invoking fn
	// for every chunk. It MUST abort promptly on ctx cancellation or when
	// fn returns a non-nil error.
	ScanByVersion(ctx context.Context, v vo.Version, size int, fn UtCaseScanFn) error

	// ScanByChannelVersion is the (channel, version) variant.
	ScanByChannelVersion(ctx context.Context, ch vo.Channel, v vo.Version, size int, fn UtCaseScanFn) error
}
