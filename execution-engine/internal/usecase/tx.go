package usecase

import "context"

// TxRunner lets the usecase wrap a block of work in an ACID boundary without
// knowing about gorm.DB. The infrastructure adapter injects the transactional
// handle into the child ctx so repositories called inside fn participate in
// the same transaction transparently.
type TxRunner interface {
	// Do invokes fn under a transaction. Returning a non-nil error rolls
	// back; returning nil commits. The child ctx carries the transaction
	// handle and cancels with the parent ctx.
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}
