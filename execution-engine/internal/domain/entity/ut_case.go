// Package entity holds aggregate roots and other mutable domain objects.
package entity

import "execution-engine/internal/domain/vo"

// UtCase is a test-case definition fetched from the master catalog.
// Only the fields participating in dispatch are modelled here; full UT case
// semantics live in the Python domain.
type UtCase struct {
	CaseID   int64
	CaseName string
	Version  vo.Version
	Channel  vo.Channel
}
