// Package errs enumerates domain-level errors. Handlers map these onto
// gRPC status codes; use errors.Is/errors.As for detection.
package errs

import (
	"errors"
	"fmt"
)

// Sentinel errors; wrap them with %w to attach context.
var (
	ErrCaseNotExist            = errors.New("case not exist")
	ErrExecutionNotFound       = errors.New("execution record not found")
	ErrIllegalStatusTransition = errors.New("illegal execution status transition")
	ErrInvalidArgument         = errors.New("invalid argument")
)

// NewCaseNotExist wraps ErrCaseNotExist with the offending case id.
func NewCaseNotExist(caseID int64) error {
	return fmt.Errorf("%w: caseID=%d", ErrCaseNotExist, caseID)
}

// NewExecutionNotFound wraps ErrExecutionNotFound with the offending execution id.
func NewExecutionNotFound(executionID int64) error {
	return fmt.Errorf("%w: executionID=%d", ErrExecutionNotFound, executionID)
}

// NewIllegalStatusTransition wraps ErrIllegalStatusTransition.
func NewIllegalStatusTransition(executionID int64, from, to string) error {
	return fmt.Errorf("%w: executionID=%d %s->%s", ErrIllegalStatusTransition, executionID, from, to)
}

// NewInvalidArgument wraps ErrInvalidArgument with a human message.
func NewInvalidArgument(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, msg)
}
