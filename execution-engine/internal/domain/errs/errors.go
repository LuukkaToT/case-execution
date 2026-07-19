// Package errs 定义领域错误，handler 将其映射为 gRPC 状态码；调用方使用
// errors.Is/errors.As 判断具体类型。
package errs

import (
	"errors"
	"fmt"
)

// 哨兵错误，附加上下文时应使用 %w 包装。
var (
	ErrCaseNotExist            = errors.New("case not exist")
	ErrExecutionNotFound       = errors.New("execution record not found")
	ErrIllegalStatusTransition = errors.New("illegal execution status transition")
	ErrInvalidArgument         = errors.New("invalid argument")
)

// NewCaseNotExist 为 ErrCaseNotExist 附加对应 case_id。
func NewCaseNotExist(caseID int64) error {
	return fmt.Errorf("%w: caseID=%d", ErrCaseNotExist, caseID)
}

// NewExecutionNotFound 为 ErrExecutionNotFound 附加对应 execution_id。
func NewExecutionNotFound(executionID int64) error {
	return fmt.Errorf("%w: executionID=%d", ErrExecutionNotFound, executionID)
}

// NewIllegalStatusTransition 为 ErrIllegalStatusTransition 附加迁移上下文。
func NewIllegalStatusTransition(executionID int64, from, to string) error {
	return fmt.Errorf("%w: executionID=%d %s->%s", ErrIllegalStatusTransition, executionID, from, to)
}

// NewInvalidArgument 为 ErrInvalidArgument 附加可读错误信息。
func NewInvalidArgument(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, msg)
}
