package errs

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCaseNotExist(t *testing.T) {
	err := NewCaseNotExist(42)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCaseNotExist))
	assert.Contains(t, err.Error(), "42")
}

func TestNewExecutionNotFound(t *testing.T) {
	err := NewExecutionNotFound(99)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExecutionNotFound))
	assert.Contains(t, err.Error(), "99")
}

func TestNewIllegalStatusTransition(t *testing.T) {
	err := NewIllegalStatusTransition(7, "WAIT", "RUNNING")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIllegalStatusTransition))
	assert.True(t, strings.Contains(err.Error(), "WAIT"))
	assert.True(t, strings.Contains(err.Error(), "RUNNING"))
}

func TestNewInvalidArgument(t *testing.T) {
	err := NewInvalidArgument("version is required")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidArgument))
	assert.Contains(t, err.Error(), "version is required")
}
