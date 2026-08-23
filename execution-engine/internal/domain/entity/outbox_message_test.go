package entity

import (
	"testing"

	"execution-engine/internal/domain/vo"

	"github.com/stretchr/testify/assert"
)

func TestOutboxMessage_ReplayStatus(t *testing.T) {
	cases := []struct {
		state  OutboxState
		status vo.ExecutionStatus
		done   bool
	}{
		{OutboxPending, "", false},
		{OutboxProcessing, vo.StatusWait, true},
		{OutboxPublished, vo.StatusWait, true},
		{OutboxDead, vo.StatusDispatchFailed, true},
	}
	for _, c := range cases {
		status, done := (&OutboxMessage{State: c.state}).ReplayStatus()
		assert.Equal(t, c.status, status, "state=%s", c.state)
		assert.Equal(t, c.done, done, "state=%s", c.state)
	}
}
