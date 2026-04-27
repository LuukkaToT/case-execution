package vo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTerminal(t *testing.T) {
	cases := []struct {
		status   ExecutionStatus
		terminal bool
	}{
		{StatusInit, false},
		{StatusWait, false},
		{StatusRunning, false},
		{StatusSuccess, true},
		{StatusFailed, true},
	}
	for _, c := range cases {
		assert.Equal(t, c.terminal, c.status.IsTerminal(), "status=%s", c.status)
	}
}

func TestCanTransitTo(t *testing.T) {
	cases := []struct {
		from  ExecutionStatus
		to    ExecutionStatus
		legal bool
	}{
		// Legal edges
		{StatusInit, StatusWait, true},
		{StatusInit, StatusFailed, true},
		{StatusWait, StatusRunning, true},
		{StatusWait, StatusFailed, true},
		{StatusWait, StatusSuccess, true},
		{StatusRunning, StatusSuccess, true},
		{StatusRunning, StatusFailed, true},
		// Illegal: INIT cannot go to RUNNING/SUCCESS
		{StatusInit, StatusRunning, false},
		{StatusInit, StatusSuccess, false},
		// Illegal: terminal states cannot transition anywhere
		{StatusSuccess, StatusFailed, false},
		{StatusSuccess, StatusWait, false},
		{StatusFailed, StatusWait, false},
		{StatusFailed, StatusRunning, false},
		// Illegal: RUNNING cannot go back
		{StatusRunning, StatusWait, false},
		{StatusRunning, StatusInit, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.legal, c.from.CanTransitTo(c.to), "%s -> %s", c.from, c.to)
	}
}
