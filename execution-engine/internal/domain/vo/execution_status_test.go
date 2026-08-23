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
		{StatusWait, false},
		{StatusRunning, false},
		{StatusSuccess, true},
		{StatusFailed, true},
		{StatusDispatchFailed, true},
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
		// 合法迁移
		{StatusWait, StatusRunning, true},
		{StatusWait, StatusFailed, true},
		{StatusWait, StatusSuccess, true},
		{StatusWait, StatusDispatchFailed, true},
		{StatusRunning, StatusSuccess, true},
		{StatusRunning, StatusFailed, true},
		// 非法迁移：WAIT 不能保持自身。
		{StatusWait, StatusWait, false},
		// 非法迁移：执行中不能写成下发失败。
		{StatusRunning, StatusDispatchFailed, false},
		// 非法迁移：终态不能继续迁移。
		{StatusSuccess, StatusFailed, false},
		{StatusSuccess, StatusWait, false},
		{StatusFailed, StatusWait, false},
		{StatusFailed, StatusRunning, false},
		{StatusDispatchFailed, StatusWait, false},
		{StatusDispatchFailed, StatusFailed, false},
		{StatusDispatchFailed, StatusRunning, false},
		// 非法迁移：RUNNING 不能回退。
		{StatusRunning, StatusWait, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.legal, c.from.CanTransitTo(c.to), "%s -> %s", c.from, c.to)
	}
}
