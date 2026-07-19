package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPercentileMs(t *testing.T) {
	values := []time.Duration{
		50 * time.Millisecond,
		10 * time.Millisecond,
		40 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
	}

	assert.Equal(t, float64(30), percentileMs(values, 0.50))
	assert.Equal(t, float64(50), percentileMs(values, 0.95))
}

func TestPercentileMs_空输入(t *testing.T) {
	assert.Zero(t, percentileMs(nil, 0.95))
}
