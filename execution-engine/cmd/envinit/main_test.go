package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLooksLikeTestResource(t *testing.T) {
	assert.True(t, looksLikeTestResource("ut.exec.bench.20260720"))
	assert.True(t, looksLikeTestResource("qa-ut-exec"))
	assert.False(t, looksLikeTestResource("ut.exec"))
}

func TestSafeIdentifier(t *testing.T) {
	assert.True(t, safeIdentifier.MatchString("case_execution_test"))
	assert.False(t, safeIdentifier.MatchString("case-execution;drop"))
}
