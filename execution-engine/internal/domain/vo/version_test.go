package vo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVersion(t *testing.T) {
	assert.Equal(t, Version("v1.0"), NewVersion("v1.0"))
	assert.Equal(t, Version("v1.0"), NewVersion("  v1.0  "))
	assert.Equal(t, Version(""), NewVersion(""))
	assert.Equal(t, Version(""), NewVersion("   "))
}

func TestVersionIsEmpty(t *testing.T) {
	assert.True(t, NewVersion("").IsEmpty())
	assert.True(t, NewVersion("  ").IsEmpty())
	assert.False(t, NewVersion("v1.0").IsEmpty())
}

func TestVersionString(t *testing.T) {
	assert.Equal(t, "v1.0", NewVersion("v1.0").String())
}
