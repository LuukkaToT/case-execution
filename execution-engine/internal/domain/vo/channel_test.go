package vo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewChannel(t *testing.T) {
	assert.Equal(t, Channel("ios"), NewChannel("ios"))
	assert.Equal(t, Channel("ios"), NewChannel("  ios  "))
	assert.Equal(t, Channel(""), NewChannel(""))
	assert.Equal(t, Channel(""), NewChannel("   "))
}

func TestChannelIsEmpty(t *testing.T) {
	assert.True(t, NewChannel("").IsEmpty())
	assert.True(t, NewChannel("  ").IsEmpty())
	assert.False(t, NewChannel("ios").IsEmpty())
}

func TestChannelString(t *testing.T) {
	assert.Equal(t, "android", NewChannel("android").String())
}
