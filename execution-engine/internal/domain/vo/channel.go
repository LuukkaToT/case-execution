package vo

import "strings"

// Channel identifies a test-case channel (e.g. protocol family / product line).
type Channel string

// NewChannel trims whitespace only; semantic validation is done at the app layer.
func NewChannel(s string) Channel { return Channel(strings.TrimSpace(s)) }

// String implements fmt.Stringer.
func (c Channel) String() string { return string(c) }

// IsEmpty reports whether the channel is the zero value.
func (c Channel) IsEmpty() bool { return string(c) == "" }
