// Package vo holds immutable domain value objects.
package vo

import "strings"

// Version is an immutable semantic version tag of a test case.
type Version string

// NewVersion trims whitespace; empty string becomes an empty Version and must
// be validated by the caller. Validation rules (regex etc) live in the app
// layer where they can surface to the user via grpc.InvalidArgument.
func NewVersion(s string) Version { return Version(strings.TrimSpace(s)) }

// String implements fmt.Stringer.
func (v Version) String() string { return string(v) }

// IsEmpty reports whether the version is the zero value.
func (v Version) IsEmpty() bool { return string(v) == "" }
