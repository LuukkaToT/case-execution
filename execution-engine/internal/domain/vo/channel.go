package vo

import "strings"

// Channel 表示用例渠道，例如协议族或产品线。
type Channel string

// NewChannel 只去除首尾空白，语义校验由应用层负责。
func NewChannel(s string) Channel { return Channel(strings.TrimSpace(s)) }

// String 实现 fmt.Stringer。
func (c Channel) String() string { return string(c) }

// IsEmpty 判断渠道是否为零值。
func (c Channel) IsEmpty() bool { return string(c) == "" }
