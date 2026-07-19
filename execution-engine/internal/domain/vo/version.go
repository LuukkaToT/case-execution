// Package vo 包含不可变领域值对象。
package vo

import "strings"

// Version 是用例不可变的版本标识。
type Version string

// NewVersion 去除首尾空白。空字符串会生成空 Version，由调用方校验；正则等
// 业务规则放在应用层，以便通过 grpc.InvalidArgument 返回给调用方。
func NewVersion(s string) Version { return Version(strings.TrimSpace(s)) }

// String 实现 fmt.Stringer。
func (v Version) String() string { return string(v) }

// IsEmpty 判断版本是否为零值。
func (v Version) IsEmpty() bool { return string(v) == "" }
