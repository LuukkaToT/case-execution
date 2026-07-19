// Package logger 对 zap 做轻量封装，供整个执行引擎统一使用。
package logger

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	once   sync.Once
	logger *zap.Logger
)

// Config 控制日志输出格式和级别。
type Config struct {
	Level    string
	Encoding string
}

// Init 初始化全局日志器，应在进程启动时调用一次。
func Init(cfg Config) error {
	var err error
	once.Do(func() {
		level := zap.InfoLevel
		_ = level.UnmarshalText([]byte(cfg.Level))

		encoding := cfg.Encoding
		if encoding == "" {
			encoding = "console"
		}

		encCfg := zap.NewProductionEncoderConfig()
		encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encCfg.EncodeLevel = zapcore.CapitalLevelEncoder

		zcfg := zap.Config{
			Level:            zap.NewAtomicLevelAt(level),
			Development:      false,
			DisableCaller:    false,
			Encoding:         encoding,
			EncoderConfig:    encCfg,
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
		}
		logger, err = zcfg.Build(zap.AddCallerSkip(0))
	})
	return err
}

// L 返回全局日志器；Init 调用前返回空操作日志器。
func L() *zap.Logger {
	if logger == nil {
		return zap.NewNop()
	}
	return logger
}

// Sync 刷新缓冲区中的日志，应在进程退出前调用。
func Sync() {
	if logger != nil {
		_ = logger.Sync()
	}
}
