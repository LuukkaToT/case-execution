// Package logger provides a thin wrapper over zap, used across the engine.
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

// Config controls log output.
type Config struct {
	Level    string
	Encoding string
}

// Init initializes the global logger. Safe to call once.
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

// L returns the global logger. Falls back to a no-op logger before Init.
func L() *zap.Logger {
	if logger == nil {
		return zap.NewNop()
	}
	return logger
}

// Sync flushes buffered log entries; call on shutdown.
func Sync() {
	if logger != nil {
		_ = logger.Sync()
	}
}
