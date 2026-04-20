// Package mysql wires gorm as the persistence adapter. It exposes:
//
//   - Open / Close: connection lifecycle.
//   - TxRunner:     usecase.TxRunner implementation; propagates the active
//     transaction via ctx so repositories don't need a second parameter.
//   - WithTx/FromCtx: private plumbing for repositories to look up whichever
//     *gorm.DB they should use, falling back to the pooled root handle.
package mysql

import (
	"context"
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config mirrors the mysql section of configs/config.yaml.
type Config struct {
	DSN                string
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetimeSec int
}

// Open initialises a gorm DB and tunes the sql.DB pool.
func Open(cfg Config) (*gorm.DB, error) {
	gdb, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("gorm open: %w", err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm underlying db: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetimeSec > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeSec) * time.Second)
	}
	return gdb, nil
}

// Close drains the connection pool.
func Close(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// txKey is the private ctx key; using an unexported empty-struct type
// guarantees no third-party code can collide.
type txKey struct{}

// WithTx returns a new ctx that carries tx; FromCtx looks it up.
func WithTx(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// FromCtx retrieves the active *gorm.DB: the tx if one is attached, else
// the root db with ctx applied. Repositories should always call this.
func FromCtx(ctx context.Context, root *gorm.DB) *gorm.DB {
	if tx, ok := ctx.Value(txKey{}).(*gorm.DB); ok && tx != nil {
		return tx.WithContext(ctx)
	}
	return root.WithContext(ctx)
}

// TxRunner implements usecase.TxRunner on top of gorm's transaction helper.
type TxRunner struct {
	db *gorm.DB
}

// NewTxRunner constructs a TxRunner bound to the given db handle.
func NewTxRunner(db *gorm.DB) *TxRunner { return &TxRunner{db: db} }

// Do runs fn inside a gorm transaction. The child ctx handed to fn carries
// the *gorm.DB transactional handle; fn may call any repository method and
// they will see the transaction transparently via FromCtx.
func (r *TxRunner) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(WithTx(ctx, tx))
	})
}
