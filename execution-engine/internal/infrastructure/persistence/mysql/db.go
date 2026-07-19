// Package mysql 使用 GORM 实现持久化适配器，提供：
//
//   - Open/Close：管理连接生命周期。
//   - TxRunner：实现 usecase.TxRunner，通过 ctx 传递当前事务。
//   - WithTx/FromCtx：让仓储读取 ctx 中的事务，未找到时回退到根连接池。
package mysql

import (
	"context"
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config 对应 configs/config.yaml 中的 mysql 配置段。
type Config struct {
	DSN                string
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetimeSec int
}

// Open 初始化 GORM，并配置底层 sql.DB 连接池。
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

// Close 关闭数据库连接池。
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

// txKey 是私有 ctx key，使用未导出的空结构体类型可避免第三方键冲突。
type txKey struct{}

// WithTx 返回携带事务 tx 的新 ctx，FromCtx 负责读取。
func WithTx(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// FromCtx 优先返回 ctx 中的事务句柄，否则返回绑定 ctx 的根数据库句柄；仓储
// 方法应始终通过该函数获取 *gorm.DB。
func FromCtx(ctx context.Context, root *gorm.DB) *gorm.DB {
	if tx, ok := ctx.Value(txKey{}).(*gorm.DB); ok && tx != nil {
		return tx.WithContext(ctx)
	}
	return root.WithContext(ctx)
}

// TxRunner 基于 GORM 事务方法实现 usecase.TxRunner。
type TxRunner struct {
	db *gorm.DB
}

// NewTxRunner 创建绑定指定数据库句柄的事务执行器。
func NewTxRunner(db *gorm.DB) *TxRunner { return &TxRunner{db: db} }

// Do 在 GORM 事务中执行 fn。传给 fn 的子 ctx 携带事务句柄，fn 内调用的
// 任意仓储方法都可通过 FromCtx 自动加入同一事务。
func (r *TxRunner) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(WithTx(ctx, tx))
	})
}
