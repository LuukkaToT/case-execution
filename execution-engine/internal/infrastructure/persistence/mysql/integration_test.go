//go:build integration

package mysql

import (
	"context"
	"os"
	"testing"

	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"gorm.io/gorm"
)

// testDB 是每次测试进程只打开一次、由本包集成测试共享的数据库连接。
var testDB *gorm.DB

func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		// 未配置测试数据库时，直接跳过整个包的集成测试。
		os.Exit(0)
	}

	db, err := Open(Config{DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2})
	if err != nil {
		panic("open test DB: " + err.Error())
	}
	defer func() { _ = Close(db) }()

	// 根据 PO 标签自动建表；重复执行是幂等的。
	if err := db.AutoMigrate(&po.ExecutionRecord{}, &po.UtCase{}, &po.OutboxMessage{}); err != nil {
		panic("automigrate: " + err.Error())
	}

	testDB = db
	os.Exit(m.Run())
}

// withTx 开启真实数据库事务，通过 WithTx 写入返回的上下文，
// 并在 t.Cleanup 中注册回滚。该上下文内写入的数据对其他测试不可见，也不会留下脏数据。
func withTx(t *testing.T) context.Context {
	t.Helper()
	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return WithTx(context.Background(), tx)
}
