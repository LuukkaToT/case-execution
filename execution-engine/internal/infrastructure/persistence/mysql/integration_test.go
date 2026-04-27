//go:build integration

package mysql

import (
	"context"
	"os"
	"testing"

	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"gorm.io/gorm"
)

// testDB is the shared *gorm.DB opened once per test binary run.
var testDB *gorm.DB

func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		// No test DB configured — skip the whole package gracefully.
		os.Exit(0)
	}

	db, err := Open(Config{DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2})
	if err != nil {
		panic("open test DB: " + err.Error())
	}
	defer func() { _ = Close(db) }()

	// Auto-create tables from PO tags; idempotent on re-runs.
	if err := db.AutoMigrate(&po.ExecutionRecord{}, &po.UtCase{}); err != nil {
		panic("automigrate: " + err.Error())
	}

	testDB = db
	os.Exit(m.Run())
}

// withTx opens a real DB transaction, stores it in the returned context via
// WithTx, and registers a Rollback in t.Cleanup. All data written inside the
// returned ctx is invisible to other tests and leaves no dirty state.
func withTx(t *testing.T) context.Context {
	t.Helper()
	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return WithTx(context.Background(), tx)
}
