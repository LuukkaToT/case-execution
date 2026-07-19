//go:build integration

package mysql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testCaseIDBase = time.Now().UnixMilli() * 1000
	testRunPrefix  = fmt.Sprintf("it-%d-", time.Now().UnixNano())
)

func integrationCaseID(id int64) int64 { return testCaseIDBase + id }

func integrationVersion(version string) vo.Version {
	return vo.Version(testRunPrefix + version)
}

// insertUtCaseFixtures 将 po.UtCase 测试数据写入 ctx 携带的同一事务，
// 以便测试清理阶段自动回滚。
func insertUtCaseFixtures(t *testing.T, ctx context.Context, rows []po.UtCase) {
	t.Helper()
	db := FromCtx(ctx, testDB)
	for i := range rows {
		rows[i].CaseID = integrationCaseID(rows[i].CaseID)
		rows[i].Version = string(integrationVersion(rows[i].Version))
		require.NoError(t, db.Create(&rows[i]).Error)
	}
}

// ---------------------------------------------------------------------------
// 按编号查询
// ---------------------------------------------------------------------------

func TestUtCaseRepository_FindByID_Found(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10001, CaseName: "Alpha", Version: "v1.0", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	got, err := repo.FindByID(ctx, integrationCaseID(10001))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, integrationCaseID(10001), got.CaseID)
	assert.Equal(t, "Alpha", got.CaseName)
	assert.Equal(t, integrationVersion("v1.0"), got.Version)
	assert.Equal(t, vo.Channel("ios"), got.Channel)
}

func TestUtCaseRepository_FindByID_NotFound(t *testing.T) {
	ctx := withTx(t)
	repo := NewUtCaseRepository(testDB)

	got, err := repo.FindByID(ctx, 999999999)
	require.NoError(t, err)
	assert.Nil(t, got)
}

// ---------------------------------------------------------------------------
// 按版本计数
// ---------------------------------------------------------------------------

func TestUtCaseRepository_CountByVersion(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10010, CaseName: "C1", Version: "v1.0", Channel: "ios"},
		{CaseID: 10011, CaseName: "C2", Version: "v1.0", Channel: "android"},
		{CaseID: 10012, CaseName: "C3", Version: "v1.0", Channel: "ios"},
		{CaseID: 10013, CaseName: "C4", Version: "v2.0", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	n, err := repo.CountByVersion(ctx, integrationVersion("v1.0"))
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
}

// ---------------------------------------------------------------------------
// 按渠道与版本计数
// ---------------------------------------------------------------------------

func TestUtCaseRepository_CountByChannelVersion(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10020, CaseName: "C1", Version: "v1.0", Channel: "ios"},
		{CaseID: 10021, CaseName: "C2", Version: "v1.0", Channel: "android"},
		{CaseID: 10022, CaseName: "C3", Version: "v1.0", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	n, err := repo.CountByChannelVersion(ctx, vo.Channel("ios"), integrationVersion("v1.0"))
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

// ---------------------------------------------------------------------------
// 按版本游标扫描
// ---------------------------------------------------------------------------

func TestUtCaseRepository_ScanByVersion(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10030, CaseName: "S1", Version: "vscan", Channel: "ios"},
		{CaseID: 10031, CaseName: "S2", Version: "vscan", Channel: "ios"},
		{CaseID: 10032, CaseName: "S3", Version: "vscan", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	var collected []*entity.UtCase
	var batchCalls int
	err := repo.ScanByVersion(ctx, integrationVersion("vscan"), 2, func(batch []*entity.UtCase) error {
		batchCalls++
		collected = append(collected, batch...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(collected))
	// batchSize=2，3 行数据应触发 2 次回调（先 2 行，再 1 行）。
	assert.Equal(t, 2, batchCalls)
}

// ---------------------------------------------------------------------------
// 按渠道与版本游标扫描
// ---------------------------------------------------------------------------

func TestUtCaseRepository_ScanByChannelVersion(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10040, CaseName: "SC1", Version: "vch", Channel: "ios"},
		{CaseID: 10041, CaseName: "SC2", Version: "vch", Channel: "android"},
		{CaseID: 10042, CaseName: "SC3", Version: "vch", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	var collected []*entity.UtCase
	err := repo.ScanByChannelVersion(ctx, vo.Channel("ios"), integrationVersion("vch"), 100, func(batch []*entity.UtCase) error {
		collected = append(collected, batch...)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, collected, 2)
	for _, c := range collected {
		assert.Equal(t, vo.Channel("ios"), c.Channel)
	}
}

// ---------------------------------------------------------------------------
// 按版本扫描：回调返回错误时提前终止
// ---------------------------------------------------------------------------

func TestUtCaseRepository_ScanByVersion_AbortOnFnError(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10050, CaseName: "A1", Version: "vabort", Channel: "ios"},
		{CaseID: 10051, CaseName: "A2", Version: "vabort", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	var calls int
	err := repo.ScanByVersion(ctx, integrationVersion("vabort"), 1, func(_ []*entity.UtCase) error {
		calls++
		return errors.New("abort early")
	})
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}
