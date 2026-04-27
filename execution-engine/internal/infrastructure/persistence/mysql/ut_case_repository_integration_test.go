//go:build integration

package mysql

import (
	"context"
	"errors"
	"testing"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"
	"execution-engine/internal/infrastructure/persistence/mysql/po"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertUtCaseFixtures writes po.UtCase rows into the same transaction carried
// by ctx so they are rolled back automatically at test cleanup.
func insertUtCaseFixtures(t *testing.T, ctx context.Context, rows []po.UtCase) {
	t.Helper()
	db := FromCtx(ctx, testDB)
	for i := range rows {
		require.NoError(t, db.Create(&rows[i]).Error)
	}
}

// ---------------------------------------------------------------------------
// FindByID
// ---------------------------------------------------------------------------

func TestUtCaseRepository_FindByID_Found(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10001, CaseName: "Alpha", Version: "v1.0", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	got, err := repo.FindByID(ctx, 10001)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(10001), got.CaseID)
	assert.Equal(t, "Alpha", got.CaseName)
	assert.Equal(t, vo.Version("v1.0"), got.Version)
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
// CountByVersion
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
	n, err := repo.CountByVersion(ctx, vo.Version("v1.0"))
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
}

// ---------------------------------------------------------------------------
// CountByChannelVersion
// ---------------------------------------------------------------------------

func TestUtCaseRepository_CountByChannelVersion(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10020, CaseName: "C1", Version: "v1.0", Channel: "ios"},
		{CaseID: 10021, CaseName: "C2", Version: "v1.0", Channel: "android"},
		{CaseID: 10022, CaseName: "C3", Version: "v1.0", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	n, err := repo.CountByChannelVersion(ctx, vo.Channel("ios"), vo.Version("v1.0"))
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

// ---------------------------------------------------------------------------
// ScanByVersion
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
	err := repo.ScanByVersion(ctx, vo.Version("vscan"), 2, func(batch []*entity.UtCase) error {
		batchCalls++
		collected = append(collected, batch...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(collected))
	// batchSize=2, 3 rows → 2 callbacks (2 then 1)
	assert.Equal(t, 2, batchCalls)
}

// ---------------------------------------------------------------------------
// ScanByChannelVersion
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
	err := repo.ScanByChannelVersion(ctx, vo.Channel("ios"), vo.Version("vch"), 100, func(batch []*entity.UtCase) error {
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
// ScanByVersion — early abort via fn error
// ---------------------------------------------------------------------------

func TestUtCaseRepository_ScanByVersion_AbortOnFnError(t *testing.T) {
	ctx := withTx(t)
	insertUtCaseFixtures(t, ctx, []po.UtCase{
		{CaseID: 10050, CaseName: "A1", Version: "vabort", Channel: "ios"},
		{CaseID: 10051, CaseName: "A2", Version: "vabort", Channel: "ios"},
	})

	repo := NewUtCaseRepository(testDB)
	var calls int
	err := repo.ScanByVersion(ctx, vo.Version("vabort"), 1, func(_ []*entity.UtCase) error {
		calls++
		return errors.New("abort early")
	})
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}
