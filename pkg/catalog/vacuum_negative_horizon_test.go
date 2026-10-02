package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestVacuumLongRetentionRetainsTombstones pins the documented contract of
// AutoVacuumRetention ("the minimum age of a soft-deleted row before
// AutoVacuum physically removes it", database_config.go:96-100): a retention
// horizon far longer than the age of a tombstone must RETAIN it.
//
// vacuumTreeLocked (catalog_maintenance.go) computes
//
//	horizonSec := time.Now().Add(-retentionHorizon).Unix()
//
// and branches:
//
//	if horizonSec > 0 {
//	    ... retained unless deletedAt <= horizonSec ...
//	} else if bytesContainDeletedAt(value) {
//	    continue   // purge every dead row
//	}
//
// The `else` branch is only reachable when horizonSec <= 0. But horizonSec is
// POSITIVE for every ordinary retention, including the documented 0 sentinel
// (horizonSec == now), so that branch never fires for a legitimate horizon --
// it fires only when retention exceeds the Unix epoch age (~56.6 years), and
// there it purges every tombstone, the exact opposite of a long retention.
//
// Observed: retention=100y physically removed a tombstone created moments
// earlier. Expected: retained (100y > 0s).
func TestVacuumLongRetentionRetainsTombstones(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "vac_neg",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "u", Type: query.TokenText}},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	insert := func(id int64, u string) {
		t.Helper()
		if _, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "vac_neg",
			Columns: []string{"id", "u"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(id)}, &query.StringLiteral{Value: u}}},
		}, nil); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	deleteID := func(id int64) {
		t.Helper()
		if _, _, err := c.Delete(ctx, &query.DeleteStmt{
			Table: "vac_neg",
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: float64(id)},
			},
		}, nil); err != nil {
			t.Fatalf("delete %d: %v", id, err)
		}
	}

	insert(1, "a")
	insert(2, "b")
	deleteID(2)

	// A 100-year retention must retain a tombstone created moments ago.
	if err := c.VacuumTable("vac_neg", 100*365*24*time.Hour); err != nil {
		t.Fatalf("VacuumTable(100y): %v", err)
	}
	if n := physicalKeyCount(t, c, "vac_neg"); n != 2 {
		t.Fatalf("FAIL: retention=100y must RETAIN a just-deleted tombstone "+
			"(horizonSec = now-100y is negative, so vacuumTreeLocked took the "+
			"legacy purge-all branch): %d keys, want 2", n)
	}
	if !rowLive(t, c, "vac_neg", 1) {
		t.Fatalf("FAIL: live row lost")
	}

	// Control: the documented 0 sentinel removes all dead rows. This must hold
	// before AND after any fix -- it proves the proof targets the negative
	// horizonSec case specifically.
	if err := c.VacuumTable("vac_neg", 0); err != nil {
		t.Fatalf("VacuumTable(0): %v", err)
	}
	if n := physicalKeyCount(t, c, "vac_neg"); n != 1 {
		t.Fatalf("CONTROL FAILED: retention==0 is documented to remove all dead rows, got %d keys, want 1", n)
	}
	t.Logf("CONTROL PASS: retention=0 purged the tombstone as documented")
}
