package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestVacuumRetentionHorizonPurgesOldTombstones pins the documented
// VacuumTable retention contract: "removing soft-deleted rows whose
// deleted_at timestamp is at or before horizonNS; rows deleted more
// recently are retained for AS OF SYSTEM TIME temporal history."
//
// The horizon path was broken three ways: vacuumExtractDeletedAt compared
// a 12-byte slice against the 13-byte "deleted_at": literal (never matched,
// and its search window could not reach the key start); the horizon was
// computed in UnixNano while row timestamps are Unix seconds; and the
// retention branch inverted the doc semantics. Net effect: tombstones were
// never purged and dead-tuple accounting was blinded.
func TestVacuumRetentionHorizonPurgesOldTombstones(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vac_ret",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "u", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	insert := func(id int64, u string) {
		t.Helper()
		if _, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "vac_ret",
			Columns: []string{"id", "u"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(id)}, &query.StringLiteral{Value: u}}},
		}, nil); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	insert(1, "a")
	insert(2, "b")
	insert(3, "c-old")

	// Delete row 3, then backdate its tombstone so it is clearly older than
	// the retention horizon used below.
	if _, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "vac_ret",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 3},
		},
	}, nil); err != nil {
		t.Fatalf("delete 3: %v", err)
	}
	backdateOldestTombstone(t, c, "vac_ret", 1, 2*time.Hour)

	// Probe A (the defect): a tombstone older than the horizon must be
	// physically purged by VacuumTable.
	if err := c.VacuumTable("vac_ret", 1*time.Hour); err != nil {
		t.Fatalf("VacuumTable: %v", err)
	}
	if n := physicalKeyCount(t, c, "vac_ret"); n != 2 {
		t.Fatalf("FAIL: old tombstone survived VacuumTable: %d keys, want 2", n)
	}
	if !rowLive(t, c, "vac_ret", 1) || !rowLive(t, c, "vac_ret", 2) {
		t.Fatalf("FAIL: live rows lost by vacuum")
	}

	// Probe B (retention control): a recent tombstone must be retained.
	insert(4, "d")
	if _, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "vac_ret",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 4},
		},
	}, nil); err != nil {
		t.Fatalf("delete 4: %v", err)
	}
	if err := c.VacuumTable("vac_ret", 1*time.Hour); err != nil {
		t.Fatalf("VacuumTable (recent): %v", err)
	}
	if n := physicalKeyCount(t, c, "vac_ret"); n != 3 {
		t.Fatalf("FAIL: recent tombstone was purged: %d keys, want 3", n)
	}

	// Probe C (legacy control): Vacuum(0) removes all dead rows.
	if err := c.Vacuum(0); err != nil {
		t.Fatalf("Vacuum(0): %v", err)
	}
	if n := physicalKeyCount(t, c, "vac_ret"); n != 2 {
		t.Fatalf("FAIL: Vacuum(0) left a tombstone: %d keys, want 2", n)
	}
}

// physicalKeyCount counts raw tree entries for a table (live + tombstones).
func physicalKeyCount(t *testing.T, c *Catalog, table string) int {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	tr, ok := c.tableTrees[table]
	if !ok {
		t.Fatalf("no tree for %s", table)
	}
	iter, err := tr.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer iter.Close()
	n := 0
	for iter.HasNext() {
		k, _, err := iter.Next()
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if k == nil {
			break
		}
		n++
	}
	return n
}

// rowLive reports whether a row with the given id is live in the table.
func rowLive(t *testing.T, c *Catalog, table string, id int64) bool {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	tr, ok := c.tableTrees[table]
	if !ok {
		t.Fatalf("no tree for %s", table)
	}
	iter, err := tr.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer iter.Close()
	for iter.HasNext() {
		_, v, err := iter.Next()
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if v == nil {
			break
		}
		row, ok, err := decodeLiveRow(v, 2)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !ok {
			continue
		}
		if f, ok := row[0].(int64); ok && f == id {
			return true
		}
	}
	return false
}

// backdateOldestTombstone finds the (single) tombstone in the table and sets
// its DeletedAt to now-age. If none exists the test fails.
func backdateOldestTombstone(t *testing.T, c *Catalog, table string, expect int, age time.Duration) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	tr, ok := c.tableTrees[table]
	if !ok {
		t.Fatalf("no tree for %s", table)
	}
	old := time.Now().Add(-age).Unix()
	iter, err := tr.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer iter.Close()
	found := 0
	for iter.HasNext() {
		k, v, err := iter.Next()
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if k == nil {
			break
		}
		vrow, err := decodeVersionedRow(v, 2)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if vrow.Version.DeletedAt == 0 {
			continue
		}
		found++
		fixed, err := encodeVersionedRowFull(vrow.Data, RowVersion{
			CreatedAt: vrow.Version.CreatedAt,
			DeletedAt: old,
		})
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if err := tr.Put(k, fixed); err != nil {
			t.Fatalf("put backdated tombstone: %v", err)
		}
	}
	if found != expect {
		t.Fatalf("expected %d tombstones to backdate, found %d", expect, found)
	}
}
