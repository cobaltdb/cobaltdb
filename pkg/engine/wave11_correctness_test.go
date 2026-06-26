package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// TestRoundNegativePrecision verifies ROUND(x, n) with negative n rounds to the
// left of the decimal point (it previously returned the value unchanged).
func TestRoundNegativePrecision(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	cases := map[string]float64{
		"SELECT ROUND(123.456, -1)": 120,
		"SELECT ROUND(127, -1)":     130,
		"SELECT ROUND(1234, -2)":    1200,
		"SELECT ROUND(3.14159, 2)":  3.14, // positive precision still correct
	}
	for sql, want := range cases {
		var got float64
		if err := db.QueryRow(ctx, sql).Scan(&got); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if got != want {
			t.Errorf("%s = %v, want %v", sql, got, want)
		}
	}
}

// TestHavingWithoutGroupByOrSelectAggregate verifies HAVING is applied even when
// no aggregate appears in the SELECT list and there is no GROUP BY (whole table
// is one group).
func TestHavingWithoutGroupByOrSelectAggregate(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO items VALUES (1, 'a')")
	mustExec(t, db, "INSERT INTO items VALUES (2, 'b')")

	// COUNT(*)=2 is not > 5, so the (single) group is filtered out → 0 rows.
	rows, err := db.Query(ctx, "SELECT name FROM items HAVING COUNT(*) > 5")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if n != 0 {
		t.Fatalf("HAVING COUNT(*) > 5 returned %d rows, want 0 (HAVING ignored)", n)
	}

	// And it passes when the condition holds.
	var cnt int64
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM items HAVING COUNT(*) > 1").Scan(&cnt); err != nil {
		t.Fatalf("control query: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("HAVING COUNT(*) > 1 control: got %d, want 2", cnt)
	}
}

// TestVacuumPersistsRootAcrossReopen verifies VACUUM persists the new tree root,
// so a disk reopen does not resurrect soft-deleted rows or lose post-vacuum
// writes.
func TestVacuumPersistsRootAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "vacuum.db")
	ctx := context.Background()

	db, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 1; i <= 5; i++ {
		mustExec(t, db, "INSERT INTO t (id, v) VALUES ("+itoa(i)+", 'x')")
	}
	mustExec(t, db, "DELETE FROM t WHERE id = 2")
	mustExec(t, db, "DELETE FROM t WHERE id = 3")
	mustExec(t, db, "VACUUM")
	// A write AFTER vacuum must survive the reopen too.
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (6, 'x')")
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(dbPath, &Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	got := pkRollbackQueryIDs(t, db2, ctx, "SELECT id FROM t ORDER BY id")
	want := []int64{1, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("after vacuum+reopen, ids = %v, want %v (resurrected deletes or lost writes)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("after vacuum+reopen, ids = %v, want %v", got, want)
		}
	}
}
