package engine

import (
	"context"
	"strings"
	"testing"
)

// TestSelectStarPlusExprOverDerivedTable verifies `SELECT *, <expr> FROM (...)`
// does not panic (the projection indexed stmt.Columns by the mapping index,
// which overflows after a star expansion).
func TestSelectStarPlusExprOverDerivedTable(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (a INTEGER, b INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 2)")
	mustExec(t, db, "INSERT INTO t VALUES (3, 4)")

	rows, err := db.Query(ctx, "SELECT *, a+b AS s FROM (SELECT a, b FROM t) AS sub ORDER BY a")
	if err != nil {
		t.Fatalf("query (likely panicked): %v", err)
	}
	defer rows.Close()
	var sums []int64
	for rows.Next() {
		var a, b, s int64
		if err := rows.Scan(&a, &b, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if s != a+b {
			t.Fatalf("s=%d, want a+b=%d", s, a+b)
		}
		sums = append(sums, s)
	}
	if len(sums) != 2 || sums[0] != 3 || sums[1] != 7 {
		t.Fatalf("got sums %v, want [3 7]", sums)
	}
}

// TestFloatPrimaryKeyDistinct verifies distinct fractional float PK values do
// not collide (they were truncated to int64, making 1.2 and 1.8 the same key).
func TestFloatPrimaryKeyDistinct(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (k REAL PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1.2, 'a')")
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1.8, 'b')"); err != nil {
		t.Fatalf("second insert (distinct float PK) failed: %v", err)
	}
	var n int64
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 2 {
		t.Fatalf("COUNT = %d, want 2 (float PK collision lost a row)", n)
	}
	// Integer PK values still work (whole-number floats use the integer key).
	mustExec(t, db, "CREATE TABLE ti (k INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO ti VALUES (5, 'x')")
	mustExec(t, db, "INSERT INTO ti VALUES (6, 'y')")
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM ti").Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 2 {
		t.Fatalf("integer PK COUNT = %d, want 2", n)
	}
}

// TestInsertShortValuesRejected verifies a short VALUES list without an explicit
// column list is rejected (it used to misalign columns into the autoinc slot).
func TestInsertShortValuesRejected(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)")

	if _, err := db.Exec(ctx, "INSERT INTO t VALUES ('alice')"); err == nil {
		t.Fatal("short VALUES list without column list should be rejected")
	} else if !strings.Contains(err.Error(), "columns but") {
		t.Fatalf("unexpected error: %v", err)
	}

	// The standard form (explicit column list) still auto-fills the PK.
	mustExec(t, db, "INSERT INTO t (name) VALUES ('bob')")
	var id int64
	var name string
	if err := db.QueryRow(ctx, "SELECT id, name FROM t").Scan(&id, &name); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if name != "bob" || id < 1 {
		t.Fatalf("got id=%d name=%q, want id>=1 name=bob", id, name)
	}
}
