package catalog_test

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestGroupByNULValuesDistinctGroups pins the contract that GROUP BY keeps
// distinct value-tuples in distinct groups even when the values contain NUL
// bytes.
//
// Regression: typeTaggedKey embedded NUL bytes verbatim in its "S:" parts, and
// both the GROUP BY group key (catalog_aggregate.go joins parts with a bare
// "\x00") and composite secondary-index keys (buildCompositeIndexKey, same
// join) inherited them — so rows ("p\x00S:q", "r") and ("p", "q\x00S:r")
// produced the identical key "S:p\x00S:q\x00S:r" and GROUP BY merged them into
// one group (COUNT=2 instead of two groups with COUNT=1).
//
// Fix: escapeNULs in typeTaggedKey injectively maps \x00 -> \x00\x01 and is a
// no-op for NUL-free keys, so previously persisted keys are unaffected.
func TestGroupByNULValuesDistinctGroups(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (a TEXT, b TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// The two colliding rows (values contain NUL bytes).
	rows := [][2]string{
		{"p\x00S:q", "r"},
		{"p", "q\x00S:r"},
	}
	for _, r := range rows {
		if _, err := db.Exec(ctx, "INSERT INTO t (a, b) VALUES (?, ?)", r[0], r[1]); err != nil {
			t.Fatalf("insert (%q, %q): %v", r[0], r[1], err)
		}
	}

	result, err := db.Query(ctx, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b")
	if err != nil {
		t.Fatalf("group-by query: %v", err)
	}
	type group struct {
		a, b string
		n    int
	}
	var got []group
	for result.Next() {
		var g group
		if err := result.Scan(&g.a, &g.b, &g.n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, g)
	}
	if err := result.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("GROUP BY returned %d groups, want 2 (got %+v)", len(got), got)
	}
	for _, g := range got {
		if g.n != 1 {
			t.Fatalf("group (a=%q b=%q) has COUNT=%d, want 1 — distinct tuples merged", g.a, g.b, g.n)
		}
	}

	// Control: NUL-free grouping stays exact.
	if _, err := db.Exec(ctx, "INSERT INTO t (a, b) VALUES (?, ?)", "x", "y"); err != nil {
		t.Fatalf("control insert: %v", err)
	}
	result, err = db.Query(ctx, "SELECT COUNT(*) FROM t WHERE a = ? AND b = ?", "x", "y")
	if err != nil {
		t.Fatalf("control query: %v", err)
	}
	var n int
	if !result.Next() {
		t.Fatal("control query returned no rows")
	}
	if err := result.Scan(&n); err != nil {
		t.Fatalf("control scan: %v", err)
	}
	if err := result.Close(); err != nil {
		t.Fatalf("control close: %v", err)
	}
	if n != 1 {
		t.Fatalf("control: NUL-free lookup returned %d rows, want 1", n)
	}
}
