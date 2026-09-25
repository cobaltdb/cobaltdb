package engine

import (
	"context"
	"testing"
)

// TestCountNullContracts pins the MySQL COUNT semantics through the real
// dispatch: COUNT(*) counts rows (all-NULL rows included); COUNT(col) skips
// NULLs and returns 0 — never NULL — for all-NULL and empty sets; and the
// fast path agrees with the GROUP BY route.
func TestCountNullContracts(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE cnt (v INTEGER, s TEXT)`)
	mustExec(t, db, `INSERT INTO cnt VALUES (1, 'a'), (2, NULL), (NULL, 'b'), (NULL, NULL)`)
	// v = [1, 2, NULL, NULL] → COUNT(v) = 2; s = ['a', NULL, 'b', NULL] → COUNT(s) = 2; COUNT(*) = 4.

	asFloat := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case int64:
			return float64(n), true
		case float64:
			return n, true
		case int:
			return float64(n), true
		}
		return 0, false
	}
	check := func(desc, sql string, want float64) {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("FAIL: %s: query: %v", desc, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("FAIL: %s: no rows", desc)
		}
		var got interface{}
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("FAIL: %s: scan: %v", desc, err)
		}
		g, ok := asFloat(got)
		if !ok {
			t.Fatalf("FAIL: %s: got %T(%v), want numeric %v", desc, got, got, want)
		}
		if g != want {
			t.Fatalf("FAIL: %s: got %v, want %v", desc, g, want)
		}
	}

	// COUNT(*) includes the all-NULL row.
	check("COUNT(*)", "SELECT COUNT(*) FROM cnt", 4)
	// COUNT(col) skips NULLs.
	check("COUNT(v)", "SELECT COUNT(v) FROM cnt", 2)
	check("COUNT(s)", "SELECT COUNT(s) FROM cnt", 2)
	// The all-NULL subset → 0, never NULL (the fast path with WHERE → the full-decode branch).
	check("COUNT(v) all-NULL subset", "SELECT COUNT(v) FROM cnt WHERE v IS NULL", 0)
	// The empty set → 0.
	check("COUNT(*) empty", "SELECT COUNT(*) FROM cnt WHERE 1=0", 0)
	check("COUNT(v) empty", "SELECT COUNT(v) FROM cnt WHERE 1=0", 0)

	// The cross-path consistency: the GROUP BY route (the reduce/accumulator path)
	// must agree with the fast path — including the all-NULL group → 0.
	rows, err := db.Query(ctx, "SELECT (v IS NULL) AS g, COUNT(v) FROM cnt GROUP BY (v IS NULL) ORDER BY g")
	if err != nil {
		t.Fatalf("FAIL: group-by query: %v", err)
	}
	defer rows.Close()
	type groupRow struct {
		g     interface{}
		count float64
	}
	var groups []groupRow
	for rows.Next() {
		var g interface{}
		var c interface{}
		if err := rows.Scan(&g, &c); err != nil {
			t.Fatalf("FAIL: group-by scan: %v", err)
		}
		f, ok := asFloat(c)
		if !ok {
			t.Fatalf("FAIL: group-by COUNT(v) = %T(%v), want numeric", c, c)
		}
		groups = append(groups, groupRow{g: g, count: f})
	}
	if len(groups) != 2 {
		t.Fatalf("FAIL: group-by groups = %d, want 2", len(groups))
	}
	// ORDER BY g: false group first (0 < 1), then the all-NULL group.
	if groups[0].count != 2 {
		t.Fatalf("FAIL: non-NULL group COUNT(v) = %v, want 2", groups[0].count)
	}
	if groups[1].count != 0 {
		t.Fatalf("FAIL: all-NULL group COUNT(v) = %v, want 0 (never NULL)", groups[1].count)
	}
}
