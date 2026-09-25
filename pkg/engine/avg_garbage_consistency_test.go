package engine

import (
	"context"
	"testing"
)

// TestAvgGarbageStringConsistency pins the engine's documented AVG semantics
// for non-numeric strings: values that fail numeric conversion are skipped
// from both sum and count (the fail-closed skip — AVG equals SUM divided by
// the number of successfully coerced values), and every execution path — the
// byte fast path, the WHERE-forced decode branch, and the GROUP BY
// accumulator route — agrees. MySQL's 0-coercion-with-warning divisor is a
// known, deliberate divergence.
func TestAvgGarbageStringConsistency(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE gavg (t TEXT)`)
	mustExec(t, db, `INSERT INTO gavg VALUES ('abc'), ('2'), ('xyz')`)

	asFloat := func(v interface{}) (float64, bool, bool) {
		if v == nil {
			return 0, false, true
		}
		switch n := v.(type) {
		case float64:
			return n, true, false
		case int64:
			return float64(n), true, false
		case int:
			return float64(n), true, false
		}
		return 0, false, false
	}
	avg := func(desc, sql string) float64 {
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
		f, ok, isNil := asFloat(got)
		if isNil {
			t.Fatalf("FAIL: %s: AVG returned NULL, want 2 (the coerced-value average)", desc)
		}
		if !ok {
			t.Fatalf("FAIL: %s: AVG = %T(%v), want numeric 2", desc, got, got)
		}
		return f
	}

	// All three execution paths must agree: the non-numeric strings are
	// skipped, the numeric string coerces — AVG = 2/1 = 2.
	fa := avg("byte fast path", "SELECT AVG(t) FROM gavg")
	fb := avg("decode branch", "SELECT AVG(t) FROM gavg WHERE 1=1")
	fc := avg("group-by route", "SELECT AVG(t) FROM gavg GROUP BY (1=1)")
	if fa != 2 || fb != 2 || fc != 2 {
		t.Fatalf("FAIL: paths diverge on garbage strings: byte=%v decode=%v group-by=%v, want 2 on all", fa, fb, fc)
	}

	// The all-garbage set → NULL (no coerced values at all).
	rows, err := db.Query(ctx, "SELECT AVG(t) FROM gavg WHERE t IN ('abc','xyz')")
	if err != nil {
		t.Fatalf("FAIL: all-garbage query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("FAIL: all-garbage: no rows")
	}
	var got interface{}
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("FAIL: all-garbage scan: %v", err)
	}
	if got != nil {
		t.Fatalf("FAIL: all-garbage AVG = %T(%v), want NULL (no coercible values)", got, got)
	}
}
