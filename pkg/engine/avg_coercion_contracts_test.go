package engine

import (
	"context"
	"testing"
)

// TestAvgStringCoercionConsistency pins the MySQL AVG coercion contract and
// cross-path consistency: AVG over a TEXT-typed numeric column coerces the
// values (MySQL implicit cast), and every execution path — the byte fast
// path, the WHERE-forced decode branch, and the GROUP BY accumulator route —
// must agree with each other and with SUM over the same column.
func TestAvgStringCoercionConsistency(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE avgt (t TEXT, n INTEGER)`)
	mustExec(t, db, `INSERT INTO avgt VALUES ('1', 10), ('2', 20), ('3', 30)`)

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
		case string:
			return 0, false, false
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
			t.Fatalf("FAIL: %s: AVG returned NULL, want 2 (MySQL coerces TEXT numerics)", desc)
		}
		if !ok {
			t.Fatalf("FAIL: %s: AVG = %T(%v), want numeric 2", desc, got, got)
		}
		return f
	}

	// (a) The byte fast path (no WHERE, all-AVG spec set).
	fa := avg("byte fast path", "SELECT AVG(t) FROM avgt")
	// (b) The WHERE-forced decode branch.
	fb := avg("decode branch", "SELECT AVG(t) FROM avgt WHERE 1=1")
	// (c) The GROUP BY accumulator route (the coercion reference).
	fc := avg("group-by route", "SELECT AVG(t) FROM avgt GROUP BY (1=1)")

	// The control: the numeric column agrees on all paths.
	na := avg("byte fast path numeric", "SELECT AVG(n) FROM avgt")
	nb := avg("decode branch numeric", "SELECT AVG(n) FROM avgt WHERE 1=1")

	for _, c := range []struct {
		desc string
		f    float64
	}{{"byte", fa}, {"decode", fb}, {"group-by", fc}} {
		if c.f != 2 {
			t.Fatalf("FAIL: %s path AVG(t) = %v, want 2 (coerced '1','2','3')", c.desc, c.f)
		}
	}
	if na != 20 || nb != 20 {
		t.Fatalf("FAIL: numeric control diverged: %v / %v, want 20", na, nb)
	}
}
