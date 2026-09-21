package engine

import (
	"testing"
)

// Regression for the composite-PK float truncation: formatKeyComponent builds
// composite primary-key components with formatKey(int64(v)) for float64
// values, truncating fractional floats. A composite PRIMARY KEY containing a
// FLOAT column therefore maps DISTINCT rows to the SAME B-tree key — the exact
// bug formatFloatKey fixed for single-column float PKs (its comment records
// the truncation as having caused "spurious UNIQUE failures / silent
// overwrites"). (1, 1.5) and (1, 1.8) must remain distinct rows.
func TestCompositePKFractionalFloatRowsAreDistinct(t *testing.T) {
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (a INTEGER, b FLOAT, PRIMARY KEY (a, b))")
	mustExec(t, db, "INSERT INTO t VALUES (1, 1.5)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 1.8)")

	rows := queryRows(t, db, "SELECT b FROM t WHERE a = 1 ORDER BY b")
	if len(rows) != 2 {
		t.Fatalf("distinct composite-PK float rows collapsed: got %d rows (%v), want exactly 2", len(rows), rows)
	}
	if got := scalar(t, db, "SELECT b FROM t WHERE a = 1 AND b = 1.8"); got != "1.8" {
		t.Fatalf("point lookup for fractional float PK component returned %q, want 1.8", got)
	}

	// Control: the single-column float PK path (formatFloatKey) was already
	// fixed to keep fractional values distinct; it must stay that way.
	mustExec(t, db, "CREATE TABLE f (id FLOAT PRIMARY KEY, tag TEXT)")
	mustExec(t, db, "INSERT INTO f VALUES (1.5, 'x')")
	mustExec(t, db, "INSERT INTO f VALUES (1.8, 'y')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM f"); got != "2" {
		t.Fatalf("single-column float PK control collapsed: count = %s, want 2", got)
	}
}
