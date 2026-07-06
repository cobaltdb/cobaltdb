package engine

import (
	"context"
	"testing"
)

// TestMultiColumnCountDistinct verifies COUNT(DISTINCT a, b, ...) counts
// distinct *tuples* over all argument expressions, excluding any row where
// one of the arguments is NULL. A prior bug evaluated only the first argument
// (aggregateArgs[0]) in collectAggregateInput, so the 2nd+ columns were
// silently ignored and the count collapsed to COUNT(DISTINCT a).
func TestMultiColumnCountDistinct(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, a INTEGER, b INTEGER)")
	// Distinct (a,b) pairs overall (non-NULL): (1,1),(1,2),(9,9) => 3.
	// Rows 4 and 5 each have a NULL argument and must be excluded.
	mustExec(t, db, "INSERT INTO t VALUES (1,1,1,1),(2,1,1,2),(3,1,1,1),(4,1,NULL,5),(5,1,2,NULL)")
	mustExec(t, db, "INSERT INTO t VALUES (6,2,9,9),(7,2,9,9)")

	scan := func(sql string) int64 {
		var v int64
		if err := db.QueryRow(ctx, sql).Scan(&v); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return v
	}

	if got := scan("SELECT COUNT(DISTINCT a, b) FROM t"); got != 3 {
		t.Errorf("COUNT(DISTINCT a, b) = %d, want 3", got)
	}
	// Single-column form must be unaffected.
	if got := scan("SELECT COUNT(DISTINCT a) FROM t"); got != 3 { // a in {1,2,9}
		t.Errorf("COUNT(DISTINCT a) = %d, want 3", got)
	}
	// Argument order does not change the distinct-tuple count.
	if got := scan("SELECT COUNT(DISTINCT b, a) FROM t"); got != 3 {
		t.Errorf("COUNT(DISTINCT b, a) = %d, want 3", got)
	}

	// Grouped: g=1 has distinct (a,b) {(1,1),(1,2)} = 2; g=2 has {(9,9)} = 1.
	rows := queryRows(t, db, "SELECT g, COUNT(DISTINCT a, b) FROM t GROUP BY g ORDER BY g")
	if len(rows) != 2 {
		t.Fatalf("grouped result has %d rows, want 2: %v", len(rows), rows)
	}
	if c := rows[0][1]; toInt64(c) != 2 {
		t.Errorf("g=1 COUNT(DISTINCT a, b) = %v, want 2", c)
	}
	if c := rows[1][1]; toInt64(c) != 1 {
		t.Errorf("g=2 COUNT(DISTINCT a, b) = %v, want 1", c)
	}
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return -1
	}
}
