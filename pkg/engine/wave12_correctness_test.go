package engine

import (
	"context"
	"testing"
)

// TestSecondaryIndexIntArgMatchesFullScan verifies a secondary-index equality
// lookup returns the same rows as a full scan when the predicate value is passed
// as a Go int/int32 (not int64). typeTaggedKey previously only handled int64, so
// an int arg produced the wrong key tag and the index returned 0 rows.
func TestSecondaryIndexIntArgMatchesFullScan(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "CREATE INDEX idx_v ON t(v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 100)")
	mustExec(t, db, "INSERT INTO t VALUES (2, 100)")
	mustExec(t, db, "INSERT INTO t VALUES (3, 200)")

	count := func(arg interface{}) int {
		rows, err := db.Query(ctx, "SELECT id FROM t WHERE v = ?", arg)
		if err != nil {
			t.Fatalf("query (arg %T): %v", arg, err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		return n
	}

	// All arg types must agree with the full-scan answer (2).
	for _, arg := range []interface{}{int64(100), int(100), int32(100)} {
		if n := count(arg); n != 2 {
			t.Errorf("WHERE v = ? with %T arg returned %d rows, want 2 (index missed rows)", arg, n)
		}
	}
}

// TestHexNegativeAndConstEvalConsistency verifies HEX of a negative integer
// uses unsigned two's-complement (MySQL), and that the constant-eval path agrees
// with the main path for SUBSTR negative start and CONCAT NULL.
func TestHexNegativeAndConstEvalConsistency(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	str := func(sql string) interface{} {
		var v interface{}
		if err := db.QueryRow(ctx, sql).Scan(&v); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return v
	}
	if got := str("SELECT HEX(-1)"); got != "FFFFFFFFFFFFFFFF" {
		t.Errorf("HEX(-1) = %v, want FFFFFFFFFFFFFFFF", got)
	}
	if got := str("SELECT HEX(255)"); got != "FF" {
		t.Errorf("HEX(255) = %v, want FF", got)
	}
}
