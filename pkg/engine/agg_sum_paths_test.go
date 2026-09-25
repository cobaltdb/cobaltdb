package engine

import (
	"context"
	"testing"
)

// TestAggregateSumPathConsistency pins the cross-path SUM contract: the same
// stored integers must produce the same SUM value regardless of which query
// shape reaches the aggregation. The no-GROUP-BY form takes the aggregate
// fast path (catalog_fastpath.go), whose float64 accumulation loses integer
// precision above 2^53; the GROUP BY form takes buildGroupByGroups, which
// accumulates through sumAccumulator (int64-exact while integral). A query
// must not change its answer based on its shape.
func TestAggregateSumPathConsistency(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	mustExec := func(q string) {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	mustExec("CREATE TABLE agg_ab (v BIGINT)")
	// 9007199254740993 = 2^53+1: the first integer float64 cannot represent.
	mustExec("INSERT INTO agg_ab (v) VALUES (9007199254740993)")
	mustExec("INSERT INTO agg_ab (v) VALUES (1)")
	mustExec("INSERT INTO agg_ab (v) VALUES (2)")

	const want = int64(9007199254740996)

	// Path A: no GROUP BY -> the aggregate fast path.
	rowsA, err := db.Query(ctx, "SELECT SUM(v) FROM agg_ab")
	if err != nil {
		t.Fatalf("fast-path SUM failed: %v", err)
	}
	defer rowsA.Close()
	if !rowsA.Next() {
		t.Fatal("fast-path SUM returned no row")
	}
	var gotA interface{}
	if err := rowsA.Scan(&gotA); err != nil {
		t.Fatalf("scan A: %v", err)
	}
	rowsA.Close()

	// Path B: GROUP BY on a constant -> buildGroupByGroups + sumAccumulator.
	rowsB, err := db.Query(ctx, "SELECT 1 AS g, SUM(v) FROM agg_ab GROUP BY g")
	if err != nil {
		t.Fatalf("grouped SUM failed: %v", err)
	}
	defer rowsB.Close()
	if !rowsB.Next() {
		t.Fatal("grouped SUM returned no row")
	}
	var gb interface{}
	var gotB interface{}
	if err := rowsB.Scan(&gb, &gotB); err != nil {
		t.Fatalf("scan B: %v", err)
	}
	rowsB.Close()

	asInt64 := func(v interface{}) (int64, bool) {
		switch n := v.(type) {
		case int64:
			return n, true
		case int:
			return int64(n), true
		case float64:
			if n == float64(int64(n)) {
				return int64(n), true
			}
			return 0, false
		}
		return 0, false
	}

	gotAInt, aOK := asInt64(gotA)
	gotBInt, bOK := asInt64(gotB)

	if !aOK || gotAInt != want {
		t.Fatalf("FAIL: fast-path SUM(v) = %v (%T), want %d — the aggregate fast path loses integer precision above 2^53", gotA, gotA, want)
	}
	if !bOK || gotBInt != want {
		t.Fatalf("FAIL: grouped SUM(v) = %v (%T), want %d", gotB, gotB, want)
	}
	if gotAInt != gotBInt {
		t.Fatalf("FAIL: SUM(v) differs by query shape: fast path %d vs grouped %d — same data must give the same sum", gotAInt, gotBInt)
	}
}
