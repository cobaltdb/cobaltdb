package engine

import (
	"context"
	"testing"
)

// TestGroupByOrderByDirectionCorrectness pins the direction contract of the
// grouped-ORDER-BY path (catalog_aggregate.go applyGroupByOrderBy): for the
// mixed-type key corpus, ORDER BY k ASC must emit the keys in the
// comparator's documented ascending order (0 < 1 ≡ true < "false" < "n") and
// ORDER BY k DESC in the descending order. Tied keys (int64 1 and bool true
// compare equal) may appear in either relative order, so each direction
// accepts both permutations of the tied pair.
//
// Recorded observation (SAGE, not fixed — unprovable observable): the
// comparator carries a latent transitivity violation (0 ≡ false ≡ "false"
// yet 0 < "false" — the round-3/6 dual-representation shape via
// toFloat64's bool acceptance); it does not manifest in this corpus's
// sorted output because the tied element masks it, and no adversarial
// permutation through the public API produced a violating order.
func TestGroupByOrderByDirectionCorrectness(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE g (v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// v branches map to the key corpus: 0→1, 1→"n", 2→true, 3→0, 4→"false"
	for _, v := range []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4} {
		if _, err := db.Exec(ctx, "INSERT INTO g (v) VALUES (?)", v); err != nil {
			t.Fatalf("insert v=%d: %v", v, err)
		}
	}

	keySeq := func(desc bool) []interface{} {
		dir := "ASC"
		if desc {
			dir = "DESC"
		}
		q := "SELECT k, COUNT(*) FROM (SELECT CASE WHEN v = 0 THEN 1 WHEN v = 1 THEN 'n' WHEN v = 2 THEN true WHEN v = 3 THEN 0 ELSE 'false' END AS k FROM g) sub GROUP BY k ORDER BY k " + dir
		rows, err := db.Query(ctx, q)
		if err != nil {
			t.Fatalf("query %s: %v", dir, err)
		}
		defer func() { _ = rows.Close() }()
		var keys []interface{}
		for rows.Next() {
			var k, cnt interface{}
			if err := rows.Scan(&k, &cnt); err != nil {
				t.Fatalf("scan: %v", err)
			}
			keys = append(keys, k)
		}
		if len(keys) != 5 {
			t.Fatalf("%s: got %d groups (%v), want 5", dir, len(keys), keys)
		}
		return keys
	}

	// tiedPair reports whether x and y are {int64(1), true} in either order.
	tiedPair := func(x, y interface{}) bool {
		return (x == int64(1) && y == true) || (x == true && y == int64(1))
	}

	asc := keySeq(false)
	if asc[0] != int64(0) {
		t.Fatalf("ASC: first key %v, want 0 (seq %v)", asc[0], asc)
	}
	if !tiedPair(asc[1], asc[2]) {
		t.Fatalf("ASC: middle pair (%v, %v) is not the tied {1, true} (seq %v)", asc[1], asc[2], asc)
	}
	if asc[3] != "false" || asc[4] != "n" {
		t.Fatalf("ASC: tail keys %v, %v — want \"false\", \"n\" (seq %v)", asc[3], asc[4], asc)
	}

	desc := keySeq(true)
	if desc[0] != "n" || desc[1] != "false" {
		t.Fatalf("DESC: head keys %v, %v — want \"n\", \"false\" (seq %v)", desc[0], desc[1], desc)
	}
	if !tiedPair(desc[2], desc[3]) {
		t.Fatalf("DESC: middle pair (%v, %v) is not the tied {1, true} (seq %v)", desc[2], desc[3], desc)
	}
	if desc[4] != int64(0) {
		t.Fatalf("DESC: last key %v, want 0 (seq %v)", desc[4], desc)
	}
}
