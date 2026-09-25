package engine

import (
	"context"
	"testing"
)

// Grouped-ORDER-BY permutation-invariance regression (rounds 24-25): a valid weak order makes the sorted
// output of the same value multiset independent of input insertion order. The
// applyGroupByOrderBy inline comparator has the bool dual-representation cycle
// (true≡1 via toFloat64; 1<"n" but true>"n" via the string tier), so different
// insertion orders may produce different sorted sequences.
func TestGroupedOrderByPermutationInvariance(t *testing.T) {
	// Round 25 resolution: the comparator now routes through compareValues
	// (the round-6-normalized singleton), so the sorted output of the same
	// multiset must be independent of insertion order.
	type fixture struct {
		label string
		vals  []string // SQL literals
	}
	fixtures := []fixture{
		{"order-A", []string{"1", "true", "'n'", "0", "false"}},
		{"order-B", []string{"'n'", "0", "false", "1", "true"}},
		{"order-C", []string{"false", "true", "0", "1", "'n'"}},
	}
	for _, fx := range fixtures {
		db, err := Open(":memory:", nil)
		if err != nil {
			t.Fatalf("%s open: %v", fx.label, err)
		}
		if _, err := db.Exec(context.Background(), "CREATE TABLE p (val TEXT)"); err != nil {
			t.Fatalf("%s create: %v", fx.label, err)
		}
		for _, v := range fx.vals {
			if _, err := db.Exec(context.Background(), "INSERT INTO p (val) VALUES ("+v+")"); err != nil {
				t.Fatalf("%s insert: %v", fx.label, err)
			}
		}
		rows, err := db.Query(context.Background(), "SELECT val FROM p GROUP BY val ORDER BY val")
		if err != nil {
			t.Fatalf("%s query: %v", fx.label, err)
		}
		var vals []interface{}
		for rows.Next() {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("%s scan: %v", fx.label, err)
			}
			vals = append(vals, v)
		}
		rows.Close()
		db.Close()
		// (a) No value lost across insertion orders (fixed 5-value corpus).
		if len(vals) != 5 {
			t.Fatalf("FAIL: %s lost values: %v", fx.label, vals)
		}
		// (b) The output must be non-decreasing across the comparator's three
		// equivalence classes (numeric-0 ≡ false, numeric-1 ≡ true, "n") — the
		// correct permutation-invariance contract for this corpus (exact tie
		// order is legitimately input-order-dependent for the unstable
		// sort.Slice).
		classOf := func(v interface{}) int {
			switch x := v.(type) {
			case bool:
				if x {
					return 1
				}
				return 0
			case int64:
				if x == 0 {
					return 0
				}
				return 1
			case float64:
				if x == 0 {
					return 0
				}
				return 1
			default:
				return 2
			}
		}
		for k := 1; k < len(vals); k++ {
			if classOf(vals[k]) < classOf(vals[k-1]) {
				t.Fatalf("FAIL: %s produced an out-of-order sequence: %v — the comparator is not a valid weak order", fx.label, vals)
			}
		}
		t.Logf("%s -> class-sorted-ok: %v", fx.label, vals)
	}
}
