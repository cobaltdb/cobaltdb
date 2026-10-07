package engine

import (
	"context"
	"testing"
)

// TestAggregateFilterPropagatesPredicateError pins that an aggregate
// FILTER (WHERE ...) predicate which fails to evaluate raises an error instead
// of being silently swallowed as "no rows matched".
//
// Contract basis: the engine already propagates predicate-evaluation errors
// from every other clause position — WHERE and HAVING both return
// "column not found: <col>" for an unresolvable column. FILTER is evaluated by
// the same expression evaluator over the same rows, so it must behave
// identically. Previously filterAggregateRows did `if err == nil && ok`, which
// silently dropped every row and made the query return a plausible but wrong
// aggregate (SUM -> NULL, COUNT -> 0) with a nil error.
func TestAggregateFilterPropagatesPredicateError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	for _, s := range []string{
		"CREATE TABLE agg_filter_err (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)",
		"INSERT INTO agg_filter_err VALUES (1,'a',10),(2,'a',20),(3,'b',30)",
	} {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}

	scalar := func(t *testing.T, q string) string {
		t.Helper()
		rows, err := db.Query(ctx, q)
		if err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		defer rows.Close()
		var got string
		n := 0
		for rows.Next() {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan %q: %v", q, err)
			}
			got = toStrEng(v)
			n++
		}
		if n != 1 {
			t.Fatalf("query %q: got %d rows, want 1", q, n)
		}
		return got
	}

	// CONTROL 1: the identical unresolvable column DOES error in WHERE and
	// HAVING. This is the established contract FILTER is now held to; if this
	// ever stops erroring the premise of the test changes.
	for _, q := range []string{
		"SELECT v FROM agg_filter_err WHERE nonexistent_col > 0",
		"SELECT SUM(v) FROM agg_filter_err HAVING nonexistent_col > 0",
	} {
		rows, qerr := db.Query(ctx, q)
		if rows != nil {
			rows.Close()
		}
		if qerr == nil {
			t.Fatalf("control broken: %q must error for an unresolvable column", q)
		}
	}

	// CONTROL 2: valid FILTER predicates still produce the correct aggregate.
	if got := scalar(t, "SELECT SUM(v) FILTER (WHERE v > 15) FROM agg_filter_err"); got != "50" {
		t.Errorf("control: SUM FILTER v>15 = %q, want %q", got, "50")
	}
	if got := scalar(t, "SELECT COUNT(*) FILTER (WHERE g = 'a') FROM agg_filter_err"); got != "2" {
		t.Errorf("control: COUNT(*) FILTER g='a' = %q, want %q", got, "2")
	}

	// THE REGRESSION: an unresolvable column inside FILTER must surface an
	// error rather than silently evaluating to "no rows matched".
	for _, q := range []string{
		"SELECT SUM(v) FILTER (WHERE nonexistent_col > 0) FROM agg_filter_err",
		"SELECT COUNT(*) FILTER (WHERE nonexistent_col > 0) FROM agg_filter_err",
		"SELECT MIN(v) FILTER (WHERE nonexistent_col > 0) FROM agg_filter_err",
		// Secondary branch: the grouped path (computeGroupResultRows) and the
		// ORDER BY / embedded-aggregate path (computeAggregatesForExpr) reach
		// the same filterAggregateRows helper.
		"SELECT g, SUM(v) FILTER (WHERE nonexistent_col > 0) FROM agg_filter_err GROUP BY g",
		"SELECT SUM(v) FILTER (WHERE nonexistent_col > 0) FROM agg_filter_err ORDER BY 1",
	} {
		rows, qerr := db.Query(ctx, q)
		if rows != nil {
			rows.Close()
		}
		if qerr == nil {
			t.Errorf("FAIL: %s returned no error; an unresolvable column in FILTER "+
				"must be reported exactly as it is in WHERE/HAVING", q)
		}
	}
}
