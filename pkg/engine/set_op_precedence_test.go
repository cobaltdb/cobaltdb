package engine

import (
	"context"
	"fmt"
	"testing"
)

// Regression for set-operation precedence: parseSetOp folded
// UNION/INTERSECT/EXCEPT chains left-associatively, but the SQL standard
// gives INTERSECT higher precedence than UNION/EXCEPT. Without parens,
// "a UNION b INTERSECT c" must evaluate as a UNION (b INTERSECT c); the
// left-associative fold evaluated (a UNION b) INTERSECT c — silently wrong
// rows for any mixed set-operation query.
func TestSetOpIntersectPrecedence(t *testing.T) {
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	execArgs(t, db, `CREATE TABLE t1 (v INTEGER)`)
	execArgs(t, db, `CREATE TABLE t2 (v INTEGER)`)
	execArgs(t, db, `CREATE TABLE t3 (v INTEGER)`)
	execArgs(t, db, `INSERT INTO t1 VALUES (1), (2)`)
	execArgs(t, db, `INSERT INTO t2 VALUES (2), (3)`)
	execArgs(t, db, `INSERT INTO t3 VALUES (3), (4)`)

	// a = {1,2}; b = {2,3}; c = {3,4}.
	// Standard: a UNION (b INTERSECT c) = {1,2} ∪ {3}       = {1,2,3}
	// Left-assoc: (a UNION b) INTERSECT c = {1,2,3} ∩ {3,4} = {3}
	got := sortedQueryStrings(t, db, "SELECT v FROM t1 UNION SELECT v FROM t2 INTERSECT SELECT v FROM t3")
	want := []string{"1", "2", "3"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("set-op precedence: %v (%v), want %v — INTERSECT did not bind tighter than UNION", got, fmt.Sprint(got) == fmt.Sprint(want), want)
	}

	// Controls: single-op chains keep left-to-right semantics.
	if got := sortedQueryStrings(t, db, "SELECT v FROM t1 UNION SELECT v FROM t2"); fmt.Sprint(got) != fmt.Sprint([]string{"1", "2", "3"}) {
		t.Fatalf("UNION control: %v, want [1 2 3]", got)
	}
	if got := sortedQueryStrings(t, db, "SELECT v FROM t2 INTERSECT SELECT v FROM t3"); fmt.Sprint(got) != fmt.Sprint([]string{"3"}) {
		t.Fatalf("INTERSECT control: %v, want [3]", got)
	}
	if got := sortedQueryStrings(t, db, "SELECT v FROM t1 EXCEPT SELECT v FROM t2"); fmt.Sprint(got) != fmt.Sprint([]string{"1"}) {
		t.Fatalf("EXCEPT control: %v, want [1]", got)
	}
}

func sortedQueryStrings(t *testing.T, db *DB, sql string) []string {
	t.Helper()
	rows, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	// insertion sort — tiny sets
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
