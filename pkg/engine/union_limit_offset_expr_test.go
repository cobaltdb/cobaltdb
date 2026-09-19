package engine

import (
	"context"
	"testing"
)

// TestUnionLimitOffsetExpressions pins full-expression LIMIT/OFFSET on set
// operations. parseSetOp moves the right SELECT's tail clauses onto the
// UnionStmt, and parseSelect parses them as full expressions — plain
// `SELECT ... LIMIT ?` works on every other engine path (scan, aggregates,
// JOIN+GROUP BY all evaluate via evaluateExpression). executeUnion applied
// them only for *query.NumberLiteral, silently dropping `LIMIT ?`/`OFFSET ?`
// on set operations while the placeholder-count check still required the
// argument: a caller asking for LIMIT 1 silently received every row.
func TestUnionLimitOffsetExpressions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")
	mustExec(t, db, "INSERT INTO t VALUES (2)")
	mustExec(t, db, "INSERT INTO t VALUES (3)")
	mustExec(t, db, "INSERT INTO t VALUES (4)")
	mustExec(t, db, "INSERT INTO t VALUES (5)")

	const unionSQL = "SELECT id FROM t UNION ALL SELECT id+10 FROM t ORDER BY id"

	count := func(sql string, args ...interface{}) int {
		t.Helper()
		rows, err := db.Query(ctx, sql, args...)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		return n
	}

	// Literal clauses (pre-fix behavior — must stay working).
	if n := count(unionSQL + " LIMIT 2"); n != 2 {
		t.Fatalf("literal LIMIT: got %d rows, want 2", n)
	}
	if n := count(unionSQL + " LIMIT 2 OFFSET 3"); n != 2 {
		t.Fatalf("literal LIMIT+OFFSET: got %d rows, want 2", n)
	}

	// Plain-select placeholder control (works on the select path).
	if n := count("SELECT id FROM t LIMIT ?", 2); n != 2 {
		t.Fatalf("plain-select LIMIT ?: got %d rows, want 2", n)
	}

	// The defect: placeholder clauses on a set operation.
	if n := count(unionSQL+" LIMIT ?", 2); n != 2 {
		t.Fatalf("union LIMIT ? ignored: got %d rows, want 2", n)
	}
	if n := count(unionSQL+" OFFSET ?", 3); n != 7 {
		t.Fatalf("union OFFSET ? ignored: got %d rows, want 7", n)
	}
	if n := count(unionSQL+" LIMIT ? OFFSET ?", 2, 3); n != 2 {
		t.Fatalf("union LIMIT ?+OFFSET ? ignored: got %d rows, want 2", n)
	}
}
