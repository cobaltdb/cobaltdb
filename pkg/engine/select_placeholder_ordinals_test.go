package engine

import (
	"context"
	"testing"
)

// Regression for the placeholder-ordinal class: parsePrimary created
// &PlaceholderExpr{} with Index=0 and no assignment, and each clause patched
// its own placeholders with a local reindex (WHERE to 0, SET to 0, ...).
// Any placeholder outside the patched clause silently bound args[0]:
//
//   - SELECT list vs WHERE:  SELECT id, ? FROM t WHERE id = ?  -> both args[0]
//   - ON CONFLICT SET vs VALUES: INSERT ... VALUES (?, ...) ON CONFLICT
//     DO UPDATE SET v = ?            -> SET bound args[0]
//
// The fix assigns ordinals at creation from the parser's placeholderCount
// (appearance order == wire order) and removes the per-clause patching.
func TestPlaceholderOrdinalsAcrossClauses(t *testing.T) {
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	execArgs(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	execArgs(t, db, `INSERT INTO t VALUES (7, 'seven')`)

	// RED-1: select-list placeholder must bind its own arg, not the WHERE's.
	row, col := queryRowValues(t, db, "SELECT id, ? FROM t WHERE id = ?", 42, 7)
	if row != 1 {
		t.Fatalf("select-list ordinal: expected exactly 1 row (id=7 with bound 42), got %d row(s)", row)
	}
	if col != 42 {
		t.Fatalf("select-list ordinal: second column = %v, want 42 (both placeholders bound args[0])", col)
	}

	// Control: WHERE-only placeholder keeps working.
	if got := scalarArgs(t, db, "SELECT v FROM t WHERE id = ?", 7); got != "seven" {
		t.Fatalf("WHERE-only control: v = %v, want seven", got)
	}

	// RED-2: ON CONFLICT DO UPDATE SET placeholder must not stomp VALUES ordinals.
	execArgs(t, db, `CREATE TABLE t2 (id INTEGER PRIMARY KEY, v TEXT)`)
	execArgs(t, db, `INSERT INTO t2 VALUES (1, 'orig')`)
	execArgs(t, db, `INSERT INTO t2 (id, v) VALUES (?, 'updated') ON CONFLICT DO UPDATE SET v = ?`, 1, "upd")
	if got := scalarArgs(t, db, "SELECT v FROM t2 WHERE id = 1"); got != "upd" {
		t.Fatalf("ON CONFLICT SET ordinal: v = %v, want upd (SET placeholder bound args[0])", got)
	}

	// Control: round-7 multi-row VALUES ordinals still sequential.
	execArgs(t, db, `INSERT INTO t2 VALUES (3, ?), (4, ?)`, "three", "four")
	if got := scalarArgs(t, db, "SELECT v FROM t2 WHERE id = 3"); got != "three" {
		t.Fatalf("multi-row VALUES control row 3: v = %v, want three", got)
	}
	if got := scalarArgs(t, db, "SELECT v FROM t2 WHERE id = 4"); got != "four" {
		t.Fatalf("multi-row VALUES control row 4: v = %v, want four", got)
	}
}

func execArgs(t *testing.T, db *DB, sql string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func scalarArgs(t *testing.T, db *DB, sql string, args ...interface{}) string {
	t.Helper()
	var s string
	if err := db.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		t.Fatalf("scalar %q: %v", sql, err)
	}
	return s
}

// queryRowValues runs a query expected to return exactly one row with two
// columns (int, interface{}) and returns (rowCount, firstRowCol2).
func queryRowValues(t *testing.T, db *DB, sql string, args ...interface{}) (int, interface{}) {
	t.Helper()
	rows, err := db.Query(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	var col2 interface{}
	for rows.Next() {
		n++
		var id int
		if err := rows.Scan(&id, &col2); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	return n, col2
}
