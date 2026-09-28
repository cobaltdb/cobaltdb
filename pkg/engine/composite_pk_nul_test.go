package engine

import (
	"context"
	"testing"
)

// Composite primary-key keys join per-column parts with a NUL separator
// (catalog_insert.go buildCompositePK). String values can themselves contain
// NUL bytes (bound parameters, \0 escapes in literals), so parts are
// NUL-escaped before the join — otherwise two DISTINCT pk tuples collide on
// one B-tree row key: ("p\x00S:q","r") and ("p","q\x00S:r") would both join
// to "S:p\x00S:q\x00S:r". A colliding second insert was falsely rejected as
// "UNIQUE constraint failed: duplicate primary key value", and REPLACE of
// one tuple evicted the other tuple's row. Single-column keys carry no
// separator and stay byte-identical (pinned by the control below).

func cpkNulOpen(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func cpkNulQuery1(t *testing.T, db *DB, sql string, args ...interface{}) interface{} {
	t.Helper()
	rows, err := db.Query(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("query %q %v: %v", sql, args, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("query %q %v: no rows", sql, args)
	}
	var v interface{}
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan %q: %v", sql, err)
	}
	return v
}

// TestCompositePKNULControls pins the unaffected paths: NUL-free composite
// PKs store distinct rows, genuine duplicates are still rejected, and a
// single-column TEXT PK holding a NUL value keeps working byte-identically.
func TestCompositePKNULControls(t *testing.T) {
	db := cpkNulOpen(t)
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE cpkc (a TEXT, b TEXT, v TEXT, PRIMARY KEY (a, b))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range [][3]string{{"p", "r", "one"}, {"p", "s", "two"}} {
		if _, err := db.Exec(ctx, "INSERT INTO cpkc (a, b, v) VALUES (?, ?, ?)", r[0], r[1], r[2]); err != nil {
			t.Fatalf("insert control %v: %v", r, err)
		}
	}
	if n := cpkNulQuery1(t, db, "SELECT COUNT(*) FROM cpkc"); n != int64(2) {
		t.Fatalf("control count = %v, want 2", n)
	}
	if _, err := db.Exec(ctx, "INSERT INTO cpkc (a, b, v) VALUES (?, ?, ?)", "p", "r", "dup"); err == nil {
		t.Fatalf("control: duplicate composite PK unexpectedly accepted")
	}

	if _, err := db.Exec(ctx, "CREATE TABLE scpk (a TEXT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create scpk: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO scpk (a, v) VALUES (?, ?)", "x\x00y", "val"); err != nil {
		t.Fatalf("insert single-col NUL PK: %v", err)
	}
	if got := cpkNulQuery1(t, db, "SELECT v FROM scpk WHERE a = ?", "x\x00y"); got != "val" {
		t.Fatalf("single-col NUL PK lookup v = %v, want val", got)
	}
}

// TestCompositePKNULValuesDistinctKeys pins the collision fix: both DISTINCT
// tuples must be stored and retrievable by their own values.
func TestCompositePKNULValuesDistinctKeys(t *testing.T) {
	db := cpkNulOpen(t)
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE cpk (a TEXT, b TEXT, v TEXT, PRIMARY KEY (a, b))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO cpk (a, b, v) VALUES (?, ?, ?)", "p\x00S:q", "r", "one"); err != nil {
		t.Fatalf("insert row1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO cpk (a, b, v) VALUES (?, ?, ?)", "p", "q\x00S:r", "two"); err != nil {
		t.Fatalf("FAIL: insert of the DISTINCT tuple ('p','q\\0S:r') rejected: %v — buildCompositePK joined unescaped \\x00 parts so this tuple collided with ('p\\0S:q','r') on the B-tree row key", err)
	}
	if n := cpkNulQuery1(t, db, "SELECT COUNT(*) FROM cpk"); n != int64(2) {
		t.Fatalf("FAIL: COUNT(*) = %v, want 2 (both distinct PK tuples stored)", n)
	}
	if got := cpkNulQuery1(t, db, "SELECT v FROM cpk WHERE a = ? AND b = ?", "p\x00S:q", "r"); got != "one" {
		t.Fatalf("row1 v = %v, want one", got)
	}
	if got := cpkNulQuery1(t, db, "SELECT v FROM cpk WHERE a = ? AND b = ?", "p", "q\x00S:r"); got != "two" {
		t.Fatalf("row2 v = %v, want two", got)
	}
}

// TestCompositePKNULReplaceKeepsOtherRow pins the second face of the same
// collision: REPLACE of one tuple must never evict the other tuple's row.
func TestCompositePKNULReplaceKeepsOtherRow(t *testing.T) {
	db := cpkNulOpen(t)
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE cpr (a TEXT, b TEXT, v TEXT, PRIMARY KEY (a, b))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO cpr (a, b, v) VALUES (?, ?, ?)", "p\x00S:q", "r", "one"); err != nil {
		t.Fatalf("insert row1: %v", err)
	}
	if _, err := db.Exec(ctx, "REPLACE INTO cpr (a, b, v) VALUES (?, ?, ?)", "p", "q\x00S:r", "two-repl"); err != nil {
		t.Fatalf("REPLACE row2: %v", err)
	}
	if got := cpkNulQuery1(t, db, "SELECT v FROM cpr WHERE a = ? AND b = ?", "p\x00S:q", "r"); got != "one" {
		t.Fatalf("FAIL: REPLACE of tuple ('p','q\\0S:r') evicted the DISTINCT row ('p\\0S:q','r') — v = %v, want one", got)
	}
	if got := cpkNulQuery1(t, db, "SELECT v FROM cpr WHERE a = ? AND b = ?", "p", "q\x00S:r"); got != "two-repl" {
		t.Fatalf("row2 v = %v, want two-repl", got)
	}
	if n := cpkNulQuery1(t, db, "SELECT COUNT(*) FROM cpr"); n != int64(2) {
		t.Fatalf("FAIL: COUNT(*) = %v, want 2", n)
	}
}
