package engine

import (
	"context"
	"testing"
)

// CREATE TABLE IF NOT EXISTS ... AS SELECT (CTAS) on a table that already
// exists must be a full NO-OP — matching executeCreateTable's IfNotExists
// pre-check and MySQL (which warns without even evaluating the source
// SELECT). Regression: executeCreateTableAsSelect copied IfNotExists into
// the create statement (so CreateTable silently skipped) but then ran the
// insert loop unconditionally, APPENDING the materialized rows into the
// pre-existing table — silent data duplication (tgt grew from 2 to 4 rows).

func ctasIfNotExistsExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func ctasIfNotExistsCount(t *testing.T, db *DB, table string) int64 {
	t.Helper()
	rows, err := db.Query(context.Background(), "SELECT COUNT(*) FROM "+table)
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("count %s: no rows", table)
	}
	var n int64
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("scan count %s: %v", table, err)
	}
	return n
}

// TestCTASIfNotExistsValidShapes pins the unaffected CTAS shapes: plain CTAS
// into a fresh name, IF NOT EXISTS into a fresh name, and the plain
// existing-table error.
func TestCTASIfNotExistsValidShapes(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctasIfNotExistsExec(t, db, "CREATE TABLE src (id INTEGER)")
	ctasIfNotExistsExec(t, db, "INSERT INTO src VALUES (1)")
	ctasIfNotExistsExec(t, db, "INSERT INTO src VALUES (2)")

	ctasIfNotExistsExec(t, db, "CREATE TABLE fresh AS SELECT * FROM src")
	if n := ctasIfNotExistsCount(t, db, "fresh"); n != 2 {
		t.Fatalf("plain CTAS count = %d, want 2", n)
	}
	ctasIfNotExistsExec(t, db, "CREATE TABLE IF NOT EXISTS fresh2 AS SELECT * FROM src")
	if n := ctasIfNotExistsCount(t, db, "fresh2"); n != 2 {
		t.Fatalf("fresh IF NOT EXISTS CTAS count = %d, want 2", n)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE fresh AS SELECT * FROM src"); err == nil {
		t.Fatal("CTAS onto an existing table without IF NOT EXISTS unexpectedly succeeded")
	}
}

// TestCTASIfNotExistsIsNoopOnExistingTable pins the fix: the statement must
// not insert any rows into the pre-existing table.
func TestCTASIfNotExistsIsNoopOnExistingTable(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	ctasIfNotExistsExec(t, db, "CREATE TABLE src (id INTEGER)")
	ctasIfNotExistsExec(t, db, "INSERT INTO src VALUES (1)")
	ctasIfNotExistsExec(t, db, "INSERT INTO src VALUES (2)")
	ctasIfNotExistsExec(t, db, "CREATE TABLE tgt AS SELECT * FROM src")
	if n := ctasIfNotExistsCount(t, db, "tgt"); n != 2 {
		t.Fatalf("construction: tgt count = %d, want 2", n)
	}

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS tgt AS SELECT * FROM src"); err != nil {
		t.Fatalf("IF NOT EXISTS CTAS errored: %v", err)
	}
	if n := ctasIfNotExistsCount(t, db, "tgt"); n != 2 {
		t.Fatalf("FAIL: COUNT(*) FROM tgt = %d, want 2 — CREATE TABLE IF NOT EXISTS ... AS SELECT on an existing table must be a no-op, but the CTAS insert loop appended the rows into the pre-existing table (silent duplication)", n)
	}

	// Repeating the statement stays a no-op too.
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS tgt AS SELECT * FROM src"); err != nil {
		t.Fatalf("second IF NOT EXISTS CTAS errored: %v", err)
	}
	if n := ctasIfNotExistsCount(t, db, "tgt"); n != 2 {
		t.Fatalf("second run: COUNT(*) FROM tgt = %d, want 2", n)
	}
}
